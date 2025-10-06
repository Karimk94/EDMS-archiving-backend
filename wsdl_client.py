import zeep
import os
import logging
from zeep import Client, Settings, xsd
from zeep.exceptions import Fault
from dotenv import load_dotenv
from db_connector import get_app_id_from_extension

load_dotenv()
WSDL_URL = os.getenv("WSDL_URL")

def dms_login(username, password):
    """Logs into the DMS SOAP service and returns a session token (DST)."""
    try:
        if not WSDL_URL:
            raise ValueError("WSDL_URL is not set in the environment file.")
        settings = Settings(strict=False, xml_huge_tree=True)
        client = Client(WSDL_URL, settings=settings)
        login_info_type = client.get_type('{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}DMSvrLoginInfo')
        login_info_instance = login_info_type(network=0, loginContext='RTA_MAIN', username=username, password=password)
        array_type = client.get_type('{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}ArrayOfDMSvrLoginInfo')
        login_info_array_instance = array_type(DMSvrLoginInfo=[login_info_instance])
        call_data = {'call': {'loginInfo': login_info_array_instance, 'authen': 1, 'dstIn': ''}}
        response = client.service.LoginSvr5(**call_data)
        if response and response.resultCode == 0 and response.DSTOut:
            return response.DSTOut
        else:
            logging.error(f"DMS login failed for user '{username}'. Result code: {getattr(response, 'resultCode', 'N/A')}")
            return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during DMS login: {e}", exc_info=True)
        return None

def upload_document_to_dms(dst, file_stream, metadata):
    svc_client, obj_client = None, None
    try:
        settings = Settings(strict=False, xml_huge_tree=True)
        svc_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMSvc', settings=settings)
        obj_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMObj', settings=settings)
        string_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
        int_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}int')
        string_array_type = svc_client.get_type('{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfstring')
        
        dms_user = metadata.get('dms_user', 'SYSTEM')

        property_names = string_array_type([
            '%TARGET_LIBRARY', '%RECENTLY_USED_LOCATION', 'DOCNAME', 'TYPE_ID', 
            'AUTHOR_ID', 'TYPIST_ID', 'ABSTRACT', 'APP_ID', 'SECURITY'
        ])
        
        # This function now correctly uses the app_id passed from db_connector
        property_values_list = [
            xsd.AnyObject(string_type, 'RTA_MAIN'), 
            xsd.AnyObject(string_type, 'DOCSOPEN!L\\RTA_MAIN'),
            xsd.AnyObject(string_type, metadata['docname']),
            xsd.AnyObject(string_type, 'DEFAULT'), 
            xsd.AnyObject(string_type, dms_user),
            xsd.AnyObject(string_type, dms_user),
            xsd.AnyObject(string_type, metadata['abstract']),
            xsd.AnyObject(string_type, metadata.get('app_id', 'UNKNOWN')), 
            xsd.AnyObject(string_type, '1')
        ]
        
        create_object_call = { 'call': { 'dstIn': dst, 'objectType': 'DEF_PROF', 'properties': { 'propertyCount': len(property_names.string), 'propertyNames': property_names, 'propertyValues': {'anyType': property_values_list} } } }

        create_reply = svc_client.service.CreateObject(**create_object_call)
        
        if not (create_reply and create_reply.resultCode == 0 and create_reply.retProperties):
            raise Exception(f"CreateObject failed. Details: {getattr(create_reply, 'errorDoc', 'No details')}")
        
        ret_prop_names = create_reply.retProperties.propertyNames.string
        ret_prop_values = create_reply.retProperties.propertyValues.anyType
        created_doc_number = ret_prop_values[ret_prop_names.index('%OBJECT_IDENTIFIER')]
        version_id = ret_prop_values[ret_prop_names.index('%VERSION_ID')]
        
        # Step 3: PutDoc
        put_doc_call = {'call': {'dstIn': dst, 'libraryName': 'RTA_MAIN', 'documentNumber': created_doc_number, 'versionID': version_id}}
        put_doc_reply = svc_client.service.PutDoc(**put_doc_call)
        if not (put_doc_reply and put_doc_reply.resultCode == 0 and put_doc_reply.putDocID):
            raise Exception(f"PutDoc failed. Result: {getattr(put_doc_reply, 'resultCode', 'N/A')}")
        put_doc_id = put_doc_reply.putDocID

        # Step 4: GetWriteStream
        get_stream_reply = obj_client.service.GetWriteStream(call={'dstIn': dst, 'contentID': put_doc_id})
        if not (get_stream_reply and get_stream_reply.resultCode == 0 and get_stream_reply.streamID):
            raise Exception(f"GetWriteStream failed. Result: {getattr(get_stream_reply, 'resultCode', 'N/A')}")
        stream_id = get_stream_reply.streamID

        # Step 5: WriteStream (Loop)
        chunk_size = 48 * 1024
        while True:
            chunk = file_stream.read(chunk_size)
            if not chunk: break
            stream_data_type = obj_client.get_type('{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}StreamData')
            stream_data_instance = stream_data_type(bufferSize=len(chunk), streamBuffer=chunk)
            write_reply = obj_client.service.WriteStream(call={'streamID': stream_id, 'streamData': stream_data_instance})
            if write_reply.resultCode != 0:
                raise Exception(f"WriteStream chunk failed. Result: {write_reply.resultCode}")
        
        # Step 6: CommitStream
        commit_reply = obj_client.service.CommitStream(call={'streamID': stream_id, 'flags': 0})
        if commit_reply.resultCode != 0:
            raise Exception(f"CommitStream failed. Result: {commit_reply.resultCode}")

        # Step 9: UpdateObject (Unlock)
        unlock_props = string_array_type(['%OBJECT_TYPE_ID', '%OBJECT_IDENTIFIER', '%TARGET_LIBRARY', '%STATUS'])
        unlock_values = [xsd.AnyObject(string_type, 'def_prof'), xsd.AnyObject(int_type, created_doc_number), xsd.AnyObject(string_type, 'rta_main'), xsd.AnyObject(string_type, '%UNLOCK')]
        update_call = {'call': {'dstIn': dst, 'objectType': 'Profile', 'properties': {'propertyCount': len(unlock_props.string), 'propertyNames': unlock_props, 'propertyValues': {'anyType': unlock_values}}}}
        update_reply = svc_client.service.UpdateObject(**update_call)
        if update_reply.resultCode != 0:
            logging.warning(f"Unlock failed for doc {created_doc_number}. Result: {update_reply.resultCode}. May remain locked.")
        
        return created_doc_number

    except Exception as e:
        logging.error(f"DMS upload process failed: {e}", exc_info=True)
        return None
    finally:
        if obj_client:
            if put_doc_id:
                try: obj_client.service.ReleaseObject(call={'objectID': put_doc_id})
                except Exception: pass
            if stream_id:
                try: obj_client.service.ReleaseObject(call={'objectID': stream_id})
                except Exception: pass

def get_document_from_dms(dst, doc_number):
    """
    Retrieves a document's content and filename from DMS.
    This logic is based on your original middleware.
    """
    svc_client, obj_client, content_id, stream_id = None, None, None, None
    try:
        settings = Settings(strict=False, xml_huge_tree=True)
        svc_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMSvc', settings=settings)
        obj_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMObj', settings=settings)

        # Get the document metadata, including the original filename
        get_doc_call = {
            'call': {
                'dstIn': dst,
                'criteria': {
                    'criteriaCount': 2,
                    'criteriaNames': {'string': ['%TARGET_LIBRARY', '%DOCUMENT_NUMBER']},
                    'criteriaValues': {'string': ['RTA_MAIN', str(doc_number)]}
                }
            }
        }
        doc_reply = svc_client.service.GetDocSvr3(**get_doc_call)

        if not (doc_reply and doc_reply.resultCode == 0 and doc_reply.getDocID):
            logging.warning(f"Document not found in DMS for doc_number: {doc_number}.")
            return None, None

        filename = f"{doc_number}" # Default
        if doc_reply.docProperties and doc_reply.docProperties.propertyValues:
            try:
                prop_names = doc_reply.docProperties.propertyNames.string
                if '%VERSION_FILE_NAME' in prop_names:
                    index = prop_names.index('%VERSION_FILE_NAME')
                    version_file_name = doc_reply.docProperties.propertyValues.anyType[index]
                    if version_file_name:
                        filename = str(version_file_name)
            except Exception:
                pass # Use default filename on error

        # Get the document content stream
        content_id = doc_reply.getDocID
        stream_reply = obj_client.service.GetReadStream(call={'dstIn': dst, 'contentID': content_id})
        if not (stream_reply and stream_reply.resultCode == 0 and stream_reply.streamID):
            raise Exception(f"Failed to get read stream for doc {doc_number}")
        
        stream_id = stream_reply.streamID
        doc_buffer = bytearray()
        while True:
            read_reply = obj_client.service.ReadStream(call={'streamID': stream_id, 'requestedBytes': 65536})
            if not read_reply or read_reply.resultCode != 0: break
            chunk_data = read_reply.streamData.streamBuffer if read_reply.streamData else None
            if not chunk_data: break
            doc_buffer.extend(chunk_data)
        
        return bytes(doc_buffer), filename

    except Exception as e:
        logging.error(f"DMS document retrieval failed for doc {doc_number}: {e}", exc_info=True)
        return None, None
    finally:
        if obj_client:
            if stream_id:
                try: obj_client.service.ReleaseObject(call={'objectID': stream_id})
                except Exception: pass
            if content_id:
                try: obj_client.service.ReleaseObject(call={'objectID': content_id})
                except Exception: pass


    svc_client, obj_client = None, None
    created_doc_number, version_id, put_doc_id, stream_id = None, None, None, None
    try:
        settings = Settings(strict=False, xml_huge_tree=True)
        svc_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMSvc', settings=settings)
        obj_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMObj', settings=settings)
        string_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
        int_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}int')
        string_array_type = svc_client.get_type('{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfstring')
        
        dms_user = metadata.get('dms_user', 'SYSTEM')

        property_names = string_array_type([
            '%TARGET_LIBRARY', '%RECENTLY_USED_LOCATION', 'DOCNAME', 'TYPE_ID', 
            'AUTHOR_ID', 'TYPIST_ID', 'ABSTRACT', 'APP_ID', 'SECURITY'
        ])
        
        property_values_list = [
            xsd.AnyObject(string_type, 'RTA_MAIN'), 
            xsd.AnyObject(string_type, 'DOCSOPEN!L\\RTA_MAIN'),
            xsd.AnyObject(string_type, metadata['docname']),
            xsd.AnyObject(string_type, 'DEFAULT'), 
            xsd.AnyObject(string_type, dms_user),
            xsd.AnyObject(string_type, dms_user),
            xsd.AnyObject(string_type, metadata['abstract']),
            xsd.AnyObject(string_type, metadata.get('app_id', 'UNKNOWN')), 
            xsd.AnyObject(string_type, '1')
        ]
        
        create_object_call = { 'call': { 'dstIn': dst, 'objectType': 'DEF_PROF', 'properties': { 'propertyCount': len(property_names.string), 'propertyNames': property_names, 'propertyValues': {'anyType': property_values_list} } } }

        create_reply = svc_client.service.CreateObject(**create_object_call)
        
        if not (create_reply and create_reply.resultCode == 0 and create_reply.retProperties):
            raise Exception(f"CreateObject failed. Details: {getattr(create_reply, 'errorDoc', 'No details')}")
        
        # The following logic correctly extracts the created_doc_number and other necessary variables.
        ret_prop_names = create_reply.retProperties.propertyNames.string
        ret_prop_values = create_reply.retProperties.propertyValues.anyType
        created_doc_number = ret_prop_values[ret_prop_names.index('%OBJECT_IDENTIFIER')]
        version_id = ret_prop_values[ret_prop_names.index('%VERSION_ID')]
        
        # Step 3: PutDoc
        put_doc_call = {'call': {'dstIn': dst, 'libraryName': 'RTA_MAIN', 'documentNumber': created_doc_number, 'versionID': version_id}}
        put_doc_reply = svc_client.service.PutDoc(**put_doc_call)
        if not (put_doc_reply and put_doc_reply.resultCode == 0 and put_doc_reply.putDocID):
            raise Exception(f"PutDoc failed. Result: {getattr(put_doc_reply, 'resultCode', 'N/A')}")
        put_doc_id = put_doc_reply.putDocID

        # Step 4: GetWriteStream
        get_stream_reply = obj_client.service.GetWriteStream(call={'dstIn': dst, 'contentID': put_doc_id})
        if not (get_stream_reply and get_stream_reply.resultCode == 0 and get_stream_reply.streamID):
            raise Exception(f"GetWriteStream failed. Result: {getattr(get_stream_reply, 'resultCode', 'N/A')}")
        stream_id = get_stream_reply.streamID

        # Step 5: WriteStream (Loop)
        chunk_size = 48 * 1024
        while True:
            chunk = file_stream.read(chunk_size)
            if not chunk: break
            stream_data_type = obj_client.get_type('{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}StreamData')
            stream_data_instance = stream_data_type(bufferSize=len(chunk), streamBuffer=chunk)
            write_reply = obj_client.service.WriteStream(call={'streamID': stream_id, 'streamData': stream_data_instance})
            if write_reply.resultCode != 0:
                raise Exception(f"WriteStream chunk failed. Result: {write_reply.resultCode}")
        
        # Step 6: CommitStream
        commit_reply = obj_client.service.CommitStream(call={'streamID': stream_id, 'flags': 0})
        if commit_reply.resultCode != 0:
            raise Exception(f"CommitStream failed. Result: {commit_reply.resultCode}")

        # Step 9: UpdateObject (Unlock)
        unlock_props = string_array_type(['%OBJECT_TYPE_ID', '%OBJECT_IDENTIFIER', '%TARGET_LIBRARY', '%STATUS'])
        unlock_values = [xsd.AnyObject(string_type, 'def_prof'), xsd.AnyObject(int_type, created_doc_number), xsd.AnyObject(string_type, 'rta_main'), xsd.AnyObject(string_type, '%UNLOCK')]
        update_call = {'call': {'dstIn': dst, 'objectType': 'Profile', 'properties': {'propertyCount': len(unlock_props.string), 'propertyNames': unlock_props, 'propertyValues': {'anyType': unlock_values}}}}
        update_reply = svc_client.service.UpdateObject(**update_call)
        if update_reply.resultCode != 0:
            logging.warning(f"Unlock failed for doc {created_doc_number}. Result: {update_reply.resultCode}. May remain locked.")
        
        return created_doc_number

    except Exception as e:
        logging.error(f"DMS upload process failed: {e}", exc_info=True)
        return None
    finally:
        if obj_client:
            if put_doc_id:
                try: obj_client.service.ReleaseObject(call={'objectID': put_doc_id})
                except Exception: pass
            if stream_id:
                try: obj_client.service.ReleaseObject(call={'objectID': stream_id})
                except Exception: pass


    svc_client, obj_client = None, None
    try:
        settings = Settings(strict=False, xml_huge_tree=True)
        svc_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMSvc', settings=settings)
        obj_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMObj', settings=settings)
        string_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
        int_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}int')
        string_array_type = svc_client.get_type('{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfstring')
        
        dms_user = metadata.get('dms_user', 'SYSTEM')

        property_names = string_array_type([
            '%TARGET_LIBRARY', '%RECENTLY_USED_LOCATION', 'DOCNAME', 'TYPE_ID', 
            'AUTHOR_ID', 'TYPIST_ID', 'ABSTRACT', 'APP_ID', 'SECURITY'
        ])
        
        # --- THIS IS THE FIX ---
        # This function now correctly uses the app_id passed from db_connector
        # and no longer has its own fallback to "DEFAULT"
        property_values_list = [
            xsd.AnyObject(string_type, 'RTA_MAIN'), 
            xsd.AnyObject(string_type, 'DOCSOPEN!L\\RTA_MAIN'),
            xsd.AnyObject(string_type, metadata['docname']),
            xsd.AnyObject(string_type, 'DEFAULT'), 
            xsd.AnyObject(string_type, dms_user),
            xsd.AnyObject(string_type, dms_user),
            xsd.AnyObject(string_type, metadata['abstract']),
            xsd.AnyObject(string_type, metadata.get('app_id', 'UNKNOWN')), 
            xsd.AnyObject(string_type, '1')
        ]
        # --- END OF FIX ---
        
        create_object_call = { 'call': { 'dstIn': dst, 'objectType': 'DEF_PROF', 'properties': { 'propertyCount': len(property_names.string), 'propertyNames': property_names, 'propertyValues': {'anyType': property_values_list} } } }

        create_reply = svc_client.service.CreateObject(**create_object_call)
        
        if not (create_reply and create_reply.resultCode == 0 and create_reply.retProperties):
            raise Exception(f"CreateObject failed. Details: {getattr(create_reply, 'errorDoc', 'No details')}")
        
        # ... (rest of the upload logic is correct and unchanged)
        
        return created_doc_number

    except Exception as e:
        logging.error(f"DMS upload process failed: {e}", exc_info=True)
        return None
    finally:
        # ... (cleanup logic is unchanged)
        pass


    svc_client, obj_client = None, None
    created_doc_number, version_id, put_doc_id, stream_id = None, None, None, None
    try:
        settings = Settings(strict=False, xml_huge_tree=True)
        svc_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMSvc', settings=settings)
        obj_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMObj', settings=settings)
        string_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
        int_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}int')
        string_array_type = svc_client.get_type('{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfstring')
        
        dms_user = metadata.get('dms_user', 'SYSTEM')

        property_names = string_array_type([
            '%TARGET_LIBRARY', '%RECENTLY_USED_LOCATION', 'DOCNAME', 'TYPE_ID', 
            'AUTHOR_ID', 'TYPIST_ID', 'ABSTRACT', 'APP_ID', 'SECURITY'
        ])
        
        property_values_list = [
            xsd.AnyObject(string_type, 'RTA_MAIN'), 
            xsd.AnyObject(string_type, 'DOCSOPEN!L\\RTA_MAIN'),
            xsd.AnyObject(string_type, metadata['docname']),
            xsd.AnyObject(string_type, 'DEFAULT'), 
            xsd.AnyObject(string_type, dms_user),
            xsd.AnyObject(string_type, dms_user),
            xsd.AnyObject(string_type, metadata['abstract']),
            xsd.AnyObject(string_type, metadata.get('app_id', 'UNKNOWN')), 
            xsd.AnyObject(string_type, '1')
        ]
        
        create_object_call = { 'call': { 'dstIn': dst, 'objectType': 'DEF_PROF', 'properties': { 'propertyCount': len(property_names.string), 'propertyNames': property_names, 'propertyValues': {'anyType': property_values_list} } } }

        create_reply = svc_client.service.CreateObject(**create_object_call)
        
        if not (create_reply and create_reply.resultCode == 0 and create_reply.retProperties):
            raise Exception(f"CreateObject failed. Details: {getattr(create_reply, 'errorDoc', 'No details')}")
        
        # --- FIX: Correctly define created_doc_number and other variables ---
        ret_prop_names = create_reply.retProperties.propertyNames.string
        ret_prop_values = create_reply.retProperties.propertyValues.anyType
        created_doc_number = ret_prop_values[ret_prop_names.index('%OBJECT_IDENTIFIER')]
        version_id = ret_prop_values[ret_prop_names.index('%VERSION_ID')]

        # Step 3: PutDoc
        put_doc_call = {'call': {'dstIn': dst, 'libraryName': 'RTA_MAIN', 'documentNumber': created_doc_number, 'versionID': version_id}}
        put_doc_reply = svc_client.service.PutDoc(**put_doc_call)
        if not (put_doc_reply and put_doc_reply.resultCode == 0 and put_doc_reply.putDocID):
            raise Exception(f"PutDoc failed. Result: {getattr(put_doc_reply, 'resultCode', 'N/A')}")
        put_doc_id = put_doc_reply.putDocID

        # Step 4: GetWriteStream
        get_stream_reply = obj_client.service.GetWriteStream(call={'dstIn': dst, 'contentID': put_doc_id})
        if not (get_stream_reply and get_stream_reply.resultCode == 0 and get_stream_reply.streamID):
            raise Exception(f"GetWriteStream failed. Result: {getattr(get_stream_reply, 'resultCode', 'N/A')}")
        stream_id = get_stream_reply.streamID

        # Step 5: WriteStream (Loop)
        chunk_size = 48 * 1024
        while True:
            chunk = file_stream.read(chunk_size)
            if not chunk: break
            stream_data_type = obj_client.get_type('{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}StreamData')
            stream_data_instance = stream_data_type(bufferSize=len(chunk), streamBuffer=chunk)
            write_reply = obj_client.service.WriteStream(call={'streamID': stream_id, 'streamData': stream_data_instance})
            if write_reply.resultCode != 0:
                raise Exception(f"WriteStream chunk failed. Result: {write_reply.resultCode}")
        
        # Step 6: CommitStream
        commit_reply = obj_client.service.CommitStream(call={'streamID': stream_id, 'flags': 0})
        if commit_reply.resultCode != 0:
            raise Exception(f"CommitStream failed. Result: {commit_reply.resultCode}")

        # Step 9: UpdateObject (Unlock)
        unlock_props = string_array_type(['%OBJECT_TYPE_ID', '%OBJECT_IDENTIFIER', '%TARGET_LIBRARY', '%STATUS'])
        unlock_values = [xsd.AnyObject(string_type, 'def_prof'), xsd.AnyObject(int_type, created_doc_number), xsd.AnyObject(string_type, 'rta_main'), xsd.AnyObject(string_type, '%UNLOCK')]
        update_call = {'call': {'dstIn': dst, 'objectType': 'Profile', 'properties': {'propertyCount': len(unlock_props.string), 'propertyNames': unlock_props, 'propertyValues': {'anyType': unlock_values}}}}
        update_reply = svc_client.service.UpdateObject(**update_call)
        if update_reply.resultCode != 0:
            logging.warning(f"Unlock failed for doc {created_doc_number}. Result: {update_reply.resultCode}. May remain locked.")
        
        return created_doc_number

    except Exception as e:
        logging.error(f"DMS upload process failed: {e}", exc_info=True)
        return None
    finally:
        if obj_client:
            if put_doc_id:
                try: obj_client.service.ReleaseObject(call={'objectID': put_doc_id})
                except Exception: pass
            if stream_id:
                try: obj_client.service.ReleaseObject(call={'objectID': stream_id})
                except Exception: pass

    svc_client, obj_client = None, None
    try:
        settings = Settings(strict=False, xml_huge_tree=True)
        svc_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMSvc', settings=settings)
        obj_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMObj', settings=settings)
        string_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
        int_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}int')
        string_array_type = svc_client.get_type('{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfstring')
        
        # --- THIS IS THE FIX ---
        # The DMS requires AUTHOR_ID and TYPIST_ID to be the logged-in system user.
        dms_user = metadata.get('dms_user', 'SYSTEM') # Fallback to 'SYSTEM' if not provided

        property_names = string_array_type([
            '%TARGET_LIBRARY', '%RECENTLY_USED_LOCATION', 'DOCNAME', 'TYPE_ID', 
            'AUTHOR_ID', 'TYPIST_ID', 'ABSTRACT', 'APP_ID', 'SECURITY'
        ])
        
        property_values_list = [
            xsd.AnyObject(string_type, 'RTA_MAIN'), 
            xsd.AnyObject(string_type, 'DOCSOPEN!L\\RTA_MAIN'), # Restored from original middleware
            xsd.AnyObject(string_type, metadata['docname']),
            xsd.AnyObject(string_type, 'DEFAULT'), 
            xsd.AnyObject(string_type, dms_user), # Use the logged-in user
            xsd.AnyObject(string_type, dms_user), # Use the logged-in user
            xsd.AnyObject(string_type, metadata['abstract']),
            xsd.AnyObject(string_type, metadata.get('app_id', 'UNKNOWN')), 
            xsd.AnyObject(string_type, '1')
        ]
        
        create_object_call = { 'call': { 'dstIn': dst, 'objectType': 'DEF_PROF', 'properties': { 'propertyCount': len(property_names.string), 'propertyNames': property_names, 'propertyValues': {'anyType': property_values_list} } } }

        create_reply = svc_client.service.CreateObject(**create_object_call)
        
        if not (create_reply and create_reply.resultCode == 0 and create_reply.retProperties):
            raise Exception(f"CreateObject failed. Details: {getattr(create_reply, 'errorDoc', 'No details')}")
        
        # ... (rest of the upload logic is correct and unchanged)
        
        return created_doc_number

    except Exception as e:
        logging.error(f"DMS upload process failed: {e}", exc_info=True)
        return None
    finally:
        # ... (cleanup logic is unchanged)
        pass

    """
    Uploads a document to the DMS using the full 9-step SOAP sequence.
    """
    print("\n--- [wsdl_client] LOG: Starting DMS upload process ---")
    svc_client, obj_client = None, None
    created_doc_number, version_id, put_doc_id, stream_id = None, None, None, None
    try:
        settings = Settings(strict=False, xml_huge_tree=True)
        svc_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMSvc', settings=settings)
        obj_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMObj', settings=settings)
        string_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
        int_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}int')
        string_array_type = svc_client.get_type('{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfstring')
        
        print("--- [wsdl_client] LOG: Step 2: CreateObject ---")
        
        _, file_extension = os.path.splitext(metadata['filename'])
        app_id = get_app_id_from_extension(file_extension.lstrip('.').upper()) or 'UNKNOWN'
        
        property_names = string_array_type([
            '%TARGET_LIBRARY', 'DOCNAME', 'TYPE_ID', 'AUTHOR_ID', 
            'TYPIST_ID', 'ABSTRACT', 'APP_ID', 'SECURITY'
        ])
        
        property_values_list = [
            xsd.AnyObject(string_type, 'RTA_MAIN'), 
            xsd.AnyObject(string_type, metadata['docname']),
            xsd.AnyObject(string_type, 'DEFAULT'), 
            xsd.AnyObject(string_type, metadata.get('author', 'SYSTEM')),
            xsd.AnyObject(string_type, metadata.get('author', 'SYSTEM')), 
            xsd.AnyObject(string_type, metadata['abstract']),
            xsd.AnyObject(string_type, app_id),
            xsd.AnyObject(string_type, '1')
        ]
        
        create_object_call = {
            'call': {
                'dstIn': dst, 'objectType': 'DEF_PROF', 
                'properties': {
                    'propertyCount': len(property_names.string), 
                    'propertyNames': property_names, 
                    'propertyValues': {'anyType': property_values_list}
                }
            }
        }

        print(f"--- [wsdl_client] DEBUG: Sending CreateObject request with properties: {[p for p in property_names.string]}")
        create_reply = svc_client.service.CreateObject(**create_object_call)
        
        if not (create_reply and create_reply.resultCode == 0 and create_reply.retProperties):
            error_details = getattr(create_reply, 'errorDoc', 'No details provided.')
            raise Exception(f"CreateObject failed. Result: {getattr(create_reply, 'resultCode', 'N/A')}. Details: {error_details}")
        
        ret_prop_names = create_reply.retProperties.propertyNames.string
        ret_prop_values = create_reply.retProperties.propertyValues.anyType
        created_doc_number = ret_prop_values[ret_prop_names.index('%OBJECT_IDENTIFIER')]
        version_id = ret_prop_values[ret_prop_names.index('%VERSION_ID')]
        print(f"--- [wsdl_client] LOG: Step 2 SUCCESS: Got DOCNUMBER {created_doc_number}")

        print("--- [wsdl_client] LOG: Step 3: PutDoc ---")
        put_doc_call = {'call': {'dstIn': dst, 'libraryName': 'RTA_MAIN', 'documentNumber': created_doc_number, 'versionID': version_id}}
        put_doc_reply = svc_client.service.PutDoc(**put_doc_call)
        if not (put_doc_reply and put_doc_reply.resultCode == 0 and put_doc_reply.putDocID):
            raise Exception(f"PutDoc failed. Result: {getattr(put_doc_reply, 'resultCode', 'N/A')}")
        put_doc_id = put_doc_reply.putDocID
        print(f"--- [wsdl_client] LOG: Step 3 SUCCESS: Got PutDocID {put_doc_id}")
        
        print("--- [wsdl_client] LOG: Step 4: GetWriteStream ---")
        get_stream_reply = obj_client.service.GetWriteStream(call={'dstIn': dst, 'contentID': put_doc_id})
        if not (get_stream_reply and get_stream_reply.resultCode == 0 and get_stream_reply.streamID):
            raise Exception(f"GetWriteStream failed. Result: {getattr(get_stream_reply, 'resultCode', 'N/A')}")
        stream_id = get_stream_reply.streamID
        print(f"--- [wsdl_client] LOG: Step 4 SUCCESS: Got StreamID {stream_id}")
        
        print("--- [wsdl_client] LOG: Step 5: WriteStream (Loop) ---")
        chunk_size = 48 * 1024
        total_bytes = 0
        while True:
            chunk = file_stream.read(chunk_size)
            if not chunk: break
            total_bytes += len(chunk)
            stream_data_type = obj_client.get_type('{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}StreamData')
            stream_data_instance = stream_data_type(bufferSize=len(chunk), streamBuffer=chunk)
            write_reply = obj_client.service.WriteStream(call={'streamID': stream_id, 'streamData': stream_data_instance})
            if write_reply.resultCode != 0:
                raise Exception(f"WriteStream chunk failed. Result: {write_reply.resultCode}")
        print(f"--- [wsdl_client] LOG: Step 5 SUCCESS: Wrote {total_bytes} bytes.")
        
        print("--- [wsdl_client] LOG: Step 6: CommitStream ---")
        commit_reply = obj_client.service.CommitStream(call={'streamID': stream_id, 'flags': 0})
        if commit_reply.resultCode != 0: raise Exception(f"CommitStream failed. Result: {commit_reply.resultCode}")
        print("--- [wsdl_client] LOG: Step 6 SUCCESS.")

        print("--- [wsdl_client] LOG: Step 9: UpdateObject (Unlock) ---")
        unlock_props = string_array_type(['%OBJECT_TYPE_ID', '%OBJECT_IDENTIFIER', '%TARGET_LIBRARY', '%STATUS'])
        unlock_values = [xsd.AnyObject(string_type, 'def_prof'), xsd.AnyObject(int_type, created_doc_number), xsd.AnyObject(string_type, 'rta_main'), xsd.AnyObject(string_type, '%UNLOCK')]
        update_call = {'call': {'dstIn': dst, 'objectType': 'Profile', 'properties': {'propertyCount': len(unlock_props.string), 'propertyNames': unlock_props, 'propertyValues': {'anyType': unlock_values}}}}
        update_reply = svc_client.service.UpdateObject(**update_call)
        if update_reply.resultCode != 0:
            logging.warning(f"Unlock failed for doc {created_doc_number}. Result: {update_reply.resultCode}.")
        else:
            print("--- [wsdl_client] LOG: Step 9 SUCCESS.")

        print(f"--- [wsdl_client] LOG: Upload process complete. Returning DOCNUMBER: {created_doc_number}")
        return created_doc_number

    except Exception as e:
        print(f"--- [wsdl_client] CRITICAL ERROR: DMS UPLOAD FAILED: {e}")
        logging.error(f"DMS upload process failed: {e}", exc_info=True)
        return None
    finally:
        print("--- [wsdl_client] LOG: Executing cleanup.")
        if obj_client:
            if put_doc_id:
                try: obj_client.service.ReleaseObject(call={'objectID': put_doc_id})
                except Exception: pass
            if stream_id:
                try: obj_client.service.ReleaseObject(call={'objectID': stream_id})
                except Exception: pass