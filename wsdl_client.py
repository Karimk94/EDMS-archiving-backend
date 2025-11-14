import zeep
import os
import logging
from zeep import Client, Settings, xsd
from zeep.exceptions import Fault
from dotenv import load_dotenv

load_dotenv()

WSDL_URL = os.getenv("WSDL_URL")
DMS_USER = os.getenv("DMS_USER")
DMS_PASSWORD = os.getenv("DMS_PASSWORD")


# --- System Login (for background tasks) ---
def dms_system_login():
    """Logs into the DMS SOAP service using system credentials from .env and returns a session token (DST)."""
    try:
        if not WSDL_URL:
            raise ValueError("WSDL_URL is not set in the environment file.")
        if not DMS_USER or not DMS_PASSWORD:
            raise ValueError("DMS_USER or DMS_PASSWORD not set in environment file.")

        settings = Settings(strict=False, xml_huge_tree=True)
        client = Client(WSDL_URL, settings=settings)

        login_info_type = client.get_type(
            '{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}DMSvrLoginInfo')
        login_info_instance = login_info_type(network=0, loginContext='RTA_MAIN', username=DMS_USER,
                                              password=DMS_PASSWORD)

        array_type = client.get_type(
            '{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}ArrayOfDMSvrLoginInfo')
        login_info_array_instance = array_type(DMSvrLoginInfo=[login_info_instance])

        call_data = {'call': {'loginInfo': login_info_array_instance, 'authen': 1, 'dstIn': ''}}

        response = client.service.LoginSvr5(**call_data)

        if response and response.resultCode == 0 and response.DSTOut:
            return response.DSTOut
        else:
            result_code = getattr(response, 'resultCode', 'N/A')
            logging.error(f"DMS system login failed. Result code: {result_code}")
            return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during DMS system login: {e}", exc_info=True)
        return None

# --- User Login (for user-facing sessions) ---
def dms_user_login(username, password):
    """Logs into the DMS SOAP service with user-provided credentials and returns a session token (DST)."""
    try:
        if not WSDL_URL:
            raise ValueError("WSDL_URL is not set in the environment file.")
        settings = Settings(strict=False, xml_huge_tree=True)
        client = Client(WSDL_URL, settings=settings)
        login_info_type = client.get_type(
            '{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}DMSvrLoginInfo')
        login_info_instance = login_info_type(network=0, loginContext='RTA_MAIN', username=username,
                                              password=password)
        array_type = client.get_type(
            '{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}ArrayOfDMSvrLoginInfo')
        login_info_array_instance = array_type(DMSvrLoginInfo=[login_info_instance])
        call_data = {'call': {'loginInfo': login_info_array_instance, 'authen': 1, 'dstIn': ''}}
        response = client.service.LoginSvr5(**call_data)
        if response and response.resultCode == 0 and response.DSTOut:
            return response.DSTOut
        else:
            logging.error(
                f"DMS login failed for user '{username}'. Result code: {getattr(response, 'resultCode', 'N/A')}")
            return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during DMS login: {e}", exc_info=True)
        return None

# --- Document Management Functions for Archiving ---
def upload_archive_document_to_dms(dst, file_stream, metadata):
    """
    Uploads a document to the DMS for the archiving system.
    'metadata' must contain 'docname', 'abstract', 'filename', 'dms_user', 'app_id'.
    """
    svc_client, obj_client = None, None
    created_doc_number, version_id, put_doc_id, stream_id = None, None, None, None
    try:
        if not WSDL_URL:
            raise ValueError("WSDL_URL is not set in the environment file.")

        settings = Settings(strict=False, xml_huge_tree=True)
        svc_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMSvc', settings=settings)
        obj_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMObj', settings=settings)
        string_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
        int_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}int')
        string_array_type = svc_client.get_type(
            '{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfstring')

        dms_user = metadata.get('dms_user', 'SYSTEM')
        app_id = metadata.get('app_id', 'UNKNOWN')

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
            xsd.AnyObject(string_type, app_id),
            xsd.AnyObject(string_type, '1')
        ]

        create_object_call = {'call': {'dstIn': dst, 'objectType': 'DEF_PROF', 'properties': {
            'propertyCount': len(property_names.string), 'propertyNames': property_names,
            'propertyValues': {'anyType': property_values_list}}}}

        logging.info(f"Uploading doc '{metadata['docname']}' for user '{dms_user}'...")
        create_reply = svc_client.service.CreateObject(**create_object_call)

        if not (create_reply and create_reply.resultCode == 0 and create_reply.retProperties):
            raise Exception(f"CreateObject failed. Details: {getattr(create_reply, 'errorDoc', 'No details')}")

        ret_prop_names = create_reply.retProperties.propertyNames.string
        ret_prop_values = create_reply.retProperties.propertyValues.anyType
        created_doc_number = ret_prop_values[ret_prop_names.index('%OBJECT_IDENTIFIER')]
        version_id = ret_prop_values[ret_prop_names.index('%VERSION_ID')]
        logging.info(f"Doc created with number: {created_doc_number}")

        put_doc_call = {'call': {'dstIn': dst, 'libraryName': 'RTA_MAIN', 'documentNumber': created_doc_number,
                                 'versionID': version_id}}
        put_doc_reply = svc_client.service.PutDoc(**put_doc_call)
        if not (put_doc_reply and put_doc_reply.resultCode == 0 and put_doc_reply.putDocID):
            raise Exception(f"PutDoc failed. Result: {getattr(put_doc_reply, 'resultCode', 'N/A')}")
        put_doc_id = put_doc_reply.putDocID

        get_stream_reply = obj_client.service.GetWriteStream(call={'dstIn': dst, 'contentID': put_doc_id})
        if not (get_stream_reply and get_stream_reply.resultCode == 0 and get_stream_reply.streamID):
            raise Exception(f"GetWriteStream failed. Result: {getattr(get_stream_reply, 'resultCode', 'N/A')}")
        stream_id = get_stream_reply.streamID

        chunk_size = 48 * 1024
        file_stream.seek(0)  # Ensure stream is at the beginning
        while True:
            chunk = file_stream.read(chunk_size)
            if not chunk: break
            stream_data_type = obj_client.get_type(
                '{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}StreamData')
            stream_data_instance = stream_data_type(bufferSize=len(chunk), streamBuffer=chunk)
            write_reply = obj_client.service.WriteStream(
                call={'streamID': stream_id, 'streamData': stream_data_instance})
            if write_reply.resultCode != 0:
                raise Exception(f"WriteStream chunk failed. Result: {write_reply.resultCode}")

        commit_reply = obj_client.service.CommitStream(call={'streamID': stream_id, 'flags': 0})
        if commit_reply.resultCode != 0:
            raise Exception(f"CommitStream failed. Result: {commit_reply.resultCode}")

        unlock_props = string_array_type(
            ['%OBJECT_TYPE_ID', '%OBJECT_IDENTIFIER', '%TARGET_LIBRARY', '%STATUS'])
        unlock_values = [xsd.AnyObject(string_type, 'def_prof'), xsd.AnyObject(int_type, created_doc_number),
                         xsd.AnyObject(string_type, 'rta_main'), xsd.AnyObject(string_type, '%UNLOCK')]
        update_call = {'call': {'dstIn': dst, 'objectType': 'Profile', 'properties': {
            'propertyCount': len(unlock_props.string), 'propertyNames': unlock_props,
            'propertyValues': {'anyType': unlock_values}}}}
        update_reply = svc_client.service.UpdateObject(**update_call)
        if update_reply.resultCode != 0:
            logging.warning(
                f"Unlock failed for doc {created_doc_number}. Result: {update_reply.resultCode}. May remain locked.")

        return created_doc_number

    except Exception as e:
        logging.error(f"DMS archive upload process failed: {e}", exc_info=True)
        # Attempt to clean up the profile if the file upload failed mid-way
        if created_doc_number and not put_doc_id:  # Profile created but no doc
            try:
                logging.warning(f"Attempting to delete orphaned profile {created_doc_number} after upload failure.")
                # This part needs a "DeleteObject" call which is not defined, logging for now.
                # A real implementation would call svc_client.service.DeleteObject(...)
                pass
            except Exception as delete_e:
                logging.error(f"Failed to delete orphaned profile: {delete_e}")
        return None
    finally:
        if obj_client:
            if put_doc_id:
                try:
                    obj_client.service.ReleaseObject(call={'objectID': put_doc_id})
                except Exception:
                    pass
            if stream_id:
                try:
                    obj_client.service.ReleaseObject(call={'objectID': stream_id})
                except Exception:
                    pass

def get_document_from_dms(dst, doc_number):
    """
    Retrieves a document's content and filename from DMS for the archiving system.
    """
    svc_client, obj_client, content_id, stream_id = None, None, None, None
    try:
        if not WSDL_URL:
            raise ValueError("WSDL_URL is not set in the environment file.")

        settings = Settings(strict=False, xml_huge_tree=True)
        svc_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMSvc', settings=settings)
        obj_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMObj', settings=settings)

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

        filename = f"{doc_number}"  # Default
        if doc_reply.docProperties and doc_reply.docProperties.propertyValues:
            try:
                prop_names = doc_reply.docProperties.propertyNames.string
                if '%VERSION_FILE_NAME' in prop_names:
                    index = prop_names.index('%VERSION_FILE_NAME')
                    version_file_name = doc_reply.docProperties.propertyValues.anyType[index]
                    if version_file_name:
                        filename = str(version_file_name)
            except Exception:
                pass  # Use default filename

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
                try:
                    obj_client.service.ReleaseObject(call={'objectID': stream_id})
                except Exception:
                    pass
            if content_id:
                try:
                    obj_client.service.ReleaseObject(call={'objectID': content_id})
                except Exception:
                    pass