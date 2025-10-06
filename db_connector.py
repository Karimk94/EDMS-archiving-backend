import oracledb
import os
from dotenv import load_dotenv
import wsdl_client 
import re
from datetime import datetime, timedelta

load_dotenv()

def get_connection():
    try:
        dsn = f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_SERVICE_NAME')}"
        return oracledb.connect(user=os.getenv('DB_USERNAME'), password=os.getenv('DB_PASSWORD'), dsn=dsn)
    except oracledb.Error as e:
        print(f"DB connection error: {e}")
        return None

def get_app_id_from_extension(extension):
    conn = get_connection()
    if not conn: return None
    app_id = None
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT APPLICATION FROM APPS WHERE DEFAULT_EXTENSION = :ext", ext=extension)
            result = cursor.fetchone()
            if result: app_id = result[0]
            else:
                cursor.execute("SELECT APPLICATION FROM APPS WHERE FILE_TYPES LIKE :ext_like", ext_like=f"%{extension}%")
                result = cursor.fetchone()
                if result: app_id = result[0]
    finally:
        if conn: conn.close()
    return app_id

def get_dashboard_counts():
    conn = get_connection()
    if not conn:
        return {
            "total_employees": 0,
            "active_employees": 0,
            "judicial_warrants": 0,
            "expiring_soon": 0,
        }

    counts = {}
    try:
        with conn.cursor() as cursor:
            # Total employees
            cursor.execute("SELECT COUNT(*) FROM LKP_PTA_EMP_ARCH")
            counts["total_employees"] = cursor.fetchone()[0]

            # Active employees
            cursor.execute("""
                SELECT COUNT(*)
                FROM LKP_PTA_EMP_ARCH arch
                JOIN LKP_PTA_EMP_STATUS stat ON arch.STATUS_ID = stat.SYSTEM_ID
                WHERE TRIM(stat.NAME_ENGLISH) = 'Active'
            """)
            counts["active_employees"] = cursor.fetchone()[0]

            # Judicial Warrants
            cursor.execute("""
                SELECT COUNT(DISTINCT arch.SYSTEM_ID)
                FROM LKP_PTA_EMP_ARCH arch
                JOIN LKP_PTA_EMP_DOCS doc ON arch.SYSTEM_ID = doc.PTA_EMP_ARCH_ID
                JOIN LKP_PTA_DOC_TYPES dt ON doc.DOC_TYPE_ID = dt.SYSTEM_ID
                WHERE (TRIM(dt.NAME) LIKE '%Warrant Decisions%' OR TRIM(dt.NAME) LIKE '%القرارات الخاصة بالضبطية%') AND doc.DISABLED = '0'
            """)
            counts["judicial_warrants"] = cursor.fetchone()[0]

            # Expiring Soon (in the next 30 days)
            cursor.execute("""
                SELECT COUNT(DISTINCT arch.SYSTEM_ID)
                FROM LKP_PTA_EMP_ARCH arch
                JOIN LKP_PTA_EMP_DOCS doc ON arch.SYSTEM_ID = doc.PTA_EMP_ARCH_ID
                WHERE doc.EXPIRY BETWEEN SYSDATE AND SYSDATE + 30 AND doc.DISABLED = '0'
            """)
            counts["expiring_soon"] = cursor.fetchone()[0]
    finally:
        if conn:
            conn.close()
    return counts

def fetch_archived_employees(page=1, page_size=20, search_term=None, status=None, filter_type=None):
    conn = get_connection()
    if not conn: return [], 0
    offset = (page - 1) * page_size
    employees, total_rows = [], 0
    base_query = """
        FROM LKP_PTA_EMP_ARCH arch
        JOIN lkp_hr_employees hr ON arch.EMPLOYEE_ID = hr.SYSTEM_ID
        LEFT JOIN LKP_PTA_EMP_STATUS stat ON arch.STATUS_ID = stat.SYSTEM_ID
    """
    where_clauses, params = [], {}
    if search_term:
        where_clauses.append("(UPPER(TRIM(hr.FULLNAME_EN)) LIKE :search OR UPPER(TRIM(hr.FULLNAME_AR)) LIKE :search OR TRIM(hr.EMPNO) LIKE :search)")
        params['search'] = f"%{search_term.upper()}%"
    if status:
        where_clauses.append("TRIM(stat.NAME_ENGLISH) = :status")
        params['status'] = status

    if filter_type == 'judicial_warrant':
        base_query += " JOIN LKP_PTA_EMP_DOCS doc ON arch.SYSTEM_ID = doc.PTA_EMP_ARCH_ID JOIN LKP_PTA_DOC_TYPES dt ON doc.DOC_TYPE_ID = dt.SYSTEM_ID"
        where_clauses.append("(TRIM(dt.NAME) LIKE '%Warrant Decisions%' OR TRIM(dt.NAME) LIKE '%القرارات الخاصة بالضبطية%') AND doc.DISABLED = '0'")
    elif filter_type == 'expiring_soon':
        base_query += " JOIN LKP_PTA_EMP_DOCS doc ON arch.SYSTEM_ID = doc.PTA_EMP_ARCH_ID"
        where_clauses.append("doc.EXPIRY BETWEEN SYSDATE AND SYSDATE + 30 AND doc.DISABLED = '0'")

    final_where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    try:
        with conn.cursor() as cursor:
            count_query = f"SELECT COUNT(DISTINCT arch.SYSTEM_ID) {base_query} {final_where_clause}"
            cursor.execute(count_query, params)
            total_rows = cursor.fetchone()[0]

            fetch_query = f"""
                SELECT DISTINCT arch.SYSTEM_ID, TRIM(hr.FULLNAME_EN) as FULLNAME_EN, TRIM(hr.FULLNAME_AR) as FULLNAME_AR, TRIM(hr.EMPNO) as EMPNO, TRIM(hr.DEPARTEMENT) as DEPARTMENT, TRIM(hr.SECTION) as SECTION,
                       TRIM(stat.NAME_ENGLISH) as STATUS_EN, TRIM(stat.NAME_ARABIC) as STATUS_AR, TRIM(hr.JOB_NAME) as JOB_NAME
                {base_query} {final_where_clause} ORDER BY arch.SYSTEM_ID DESC
                OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
            """
            params.update({'offset': offset, 'page_size': page_size})
            cursor.execute(fetch_query, params)
            
            columns = [c[0].lower() for c in cursor.description]
            employees = []
            for row in cursor.fetchall():
                emp = dict(zip(columns, row))
                
                # Fetch judicial card details
                cursor.execute("""
                    SELECT doc.EXPIRY
                    FROM LKP_PTA_EMP_DOCS doc
                    JOIN LKP_PTA_DOC_TYPES dt ON doc.DOC_TYPE_ID = dt.SYSTEM_ID
                    WHERE doc.PTA_EMP_ARCH_ID = :1 AND (TRIM(dt.NAME) LIKE '%Judicial Card%' OR TRIM(dt.NAME) LIKE '%بطاقة الضبطية%') AND doc.DISABLED = '0'
                """, [emp['system_id']])
                judicial_card = cursor.fetchone()
                
                emp['warrant_status'] = 'توجد / Yes' if 'ضبط' in (emp.get('job_name') or '') else 'لا توجد / No'
                
                if judicial_card:
                    emp['card_status'] = 'توجد / Yes'
                    expiry_date = judicial_card[0]
                    if expiry_date:
                        emp['card_expiry'] = expiry_date.strftime('%Y-%m-%d')
                        if expiry_date < datetime.now():
                            emp['card_status_class'] = 'expired'
                        elif expiry_date < datetime.now() + timedelta(days=30):
                            emp['card_status_class'] = 'expiring-soon'
                        else:
                            emp['card_status_class'] = 'valid'
                    else:
                        emp['card_expiry'] = 'N/A'
                        emp['card_status_class'] = 'valid'
                else:
                    emp['card_status'] = 'لا توجد / No'
                    emp['card_expiry'] = 'N/A'
                    emp['card_status_class'] = ''

                employees.append(emp)
    finally:
        if conn: conn.close()
    return employees, total_rows

def fetch_hr_employees_paginated(search_term="", page=1, page_size=10):
    conn = get_connection()
    if not conn: return [], 0
    offset = (page - 1) * page_size
    employees, total_rows = [], 0
    base_query = "FROM lkp_hr_employees hr WHERE hr.SYSTEM_ID NOT IN (SELECT EMPLOYEE_ID FROM LKP_PTA_EMP_ARCH WHERE EMPLOYEE_ID IS NOT NULL)"
    params = {}
    search_clause = ""
    if search_term:
        search_clause = " AND (UPPER(TRIM(hr.FULLNAME_EN)) LIKE :search OR UPPER(TRIM(hr.FULLNAME_AR)) LIKE :search OR TRIM(hr.EMPNO) LIKE :search)"
        params['search'] = f"%{search_term.upper()}%"
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(hr.SYSTEM_ID) {base_query} {search_clause}", params)
            total_rows = cursor.fetchone()[0]
            query = f"SELECT SYSTEM_ID, TRIM(FULLNAME_EN) as FULLNAME_EN, TRIM(FULLNAME_AR) as FULLNAME_AR, TRIM(EMPNO) as EMPNO {base_query} {search_clause} ORDER BY hr.FULLNAME_EN OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY"
            params.update({'offset': offset, 'page_size': page_size})
            cursor.execute(query, params)
            employees = [dict(zip([c[0].lower() for c in cursor.description], row)) for row in cursor.fetchall()]
    finally:
        if conn: conn.close()
    return employees, total_rows

def fetch_hr_employee_details(employee_id):
    conn = get_connection()
    if not conn: return None
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT SYSTEM_ID, TRIM(FULLNAME_EN) as FULLNAME_EN, TRIM(FULLNAME_AR) as FULLNAME_AR, TRIM(EMPNO) as EMPNO, TRIM(DEPARTEMENT) as DEPARTMENT, TRIM(SECTION) as SECTION, TRIM(EMAIL) as EMAIL, TRIM(MOBILE) as MOBILE, TRIM(SUPERVISORNAME) as SUPERVISORNAME, TRIM(NATIONALITY) as NATIONALITY, TRIM(JOB_NAME) as JOB_NAME FROM lkp_hr_employees WHERE SYSTEM_ID = :1", [employee_id])
            columns = [col[0].lower() for col in cursor.description]
            row = cursor.fetchone()
            return dict(zip(columns, row)) if row else None
    finally:
        if conn: conn.close()

def fetch_statuses():
    conn = get_connection()
    if not conn: return {}
    statuses = {}
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT SYSTEM_ID, TRIM(NAME_ENGLISH) as NAME_ENGLISH, TRIM(NAME_ARABIC) as NAME_ARABIC FROM LKP_PTA_EMP_STATUS WHERE DISABLED='0'")
            statuses['employee_status'] = [dict(zip([c[0].lower() for c in cursor.description], row)) for row in cursor.fetchall()]
    finally:
        if conn: conn.close()
    return statuses

def fetch_document_types():
    conn = get_connection()
    if not conn: return {"all_types": [], "types_with_expiry": []}
    doc_types = {"all_types": [], "types_with_expiry": []}
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT SYSTEM_ID, TRIM(NAME) as NAME, HAS_EXPIRY FROM LKP_PTA_DOC_TYPES WHERE DISABLED = '0' ORDER BY SYSTEM_ID")
            for row in cursor:
                doc_type_obj = {'system_id': row[0], 'name': row[1]}
                doc_types['all_types'].append(doc_type_obj)
                if row[2] == '1':
                    doc_types['types_with_expiry'].append(doc_type_obj)
    finally:
        if conn: conn.close()
    return doc_types

def fetch_legislations():
    conn = get_connection()
    if not conn: return []
    legislations = []
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT SYSTEM_ID, TRIM(NAME) as NAME FROM LKP_PTA_LEGISL WHERE DISABLED = '0' ORDER BY NAME")
            legislations = [dict(zip([c[0].lower() for c in cursor.description], row)) for row in cursor.fetchall()]
    finally:
        if conn: conn.close()
    return legislations

def fetch_single_archived_employee(archive_id):
    conn = get_connection()
    if not conn: return None
    employee_details = {}
    try:
        with conn.cursor() as cursor:
            query = "SELECT arch.SYSTEM_ID as ARCHIVE_ID, arch.EMPLOYEE_ID, arch.STATUS_ID, arch.HIRE_DATE, TRIM(hr.FULLNAME_EN) as FULLNAME_EN, TRIM(hr.FULLNAME_AR) as FULLNAME_AR, TRIM(hr.EMPNO) as EMPNO, TRIM(hr.DEPARTEMENT) as DEPARTMENT, TRIM(hr.SECTION) as SECTION, TRIM(hr.EMAIL) as EMAIL, TRIM(hr.MOBILE) as MOBILE, TRIM(hr.SUPERVISORNAME) as SUPERVISORNAME, TRIM(hr.NATIONALITY) as NATIONALITY, TRIM(hr.JOB_NAME) as JOB_NAME FROM LKP_PTA_EMP_ARCH arch JOIN lkp_hr_employees hr ON arch.EMPLOYEE_ID = hr.SYSTEM_ID WHERE arch.SYSTEM_ID = :1"
            cursor.execute(query, [archive_id])
            columns = [col[0].lower() for col in cursor.description]
            row = cursor.fetchone()
            if not row: return None
            employee_details = dict(zip(columns, row))
            
            doc_query = "SELECT d.SYSTEM_ID, d.DOCNUMBER, d.DOC_TYPE_ID, d.EXPIRY, d.LEGISLATION_ID, TRIM(dt.NAME) as DOC_NAME FROM LKP_PTA_EMP_DOCS d JOIN LKP_PTA_DOC_TYPES dt ON d.DOC_TYPE_ID = dt.SYSTEM_ID WHERE d.PTA_EMP_ARCH_ID = :1 AND d.DISABLED = '0'"

            cursor.execute(doc_query, [archive_id])
            doc_columns = [col[0].lower() for col in cursor.description]
            employee_details['documents'] = [dict(zip(doc_columns, doc_row)) for doc_row in cursor.fetchall()]
    finally:
        if conn: conn.close()
    return employee_details

def add_employee_archive_with_docs(dst, dms_user, employee_data, documents):
    conn = get_connection()
    if not conn: return False, "Database connection failed."
    try:
        conn.begin()
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM LKP_PTA_EMP_ARCH WHERE EMPLOYEE_ID = :1", [employee_data['employee_id']])
            if cursor.fetchone()[0] > 0: return False, "This employee is already in the archive."

            # Update lkp_hr_employees with any changes from the form
            hr_update_query = """
                UPDATE lkp_hr_employees
                SET JOB_NAME = :jobTitle, NATIONALITY = :nationality, EMAIL = :email,
                    MOBILE = :phone, SUPERVISORNAME = :manager,
                    DEPARTEMENT = :department, SECTION = :section
                WHERE SYSTEM_ID = :employee_id
            """
            cursor.execute(hr_update_query, {
                'jobTitle': employee_data.get('jobTitle'),
                'nationality': employee_data.get('nationality'),
                'email': employee_data.get('email'),
                'phone': employee_data.get('phone'),
                'manager': employee_data.get('manager'),
                'department': employee_data.get('department'),
                'section': employee_data.get('section'),
                'employee_id': employee_data['employee_id']
            })

            doc_types_to_add = [doc.get('doc_type_id') for doc in documents]
            if len(doc_types_to_add) != len(set(doc_types_to_add)):
                raise Exception("Cannot add the same document type twice.")

            cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PTA_EMP_ARCH")
            new_archive_id = cursor.fetchone()[0]
            
            archive_query = "INSERT INTO LKP_PTA_EMP_ARCH (SYSTEM_ID, EMPLOYEE_ID, STATUS_ID, HIRE_DATE, DISABLED, LAST_UPDATE) VALUES (:1, :2, :3, TO_DATE(:4, 'YYYY-MM-DD'), '0', SYSDATE)"
            cursor.execute(archive_query, [new_archive_id, employee_data['employee_id'], employee_data['status_id'], employee_data.get('hireDate') if employee_data.get('hireDate') else None])

            for doc in documents:
                file_stream = doc['file'].stream
                file_stream.seek(0)
                
                _, file_extension = os.path.splitext(doc['file'].filename)
                app_id = get_app_id_from_extension(file_extension.lstrip('.').upper()) or 'UNKNOWN'

                sanitized_doc_type = re.sub(r'[^a-zA-Z0-9]', '_', doc['doc_type_name'])
                safe_docname = f"Archive_{employee_data['employeeNumber']}_{sanitized_doc_type}"
                
                dms_metadata = { 
                    "docname": safe_docname, 
                    "abstract": f"{doc['doc_type_name']} for {employee_data['name_en']}", 
                    "app_id": app_id, 
                    "filename": doc['file'].filename, 
                    "dms_user": dms_user 
                }
                
                docnumber = wsdl_client.upload_document_to_dms(dst, file_stream, dms_metadata)
                if not docnumber: raise Exception(f"Failed to upload {doc['doc_type_name']}")

                cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PTA_EMP_DOCS")
                new_doc_table_id = cursor.fetchone()[0]
                doc_query = "INSERT INTO LKP_PTA_EMP_DOCS (SYSTEM_ID, PTA_EMP_ARCH_ID, DOCNUMBER, DOC_TYPE_ID, EXPIRY, LEGISLATION_ID, DISABLED, LAST_UPDATE) VALUES (:1, :2, :3, :4, TO_DATE(:5, 'YYYY-MM-DD'), :6, '0', SYSDATE)"
                cursor.execute(doc_query, [new_doc_table_id, new_archive_id, docnumber, doc.get('doc_type_id'), doc.get('expiry') or None, doc.get('legislation_id') or None])
        
        conn.commit()
        return True, "Employee and documents archived successfully."
    except Exception as e:
        conn.rollback()
        return False, f"Transaction failed: {e}"
    finally:
        if conn: conn.close()

def update_archived_employee(dst, dms_user, archive_id, employee_data, new_documents, deleted_doc_ids):
    conn = get_connection()
    if not conn: return False, "Database connection failed."
    try:
        conn.begin()
        with conn.cursor() as cursor:
            # Update the archive status table
            update_query = "UPDATE LKP_PTA_EMP_ARCH SET STATUS_ID = :status_id, HIRE_DATE = TO_DATE(:hireDate, 'YYYY-MM-DD'), LAST_UPDATE = SYSDATE WHERE SYSTEM_ID = :archive_id"
            cursor.execute(update_query, {'status_id': employee_data['status_id'], 'hireDate': employee_data.get('hireDate') if employee_data.get('hireDate') else None, 'archive_id': archive_id})

            # Update the main employee details table
            hr_update_query = """
                UPDATE lkp_hr_employees
                SET JOB_NAME = :jobTitle, NATIONALITY = :nationality, EMAIL = :email,
                    MOBILE = :phone, SUPERVISORNAME = :manager,
                    DEPARTEMENT = :department, SECTION = :section
                WHERE SYSTEM_ID = :employee_id
            """
            cursor.execute(hr_update_query, {
                'jobTitle': employee_data.get('jobTitle'),
                'nationality': employee_data.get('nationality'),
                'email': employee_data.get('email'),
                'phone': employee_data.get('phone'),
                'manager': employee_data.get('manager'),
                'department': employee_data.get('department'),
                'section': employee_data.get('section'),
                'employee_id': employee_data['employee_id']
            })

            if deleted_doc_ids:
                cursor.executemany("UPDATE LKP_PTA_EMP_DOCS SET DISABLED = '1', LAST_UPDATE = SYSDATE WHERE SYSTEM_ID = :1", [[doc_id] for doc_id in deleted_doc_ids])

            cursor.execute("SELECT DOC_TYPE_ID FROM LKP_PTA_EMP_DOCS WHERE PTA_EMP_ARCH_ID = :1 AND DISABLED = '0'", [archive_id])
            existing_doc_type_ids = {row[0] for row in cursor.fetchall()}

            for doc in new_documents:
                if int(doc['doc_type_id']) in existing_doc_type_ids:
                    raise Exception(f"Document type '{doc['doc_type_name']}' already exists for this employee.")

                file_stream = doc['file'].stream
                file_stream.seek(0)
                
                _, file_extension = os.path.splitext(doc['file'].filename)
                app_id = get_app_id_from_extension(file_extension.lstrip('.').upper()) or 'UNKNOWN'

                sanitized_doc_type = re.sub(r'[^a-zA-Z0-9]', '_', doc['doc_type_name'])
                safe_docname = f"Archive_{employee_data['employeeNumber']}_{sanitized_doc_type}"
                
                dms_metadata = { "docname": safe_docname, "abstract": f"Updated document for {employee_data['name_en']}", "app_id": app_id, "filename": doc['file'].filename, "dms_user": dms_user }
                
                docnumber = wsdl_client.upload_document_to_dms(dst, file_stream, dms_metadata)
                if not docnumber: raise Exception(f"Failed to upload new document {doc['doc_type_name']}")
                
                cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PTA_EMP_DOCS")
                new_doc_table_id = cursor.fetchone()[0]
                
                doc_query = "INSERT INTO LKP_PTA_EMP_DOCS (SYSTEM_ID, PTA_EMP_ARCH_ID, DOCNUMBER, DOC_TYPE_ID, EXPIRY, LEGISLATION_ID, DISABLED, LAST_UPDATE) VALUES (:1, :2, :3, :4, TO_DATE(:5, 'YYYY-MM-DD'), :6, '0', SYSDATE)"
                cursor.execute(doc_query, [new_doc_table_id, archive_id, docnumber, doc.get('doc_type_id'), doc.get('expiry') or None, doc.get('legislation_id') or None])
        
        conn.commit()
        return True, "Employee archive updated successfully."
    except Exception as e:
        conn.rollback()
        return False, f"Update transaction failed: {e}"
    finally:
        if conn: conn.close()