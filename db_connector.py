import oracledb
import os
from dotenv import load_dotenv
import re
import logging
from datetime import datetime, timedelta
import wsdl_client

load_dotenv()

# --- Oracle Database Interaction ---

def get_connection():
    """Establishes a connection to the Oracle database."""
    try:
        dsn = f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_SERVICE_NAME')}"
        user = os.getenv('DB_USERNAME')
        password = os.getenv('DB_PASSWORD')
        if not all([user, password, dsn]):
            logging.error("Database connection details missing in environment variables.")
            return None
        return oracledb.connect(user=user, password=password, dsn=dsn)
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"DB connection error: {error.message} (Code: {error.code}, Context: {error.context})")
        return None

def get_app_id_from_extension(extension):
    """
    Looks up the APPLICATION (APP_ID) from the APPS table based on the file extension.
    Converts extension to uppercase for comparison.
    """
    conn = get_connection()
    if not conn:
        return None

    app_id = None
    upper_extension = extension.upper() if extension else ''
    try:
        with conn.cursor() as cursor:
            # First, check the DEFAULT_EXTENSION column (case-insensitive)
            cursor.execute("SELECT APPLICATION FROM APPS WHERE UPPER(DEFAULT_EXTENSION) = :ext", ext=upper_extension)
            result = cursor.fetchone()
            if result:
                app_id = result[0]
            else:
                # If not found, check the FILE_TYPES column (case-insensitive, using LIKE)
                cursor.execute("SELECT APPLICATION FROM APPS WHERE UPPER(FILE_TYPES) LIKE :ext_like",
                               ext_like=f"%{upper_extension}%")
                result = cursor.fetchone()
                if result:
                    app_id = result[0]
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_app_id_from_extension for '{extension}': {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
    return app_id

# --- Auth Functions ---
def get_pta_user_security_level(username):
    """Fetches the user's security level name from the database using their user ID from the PEOPLE table."""
    conn = get_connection()
    if not conn:
        return None  # Return None if DB connection fails

    security_level = None  # Default value is now None
    try:
        with conn.cursor() as cursor:
            # Use upper for case-insensitive comparison
            cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=username)
            user_result = cursor.fetchone()

            if user_result:
                user_id = user_result[0]

                # Now, get the security level using the user_id
                query = """
                        SELECT sl.NAME
                        FROM LKP_PTA_USR_SECUR us
                                 JOIN LKP_PTA_SECURITY sl ON us.SECURITY_LEVEL_ID = sl.SYSTEM_ID
                        WHERE us.USER_ID = :user_id \
                        """
                cursor.execute(query, user_id=user_id)
                level_result = cursor.fetchone()
                if level_result:
                    security_level = level_result[0]
                else:
                    logging.warning(f"No security level found for user_id {user_id} (DMS user: {username})")
            else:
                logging.warning(f"No PEOPLE record found for DMS user: {username}")
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_user_security_level for {username}: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
    return security_level

def get_pta_user_details(username):
    """Fetches user details including security level for PTA app."""
    conn = get_connection()
    if not conn:
        return None

    user_details = None
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=username)
            user_result = cursor.fetchone()

            if user_result:
                user_id = user_result[0]

                query = """
                        SELECT sl.NAME
                        FROM LKP_PTA_USR_SECUR us
                                 JOIN LKP_PTA_SECURITY sl ON us.SECURITY_LEVEL_ID = sl.SYSTEM_ID
                        WHERE us.USER_ID = :user_id \
                        """
                cursor.execute(query, user_id=user_id)
                details_result = cursor.fetchone()

                if details_result:
                    security_level = details_result[0]  # Get the first column (NAME)
                    user_details = {
                        'username': username,
                        'security_level': security_level,
                    }
                else:
                    logging.warning(f"No security details found for user_id {user_id} (DMS user: {username})")
            else:
                logging.warning(f"No PEOPLE record found for DMS user: {username}")

    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_pta_user_details for {username}: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

    return user_details

# --- Archiving Database Functions ---
def get_dashboard_counts():
    conn = get_connection()
    if not conn:
        return {
            "total_employees": 0,
            "active_employees": 0,
            "inactive_employees": 0,
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
                           SELECT COUNT(DISTINCT arch.SYSTEM_ID)
                           FROM LKP_PTA_EMP_ARCH arch
                                    JOIN LKP_PTA_EMP_STATUS stat ON arch.STATUS_ID = stat.SYSTEM_ID
                           WHERE TRIM(stat.NAME_ENGLISH) = 'Active'
                             AND EXISTS (SELECT 1
                                         FROM LKP_PTA_EMP_DOCS doc
                                                  JOIN LKP_PTA_DOC_TYPES dt ON doc.DOC_TYPE_ID = dt.SYSTEM_ID
                                         WHERE doc.PTA_EMP_ARCH_ID = arch.SYSTEM_ID
                                           AND (TRIM(dt.NAME) LIKE '%Judicial Card%' OR
                                                TRIM(dt.NAME) LIKE '%بطاقة الضبطية%')
                                           AND doc.DISABLED = '0')
                           """)
            counts["active_employees"] = cursor.fetchone()[0]

            # Inactive employees
            cursor.execute("""
                           SELECT COUNT(*)
                           FROM LKP_PTA_EMP_ARCH arch
                                    JOIN LKP_PTA_EMP_STATUS stat ON arch.STATUS_ID = stat.SYSTEM_ID
                           WHERE TRIM(stat.NAME_ENGLISH) = 'Inactive'
                           """)
            counts["inactive_employees"] = cursor.fetchone()[0]

            # Expiring Soon or Expired (in the next 30 days or already expired)
            cursor.execute("""
                           SELECT COUNT(DISTINCT arch.SYSTEM_ID)
                           FROM LKP_PTA_EMP_ARCH arch
                                    JOIN LKP_PTA_EMP_DOCS doc ON arch.SYSTEM_ID = doc.PTA_EMP_ARCH_ID
                                    JOIN LKP_PTA_DOC_TYPES dt ON doc.DOC_TYPE_ID = dt.SYSTEM_ID
                           WHERE doc.DISABLED = '0'
                             AND (TRIM(dt.NAME) LIKE '%Judicial Card%' OR TRIM(dt.NAME) LIKE '%بطاقة الضبطية%')
                             AND doc.EXPIRY IS NOT NULL
                             AND doc.EXPIRY < (SYSDATE + 30)
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
        where_clauses.append(
            "(UPPER(TRIM(hr.FULLNAME_EN)) LIKE :search OR UPPER(TRIM(hr.FULLNAME_AR)) LIKE :search OR TRIM(hr.EMPNO) LIKE :search)")
        params['search'] = f"%{search_term.upper()}%"
    if status:
        where_clauses.append("TRIM(stat.NAME_ENGLISH) = :status")
        params['status'] = status

    # Handle filter_type logic
    if filter_type == 'has_warrant':
        # MODIFIED: Find employees who HAVE a Judicial Card (per user request)
        where_clauses.append("""
                EXISTS (
                    SELECT 1
                    FROM LKP_PTA_EMP_DOCS doc
                    JOIN LKP_PTA_DOC_TYPES dt ON doc.DOC_TYPE_ID = dt.SYSTEM_ID
                    WHERE doc.PTA_EMP_ARCH_ID = arch.SYSTEM_ID
                      AND (TRIM(dt.NAME) LIKE '%Judicial Card%' OR TRIM(dt.NAME) LIKE '%بطاقة الضبطية%')
                      AND doc.DISABLED = '0'
                )
            """)
    elif filter_type == 'no_warrant':
        # MODIFIED: Find employees who DO NOT HAVE a Judicial Card
        where_clauses.append("""
                NOT EXISTS (
                    SELECT 1
                    FROM LKP_PTA_EMP_DOCS doc
                    JOIN LKP_PTA_DOC_TYPES dt ON doc.DOC_TYPE_ID = dt.SYSTEM_ID
                    WHERE doc.PTA_EMP_ARCH_ID = arch.SYSTEM_ID
                      AND (TRIM(dt.NAME) LIKE '%Judicial Card%' OR TRIM(dt.NAME) LIKE '%بطاقة الضبطية%')
                      AND doc.DISABLED = '0'
                )
            """)
    elif filter_type == 'expiring_soon_or_expired':
        # Find employees who have ANY document expiring soon or already expired
        where_clauses.append("""
                    EXISTS (
                        SELECT 1
                        FROM LKP_PTA_EMP_DOCS doc
                        JOIN LKP_PTA_DOC_TYPES dt ON doc.DOC_TYPE_ID = dt.SYSTEM_ID
                        WHERE doc.PTA_EMP_ARCH_ID = arch.SYSTEM_ID
                          AND (TRIM(dt.NAME) LIKE '%Judicial Card%' OR TRIM(dt.NAME) LIKE '%بطاقة الضبطية%')
                          AND doc.DISABLED = '0'
                          AND doc.EXPIRY IS NOT NULL
                          AND doc.EXPIRY < (SYSDATE + 30)
                    )
                """)

    final_where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    try:
        with conn.cursor() as cursor:
            count_query = f"SELECT COUNT(DISTINCT arch.SYSTEM_ID) {base_query} {final_where_clause}"
            cursor.execute(count_query, params)
            total_rows = cursor.fetchone()[0]

            fetch_query = f"""
                            SELECT DISTINCT arch.SYSTEM_ID, TRIM(hr.FULLNAME_EN) as FULLNAME_EN, TRIM(hr.FULLNAME_AR) as FULLNAME_AR, TRIM(hr.EMPNO) as EMPNO, TRIM(hr.DEPARTEMENT) as DEPARTMENT, TRIM(hr.SECTION) as SECTION,
                                   TRIM(stat.NAME_ENGLISH) as STATUS_EN, TRIM(stat.NAME_ARABIC) as STATUS_AR
                            {base_query} {final_where_clause} ORDER BY arch.SYSTEM_ID DESC
                        """

            if page_size > 0:
                fetch_query += " OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY"
                params.update({'offset': offset, 'page_size': page_size})
            else:
                params.pop('offset', None)
                params.pop('page_size', None)

            cursor.execute(fetch_query, params)

            columns = [c[0].lower() for c in cursor.description]
            employees = []
            for row in cursor.fetchall():
                emp = dict(zip(columns, row))

                # Get status of the Warrant Decision document
                cursor.execute("""
                               SELECT doc.EXPIRY
                               FROM LKP_PTA_EMP_DOCS doc
                                        JOIN LKP_PTA_DOC_TYPES dt ON doc.DOC_TYPE_ID = dt.SYSTEM_ID
                               WHERE doc.PTA_EMP_ARCH_ID = :1 
                      AND (TRIM(dt.NAME) LIKE '%Warrant Decisions%' OR TRIM(dt.NAME) LIKE '%القرارات الخاصة بالضبطية%') 
                      AND doc.DISABLED = '0'
                               ORDER BY doc.EXPIRY DESC
                                   FETCH FIRST 1 ROWS ONLY
                               """, [emp['system_id']])
                warrant_decision_doc = cursor.fetchone()

                if warrant_decision_doc:
                    expiry_date = warrant_decision_doc[0]
                    if expiry_date:
                        if expiry_date >= datetime.now().date():
                            emp['warrant_status'] = 'فعالة / Active'
                        else:
                            emp['warrant_status'] = 'منتهية / Expired'
                    else:
                        emp['warrant_status'] = 'توجد / Yes'  # Exists but no expiry date
                else:
                    emp['warrant_status'] = 'لا توجد / No'

                # Get status of the Judicial Card document
                cursor.execute("""
                               SELECT doc.EXPIRY
                               FROM LKP_PTA_EMP_DOCS doc
                                        JOIN LKP_PTA_DOC_TYPES dt ON doc.DOC_TYPE_ID = dt.SYSTEM_ID
                               WHERE doc.PTA_EMP_ARCH_ID = :1 
                      AND (TRIM(dt.NAME) LIKE '%Judicial Card%' OR TRIM(dt.NAME) LIKE '%بطاقة الضبطية%') 
                      AND doc.DISABLED = '0'
                               ORDER BY doc.EXPIRY DESC
                                   FETCH FIRST 1 ROWS ONLY
                               """, [emp['system_id']])
                judicial_card = cursor.fetchone()

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
                        emp['card_status_class'] = 'valid'  # No expiry date, assume valid
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
            cursor.execute(
                "SELECT SYSTEM_ID, TRIM(FULLNAME_EN) as FULLNAME_EN, TRIM(FULLNAME_AR) as FULLNAME_AR, TRIM(EMPNO) as EMPNO, TRIM(DEPARTEMENT) as DEPARTMENT, TRIM(SECTION) as SECTION, TRIM(EMAIL) as EMAIL, TRIM(MOBILE) as MOBILE, TRIM(SUPERVISORNAME) as SUPERVISORNAME, TRIM(NATIONALITY) as NATIONALITY, TRIM(JOB_NAME) as JOB_NAME FROM lkp_hr_employees WHERE SYSTEM_ID = :1",
                [employee_id])
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
            cursor.execute(
                "SELECT SYSTEM_ID, TRIM(NAME_ENGLISH) as NAME_ENGLISH, TRIM(NAME_ARABIC) as NAME_ARABIC FROM LKP_PTA_EMP_STATUS WHERE DISABLED='0'")
            statuses['employee_status'] = [dict(zip([c[0].lower() for c in cursor.description], row)) for row in
                                           cursor.fetchall()]
    finally:
        if conn: conn.close()
    return statuses

def fetch_document_types():
    conn = get_connection()
    if not conn: return {"all_types": [], "types_with_expiry": []}
    doc_types = {"all_types": [], "types_with_expiry": []}
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT SYSTEM_ID, TRIM(NAME) as NAME, HAS_EXPIRY FROM LKP_PTA_DOC_TYPES WHERE DISABLED = '0' ORDER BY SYSTEM_ID")
            for row in cursor:
                doc_type_obj = {'system_id': row[0], 'name': row[1]}
                doc_types['all_types'].append(doc_type_obj)
                if row[2] and str(row[2]).strip() == '1':
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
            cursor.execute(
                "SELECT SYSTEM_ID, TRIM(NAME) as NAME FROM LKP_PTA_LEGISL WHERE DISABLED = '0' ORDER BY NAME")
            legislations = [dict(zip([c[0].lower() for c in cursor.description], row)) for row in
                            cursor.fetchall()]
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
            if employee_details.get('hire_date') and hasattr(employee_details['hire_date'], 'strftime'):
                employee_details['hire_date'] = employee_details['hire_date'].strftime('%Y-%m-%d')

            doc_query = """
                        SELECT d.SYSTEM_ID, d.DOCNUMBER, d.DOC_TYPE_ID, d.EXPIRY, TRIM(dt.NAME) as DOC_NAME
                        FROM LKP_PTA_EMP_DOCS d
                                 JOIN LKP_PTA_DOC_TYPES dt ON d.DOC_TYPE_ID = dt.SYSTEM_ID
                        WHERE d.PTA_EMP_ARCH_ID = :1 AND d.DISABLED = '0' \
                        """
            cursor.execute(doc_query, [archive_id])
            doc_columns = [col[0].lower() for col in cursor.description]
            documents = []

            for doc_row in cursor.fetchall():
                doc_dict = dict(zip(doc_columns, doc_row))
                if doc_dict.get('expiry') and hasattr(doc_dict['expiry'], 'strftime'):
                    doc_dict['expiry'] = doc_dict['expiry'].strftime('%Y-%m-%d')

                doc_dict['legislation_ids'] = []
                doc_dict['legislation_names'] = []
                leg_query = """
                            SELECT dl.LEGISLATION_ID, TRIM(l.NAME)
                            FROM LKP_PTA_DOC_LEGISL dl
                                     JOIN LKP_PTA_LEGISL l ON dl.LEGISLATION_ID = l.SYSTEM_ID
                            WHERE dl.DOC_ID = :1 \
                            """
                cursor.execute(leg_query, [doc_dict['system_id']])
                for leg_row in cursor.fetchall():
                    doc_dict['legislation_ids'].append(leg_row[0])
                    doc_dict['legislation_names'].append(leg_row[1])

                documents.append(doc_dict)

            employee_details['documents'] = documents
    finally:
        if conn: conn.close()
    return employee_details

def add_employee_archive_with_docs(dst, dms_user, employee_data, documents):
    conn = get_connection()
    if not conn: return False, "Database connection failed."
    try:
        conn.begin()
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM LKP_PTA_EMP_ARCH WHERE EMPLOYEE_ID = :1",
                           [employee_data['employee_id']])
            if cursor.fetchone()[0] > 0: return False, "This employee is already in the archive."

            # Update lkp_hr_employees with any changes from the form
            hr_update_query = """
                              UPDATE lkp_hr_employees
                              SET JOB_NAME       = :jobTitle, \
                                  NATIONALITY    = :nationality, \
                                  EMAIL          = :email,
                                  MOBILE         = :phone, \
                                  SUPERVISORNAME = :manager,
                                  DEPARTEMENT    = :department, \
                                  SECTION        = :section
                              WHERE SYSTEM_ID = :employee_id \
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
            cursor.execute(archive_query, [new_archive_id, employee_data['employee_id'],
                                           employee_data['status_id'],
                                           employee_data.get('hireDate') if employee_data.get(
                                               'hireDate') else None])

            for doc in documents:
                file_stream = doc['file'].stream
                file_stream.seek(0)

                sanitized_doc_type = re.sub(r'[^a-zA-Z0-9]', '_', doc['doc_type_name'])
                safe_docname = f"Archive_{employee_data['employeeNumber']}_{sanitized_doc_type}"

                _, file_extension = os.path.splitext(doc['file'].filename)
                app_id = get_app_id_from_extension(file_extension.lstrip('.').upper()) or 'UNKNOWN'

                dms_metadata = {
                    "docname": safe_docname,
                    "abstract": f"{doc['doc_type_name']} for {employee_data['name_en']}",
                    "filename": doc['file'].filename,
                    "dms_user": dms_user,
                    "app_id": app_id
                }

                docnumber = wsdl_client.upload_archive_document_to_dms(dst, file_stream, dms_metadata)
                if not docnumber: raise Exception(f"Failed to upload {doc['doc_type_name']}")

                cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PTA_EMP_DOCS")
                new_doc_table_id = cursor.fetchone()[0]
                doc_query = "INSERT INTO LKP_PTA_EMP_DOCS (SYSTEM_ID, PTA_EMP_ARCH_ID, DOCNUMBER, DOC_TYPE_ID, EXPIRY, DISABLED, LAST_UPDATE) VALUES (:1, :2, :3, :4, TO_DATE(:5, 'YYYY-MM-DD'), '0', SYSDATE)"
                cursor.execute(doc_query, [new_doc_table_id, new_archive_id, docnumber, doc.get('doc_type_id'),
                                           doc.get('expiry') or None])

                # Handle multiple legislations
                legislation_ids = doc.get('legislation_ids')
                if legislation_ids and isinstance(legislation_ids, list):
                    for leg_id in legislation_ids:
                        if leg_id:  # Ensure not empty
                            cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PTA_DOC_LEGISL")
                            new_leg_link_id = cursor.fetchone()[0]
                            leg_query = "INSERT INTO LKP_PTA_DOC_LEGISL (SYSTEM_ID, DOC_ID, LEGISLATION_ID) VALUES (:1, :2, :3)"
                            cursor.execute(leg_query, [new_leg_link_id, new_doc_table_id, leg_id])

        conn.commit()
        return True, "Employee and documents archived successfully."
    except Exception as e:
        conn.rollback()
        logging.error(f"Error in add_employee_archive_with_docs: {e}", exc_info=True)
        return False, f"Transaction failed: {e}"
    finally:
        if conn: conn.close()

def update_archived_employee(dst, dms_user, archive_id, employee_data, new_documents, deleted_doc_ids,
                             updated_documents):
    conn = get_connection()
    if not conn: return False, "Database connection failed."
    try:
        conn.begin()
        with conn.cursor() as cursor:
            # Update the archive status table
            update_query = "UPDATE LKP_PTA_EMP_ARCH SET STATUS_ID = :status_id, HIRE_DATE = TO_DATE(:hireDate, 'YYYY-MM-DD'), LAST_UPDATE = SYSDATE WHERE SYSTEM_ID = :archive_id"
            cursor.execute(update_query, {'status_id': employee_data['status_id'],
                                          'hireDate': employee_data.get('hireDate') if employee_data.get(
                                              'hireDate') else None, 'archive_id': archive_id})

            # Update the main employee details table
            hr_update_query = """
                              UPDATE lkp_hr_employees
                              SET JOB_NAME       = :jobTitle, \
                                  NATIONALITY    = :nationality, \
                                  EMAIL          = :email,
                                  MOBILE         = :phone, \
                                  SUPERVISORNAME = :manager,
                                  DEPARTEMENT    = :department, \
                                  SECTION        = :section
                              WHERE SYSTEM_ID = :employee_id \
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
                # First delete from the junction table to maintain referential integrity
                for doc_id in deleted_doc_ids:
                    cursor.execute("DELETE FROM LKP_PTA_DOC_LEGISL WHERE DOC_ID = :1", [doc_id])

                # Then mark the document as disabled
                cursor.executemany(
                    "UPDATE LKP_PTA_EMP_DOCS SET DISABLED = '1', LAST_UPDATE = SYSDATE WHERE SYSTEM_ID = :1",
                    [[doc_id] for doc_id in deleted_doc_ids])

            # Handle updated documents' legislations
            if updated_documents:
                for doc in updated_documents:
                    doc_id = doc.get('system_id')
                    legislation_ids = doc.get('legislation_ids', [])

                    # Clear existing legislations for this document
                    cursor.execute("DELETE FROM LKP_PTA_DOC_LEGISL WHERE DOC_ID = :1", [doc_id])

                    # Add the new set of legislations
                    for leg_id in legislation_ids:
                        if leg_id:
                            cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PTA_DOC_LEGISL")
                            new_leg_link_id = cursor.fetchone()[0]
                            leg_query = "INSERT INTO LKP_PTA_DOC_LEGISL (SYSTEM_ID, DOC_ID, LEGISLATION_ID) VALUES (:1, :2, :3)"
                            cursor.execute(leg_query, [new_leg_link_id, doc_id, leg_id])

            cursor.execute("SELECT DOC_TYPE_ID FROM LKP_PTA_EMP_DOCS WHERE PTA_EMP_ARCH_ID = :1 AND DISABLED = '0'",
                           [archive_id])
            existing_doc_type_ids = {row[0] for row in cursor.fetchall()}

            for doc in new_documents:
                if int(doc['doc_type_id']) in existing_doc_type_ids:
                    raise Exception(f"Document type '{doc['doc_type_name']}' already exists for this employee.")

                file_stream = doc['file'].stream
                file_stream.seek(0)

                sanitized_doc_type = re.sub(r'[^a-zA-Z0-9]', '_', doc['doc_type_name'])
                safe_docname = f"Archive_{employee_data['employeeNumber']}_{sanitized_doc_type}"

                _, file_extension = os.path.splitext(doc['file'].filename)
                app_id = get_app_id_from_extension(file_extension.lstrip('.').upper()) or 'UNKNOWN'

                dms_metadata = {"docname": safe_docname,
                                "abstract": f"Updated document for {employee_data['name_en']}",
                                "filename": doc['file'].filename,
                                "dms_user": dms_user,
                                "app_id": app_id
                                }

                docnumber = wsdl_client.upload_archive_document_to_dms(dst, file_stream, dms_metadata)
                if not docnumber: raise Exception(f"Failed to upload new document {doc['doc_type_name']}")

                cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PTA_EMP_DOCS")
                new_doc_table_id = cursor.fetchone()[0]

                doc_query = "INSERT INTO LKP_PTA_EMP_DOCS (SYSTEM_ID, PTA_EMP_ARCH_ID, DOCNUMBER, DOC_TYPE_ID, EXPIRY, DISABLED, LAST_UPDATE) VALUES (:1, :2, :3, :4, TO_DATE(:5, 'YYYY-MM-DD'), '0', SYSDATE)"
                cursor.execute(doc_query,
                               [new_doc_table_id, archive_id, docnumber, doc.get('doc_type_id'),
                                doc.get('expiry') or None])

                # Handle multiple legislations
                legislation_ids = doc.get('legislation_ids')
                if legislation_ids and isinstance(legislation_ids, list):
                    for leg_id in legislation_ids:
                        if leg_id:  # Ensure not empty
                            cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PTA_DOC_LEGISL")
                            new_leg_link_id = cursor.fetchone()[0]
                            leg_query = "INSERT INTO LKP_PTA_DOC_LEGISL (SYSTEM_ID, DOC_ID, LEGISLATION_ID) VALUES (:1, :2, :3)"
                            cursor.execute(leg_query, [new_leg_link_id, new_doc_table_id, leg_id])

        conn.commit()
        return True, "Employee archive updated successfully."
    except Exception as e:
        conn.rollback()
        logging.error(f"Error in update_archived_employee: {e}", exc_info=True)
        return False, f"Update transaction failed: {e}"
    finally:
        if conn: conn.close()

def bulk_add_employees_from_excel(employees_data):
    conn = get_connection()
    if not conn:
        return 0, len(employees_data), ["Database connection failed."]

    success_count = 0
    fail_count = 0
    errors = []

    try:
        with conn.cursor() as cursor:
            # Get all statuses and HR employees into maps for efficiency
            cursor.execute("SELECT SYSTEM_ID, TRIM(NAME_ENGLISH) FROM LKP_PTA_EMP_STATUS")
            status_map = {name.upper(): sid for sid, name in cursor.fetchall() if name}

            cursor.execute("SELECT SYSTEM_ID, TRIM(EMPNO) FROM lkp_hr_employees")
            hr_map = {empno: sid for sid, empno in cursor.fetchall() if empno}

            cursor.execute("SELECT EMPLOYEE_ID FROM LKP_PTA_EMP_ARCH WHERE EMPLOYEE_ID IS NOT NULL")
            archived_ids = {row[0] for row in cursor.fetchall()}

            inactive_status_id = status_map.get('INACTIVE')
            if not inactive_status_id:
                logging.warning("Default status 'Inactive' not found in LKP_PTA_EMP_STATUS.")
                # Try to get *any* status if 'Inactive' is missing
                if status_map:
                    inactive_status_id = next(iter(status_map.values()))
                else:
                    inactive_status_id = None  # Last resort

            for index, emp in enumerate(employees_data):
                row_num = index + 2  # For error reporting (since header is row 1)
                try:
                    empno = str(emp.get('empno')).strip()
                    if not empno:
                        errors.append(f"Row {row_num}: Missing Employee ID (empno).")
                        fail_count += 1
                        continue

                    employee_id = hr_map.get(empno)
                    if not employee_id:
                        errors.append(
                            f"Row {row_num}: Employee ID '{empno}' not found in HR system (lkp_hr_employees).")
                        fail_count += 1
                        continue

                    if employee_id in archived_ids:
                        errors.append(f"Row {row_num}: Employee '{empno}' is already archived.")
                        fail_count += 1
                        continue

                    status_name = str(emp.get('status_name', '')).strip().upper()
                    status_id = status_map.get(status_name, inactive_status_id)  # Default to inactive

                    # 1. Update lkp_hr_employees
                    hr_update_query = """
                                      UPDATE lkp_hr_employees
                                      SET FULLNAME_EN    = :fullname_en,
                                          FULLNAME_AR    = :fullname_ar,
                                          NATIONALITY    = :nationality,
                                          JOB_NAME       = :job_name,
                                          SUPERVISORNAME = :manager,
                                          MOBILE         = :phone,
                                          EMAIL          = :email,
                                          SECTION        = :section,
                                          DEPARTEMENT    = :department
                                      WHERE SYSTEM_ID = :employee_id \
                                      """
                    cursor.execute(hr_update_query, {
                        'fullname_en': emp.get('name_en'),
                        'fullname_ar': emp.get('name_ar'),
                        'nationality': emp.get('nationality'),
                        'job_name': emp.get('job_title'),
                        'manager': emp.get('manager'),
                        'phone': emp.get('phone'),
                        'email': emp.get('email'),
                        'section': emp.get('section'),
                        'department': emp.get('department'),
                        'employee_id': employee_id
                    })

                    # 2. Insert into LKP_PTA_EMP_ARCH
                    cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PTA_EMP_ARCH")
                    new_archive_id = cursor.fetchone()[0]

                    hire_date = emp.get('hire_date')

                    archive_query = """
                                    INSERT INTO LKP_PTA_EMP_ARCH
                                        (SYSTEM_ID, EMPLOYEE_ID, STATUS_ID, HIRE_DATE, DISABLED, LAST_UPDATE)
                                    VALUES (:1, :2, :3, TO_DATE(:4, 'DD/MM/YYYY'), '0', SYSDATE) \
                                    """
                    cursor.execute(archive_query, [
                        new_archive_id,
                        employee_id,
                        status_id,
                        hire_date if hire_date else None
                    ])

                    success_count += 1
                    archived_ids.add(employee_id)  # Add to set to prevent duplicates in same run

                except oracledb.Error as db_err:
                    errors.append(f"Row {row_num} (EmpID: {emp.get('empno')}): DB Error - {db_err}")
                    fail_count += 1
                except Exception as e:
                    errors.append(f"Row {row_num} (EmpID: {emp.get('empno')}): General Error - {str(e)}")
                    fail_count += 1

        if fail_count > 0:
            conn.rollback()
            errors.insert(0, "Transaction rolled back due to errors. No employees were added.")
            return 0, fail_count, errors
        else:
            conn.commit()
            return success_count, fail_count, errors

    except Exception as e:
        conn.rollback()
        logging.error(f"Error in bulk_add_employees_from_excel: {e}", exc_info=True)
        return 0, len(employees_data), [f"An unexpected transaction error occurred: {str(e)}"]

    finally:
        if conn:
            conn.close()