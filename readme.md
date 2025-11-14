EDMS Archiving Backend

This application is a standalone Python backend specifically for the EDMS Archiving Frontend. It has been migrated from a larger middleware project and contains only the necessary API endpoints and database logic required to support the archiving application.

Endpoints Provided

This application serves the following API endpoints:

/api/auth/pta-login (POST): Handles user login and session creation.

/api/auth/pta-user (GET): Retrieves the current user's session data.

/api/auth/user (GET): A generic endpoint to check user session (also used by the frontend).

/api/auth/logout (POST): Clears the user session.

/api/dashboard_counts (GET): Retrieves statistics for the dashboard.

/api/employees (GET): Fetches the list of archived employees.

/api/employees (POST): Adds a new employee to the archive.

/api/employees/<id> (GET): Fetches detailed information for a single archived employee.

/api/employees/<id> (PUT): Updates an existing archived employee's information and documents.

/api/hr_employees (GET): Fetches a paginated list of HR employees not yet archived.

/api/hr_employees/<id> (GET): Fetches details for a single HR employee.

/api/statuses (GET): Retrieves the list of possible employee statuses.

/api/document_types (GET): Retrieves the list of document types.

/api/legislations (GET): Retrieves the list of legislations.

/api/document/<docnumber> (GET): Securely downloads a document file from DMS.

Project Structure

migrated_archiving_app.py: The main Flask application file containing all API routes.

migrated_archiving_db_connector.py: Contains all Oracle database functions required by the app.

migrated_archiving_wsdl_client.py: Contains all SOAP/WSDL functions for communicating with the DMS.

migrated_requirements.txt: A list of Python dependencies required to run this application.

.env (You must create this): A file to store your environment variables (database credentials, WSDL URL, etc.).

Setup and Running

Create a virtual environment:

python -m venv venv


Activate the environment:

Windows: venv\Scripts\activate

macOS/Linux: source venv/bin/activate

Install dependencies:

pip install -r migrated_requirements.txt


Create a .env file in the same directory and add your environment variables:

DB_HOST=your_db_host
DB_PORT=your_db_port
DB_SERVICE_NAME=your_db_service
DB_USERNAME=your_db_user
DB_PASSWORD=your_db_password
WSDL_URL=http://your_dms_wsdl_url.svc?wsdl
DMS_USER=your_system_dms_user
DMS_PASSWORD=your_system_dms_password
FLASK_SECRET_KEY=generate_a_strong_random_secret_key


Run the application:

python migrated_archiving_app.py


The server will start (usually on http://localhost:5000).