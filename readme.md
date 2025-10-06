EDMS Backend API
This Flask application serves as the backend API for the Employee Document Management System. It handles business logic, database connections to Oracle, and communication with the DMS (SOAP).

Local Development
1. Prerequisites
Python 3.8+

Oracle Instant Client installed and configured in your system's PATH.

2. Setup
Run the setup script to create a virtual environment and install dependencies:

setup.bat

3. Configuration
Create a file named .env in this directory.

Copy the contents of .env.sample into it.

Fill in the values for your database, WSDL URL, and a default DMS user. The FLASK_SECRET_KEY can be any long, random string.

4. Running the Server
Execute the run script:

run.bat

The API server will start and be accessible at http://localhost:5000.

Deployment on Windows Server with IIS
1. Prerequisites
IIS with HttpPlatformHandler installed.

A dedicated application pool for the site.

Python installed on the server.

Oracle Instant Client installed on the server.

2. Deployment Steps
Package Application: Run the bundle.bat script from the project root. This will create a deployment.zip file.

Transfer and Unzip: Copy deployment.zip to your server, for example, C:\inetpub\wwwroot\, and unzip it. Your application files should be in a structure like C:\inetpub\wwwroot\edms_app\.

Setup Python: Navigate to C:\inetpub\wwwroot\edms_app\backend on the server and run setup.bat to create the virtual environment.

Configure IIS:

Create a new website in IIS Manager.

Point the "Physical path" to the frontend folder: C:\inetpub\wwwroot\edms_app\frontend.

Assign the site to its dedicated application pool.

The web.config in the backend directory is for reference. IIS uses URL Rewrite rules to direct API calls.

Set Environment Variables: In IIS, find "Configuration Editor". In the system.webServer/httpPlatform section, add the production environment variables (database credentials, etc.).

Set Permissions: Ensure the user account for your application pool has read/execute permissions on the backend folder and its venv, and read/write permissions on the python_logs.log file.