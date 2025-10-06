@echo off
echo Starting EDMS Backend API Server...
call venv\Scripts\activate
waitress-serve --host=locahost --port=8443 app:app
