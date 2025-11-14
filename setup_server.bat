@echo off
REM =================================================================
REM  Server Setup Script for EDMS Middleware API (Offline)
REM  - Creates a new virtual environment.
REM  - Installs packages from the local 'wheels' folder (no internet).
REM =================================================================

echo [1/3] Changing directory to the script's location...
cd /d "%~dp0"

echo [2/3] Creating a new, clean Python virtual environment...
python -m venv venv

echo [3/3] Installing packages from the local 'wheels' folder...
call venv\Scripts\activate.bat
pip install --no-index --find-links=./wheels -r requirements.txt

echo.
echo Server setup is complete.
echo Please ensure your web.config points to the new venv path.
pause