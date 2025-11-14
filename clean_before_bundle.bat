@echo off
REM =================================================================
REM  Clean Script for EDMS Middleware API
REM  - Deletes all __pycache__ folders and .pyc files.
REM  - Run this BEFORE zipping the project for deployment.
REM =================================================================

echo [1/2] Changing directory to the script's location...
cd /d "%~dp0"

echo [2/2] Deleting Python cache files...
for /d /r . %%d in (__pycache__) do (
    if exist "%%d" (
        echo Deleting %%d
        rd /s /q "%%d"
    )
)

echo.
echo Cleaning complete. You can now zip the project folder.
pause