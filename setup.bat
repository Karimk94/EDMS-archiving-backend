@echo off
echo [1/2] Creating Python virtual environment in '.\venv\'...
if not exist venv (
    python -m venv venv
    echo Virtual environment created.
) else (
    echo Virtual environment already exists.
)

echo.
echo [2/2] Installing required packages from requirements.txt...
call venv\Scripts\activate.bat
pip install -r requirements.txt

echo.
echo Backend setup complete.
echo You can now run the API using the 'run.bat' script.
pause
