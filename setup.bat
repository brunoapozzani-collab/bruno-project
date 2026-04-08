@echo off
REM One-time setup. Run from the project folder.
where python >nul 2>nul
if errorlevel 1 (
    echo Python nao encontrado. Instale Python 3.11+ de https://www.python.org/downloads/
    pause
    exit /b 1
)
python -m venv venv
call venv\Scripts\activate.bat
python -m pip install --upgrade pip
pip install -r requirements.txt
echo.
echo Setup concluido. Edite o arquivo .env com seu token Dropbox.
pause
