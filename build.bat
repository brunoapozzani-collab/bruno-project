@echo off
REM Build standalone Thais.exe (Windows only).
call venv\Scripts\activate.bat
pyinstaller --onefile --name Thais ^
    --add-data "config;config" ^
    --add-data ".env;." ^
    tools\extract_fixed_expenses.py
echo.
echo Build concluido. Executavel em dist\Thais.exe
pause
