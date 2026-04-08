@echo off
call venv\Scripts\activate.bat
python tools\extract_fixed_expenses.py %*
pause
