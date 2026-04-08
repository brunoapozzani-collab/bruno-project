@echo off
REM Windows launcher - double-click to start the app.
cd /d "%~dp0"
if not exist venv (
  echo Criando ambiente Python...
  python -m venv venv
  call venv\Scripts\activate.bat
  pip install -r requirements.txt
) else (
  call venv\Scripts\activate.bat
)
streamlit run app.py
pause
