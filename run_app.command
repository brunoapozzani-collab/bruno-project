#!/bin/bash
# Mac launcher — double-click to start the app.
cd "$(dirname "$0")"
if [ ! -d "venv" ]; then
  echo "Criando ambiente Python..."
  python3 -m venv venv
  source venv/bin/activate
  pip install -r requirements.txt
else
  source venv/bin/activate
fi
streamlit run app.py
