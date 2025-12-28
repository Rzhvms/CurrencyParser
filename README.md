/opt/homebrew/bin/python3.12 -m venv .venv   

# python3 -m venv .venv

source .venv/bin/activate   # Linux/macOS

# .venv\Scripts\activate    # Windows PowerShell

pip install -r requirements.txt

docker-compose up -d

uvicorn app.main:app