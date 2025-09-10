import sqlite3
from app.config import settings
def get_db():
    conn = sqlite3.connect(settings.DB_PATH, check_same_thread=False)
    conn.execute("CREATE TABLE IF NOT EXISTS signals(id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, ticker TEXT, prob REAL, signal TEXT)")
    return conn
_db = get_db()
