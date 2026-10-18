import sqlite3

conn = sqlite3.connect('/home/ubuntu/telegram-transfer-bot/cloner.db')
c = conn.cursor()
c.execute("""
    CREATE TABLE IF NOT EXISTS authorized_users (
        user_id INTEGER PRIMARY KEY,
        added_by INTEGER,
        username TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
conn.commit()
print("authorized_users table initialized successfully.")
