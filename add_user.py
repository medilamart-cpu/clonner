import sys
import sqlite3

if len(sys.argv) < 2:
    print("Usage: python3 add_user.py <user_id> [username]")
    sys.exit(1)

user_id = int(sys.argv[1].strip().replace("@", ""))
username = sys.argv[2].strip().replace("@", "") if len(sys.argv) > 2 else None

conn = sqlite3.connect('/home/ubuntu/telegram-transfer-bot/cloner.db')
c = conn.cursor()
c.execute("""
    INSERT OR REPLACE INTO authorized_users (user_id, added_by, username)
    VALUES (?, ?, ?)
""", (user_id, 599085830, username))
conn.commit()

print(f"✅ User {user_id} (@{username or 'None'}) has been added as an authorized user!")
