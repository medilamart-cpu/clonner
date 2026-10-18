import sqlite3

db = sqlite3.connect('/home/ubuntu/telegram-transfer-bot/cloner.db')
c = db.cursor()
c.execute('SELECT count(1) FROM cloned_messages')
print('Cloned so far:', c.fetchone()[0])
c.execute('SELECT max(source_msg_id) FROM cloned_messages')
print('Max message ID:', c.fetchone()[0])
