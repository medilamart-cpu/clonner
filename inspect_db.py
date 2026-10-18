import sqlite3

conn = sqlite3.connect('/home/ubuntu/telegram-transfer-bot/cloner.db')
c = conn.cursor()
c.execute('SELECT source_msg_id FROM cloned_messages ORDER BY source_msg_id')
all_cloned = set(r[0] for r in c.fetchall())

c.execute('SELECT min(source_msg_id), max(source_msg_id), count(*) FROM cloned_messages')
min_id, max_id, count = c.fetchone()
print(f"Total cloned in DB: {count}")
print(f"Range: ID {min_id} to ID {max_id}")

missing = [i for i in range(1, max_id + 1) if i not in all_cloned]
print(f"Total skipped/missing IDs from 1 to {max_id}: {len(missing)}")
print("Missing IDs:")
print(missing)

c.execute('SELECT source_topic_id, dest_topic_id, title FROM topic_mappings')
topics = c.fetchall()
print("\nTopic Mappings:")
for t in topics:
    print(f"  Source Topic ID: {t[0]} -> Dest Topic ID: {t[1]} ({t[2]})")
