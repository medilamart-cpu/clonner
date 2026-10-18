import sqlite3
import os
from typing import Optional, Dict

class Database:
    def __init__(self, db_path: str = "cloner.db"):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Topic mappings
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS topic_mappings (
                    source_topic_id INTEGER,
                    dest_topic_id INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    dest_chat_id INTEGER,
                    PRIMARY KEY (dest_chat_id, source_topic_id)
                )
            """)
            try:
                cursor.execute("PRAGMA table_info(topic_mappings)")
                cols = [info[1] for info in cursor.fetchall()]
                if "dest_chat_id" not in cols:
                    cursor.execute("ALTER TABLE topic_mappings ADD COLUMN dest_chat_id INTEGER")
            except Exception:
                pass
            # Cloned messages to prevent duplicates and allow resume
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cloned_messages (
                    source_msg_id INTEGER,
                    dest_msg_id INTEGER,
                    source_topic_id INTEGER,
                    source_chat_id INTEGER,
                    cloned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (source_chat_id, source_msg_id)
                )
            """)
            try:
                cursor.execute("PRAGMA table_info(cloned_messages)")
                cols = [info[1] for info in cursor.fetchall()]
                if "source_chat_id" not in cols:
                    cursor.execute("ALTER TABLE cloned_messages ADD COLUMN source_chat_id INTEGER")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_cloned_chat_msg ON cloned_messages(source_chat_id, source_msg_id)")
            except Exception:
                pass
            # Key-value state tracking
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cloner_state (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            # Authorized users with permissions
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS authorized_users (
                    user_id INTEGER PRIMARY KEY,
                    added_by INTEGER,
                    username TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Caption modification rules (remove, replace, prefix, suffix)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS caption_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rule_type TEXT NOT NULL,
                    pattern TEXT NOT NULL,
                    replacement TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # User sessions for multi-account login
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_sessions (
                    user_id INTEGER PRIMARY KEY,
                    session_string TEXT NOT NULL,
                    phone_number TEXT,
                    first_name TEXT,
                    username TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Multi-profile setups per user
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_setups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    setup_name TEXT NOT NULL,
                    source_peer TEXT NOT NULL,
                    dest_peer TEXT NOT NULL,
                    source_title TEXT,
                    dest_title TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    # Topic mapping methods
    def save_topic_mapping(self, source_topic_id: int, dest_topic_id: int, title: str, dest_chat_id: Optional[int] = None):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if dest_chat_id is not None:
                cursor.execute("""
                    INSERT OR REPLACE INTO topic_mappings (source_topic_id, dest_topic_id, title, dest_chat_id)
                    VALUES (?, ?, ?, ?)
                """, (source_topic_id, dest_topic_id, title, dest_chat_id))
            else:
                cursor.execute("""
                    INSERT OR REPLACE INTO topic_mappings (source_topic_id, dest_topic_id, title)
                    VALUES (?, ?, ?)
                """, (source_topic_id, dest_topic_id, title))
            conn.commit()

    def get_dest_topic_id(self, source_topic_id: Optional[int], dest_chat_id: Optional[int] = None) -> Optional[int]:
        if source_topic_id is None:
            return None
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if dest_chat_id is not None:
                cursor.execute("""
                    SELECT dest_topic_id FROM topic_mappings 
                    WHERE source_topic_id = ? AND dest_chat_id = ?
                    LIMIT 1
                """, (source_topic_id, dest_chat_id))
            else:
                cursor.execute("SELECT dest_topic_id FROM topic_mappings WHERE source_topic_id = ? AND dest_chat_id IS NULL", (source_topic_id,))
            row = cursor.fetchone()
            return row[0] if row else None

    def get_all_topic_mappings(self, dest_chat_id: Optional[int] = None) -> Dict[int, int]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if dest_chat_id is not None:
                cursor.execute("""
                    SELECT source_topic_id, dest_topic_id FROM topic_mappings 
                    WHERE dest_chat_id = ?
                """, (dest_chat_id,))
            else:
                cursor.execute("SELECT source_topic_id, dest_topic_id FROM topic_mappings WHERE dest_chat_id IS NULL")
            return {row[0]: row[1] for row in cursor.fetchall()}

    # Message cloning methods
    def is_message_cloned(self, source_msg_id: int, source_chat_id: Optional[int] = None) -> bool:
        if source_chat_id is None:
            return False
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 1 FROM cloned_messages 
                WHERE source_msg_id = ? AND source_chat_id = ?
            """, (source_msg_id, source_chat_id))
            return cursor.fetchone() is not None

    def record_cloned_message(self, source_msg_id: int, dest_msg_id: Optional[int], source_topic_id: Optional[int], source_chat_id: Optional[int] = None):
        # Only record successfully cloned messages (dest_msg_id must not be None)
        if dest_msg_id is None or source_chat_id is None:
            return
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO cloned_messages (source_msg_id, dest_msg_id, source_topic_id, source_chat_id)
                VALUES (?, ?, ?, ?)
            """, (source_msg_id, dest_msg_id, source_topic_id, source_chat_id))
            conn.commit()

    def get_cloned_count(self, source_chat_id: Optional[int] = None) -> int:
        if source_chat_id is None:
            return 0
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT count(*) FROM cloned_messages WHERE source_chat_id = ?", (source_chat_id,))
            row = cursor.fetchone()
            return row[0] if row else 0

    def clear_cloned_messages(self, source_chat_id: Optional[int] = None):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if source_chat_id is not None:
                cursor.execute("DELETE FROM cloned_messages WHERE source_chat_id = ?", (source_chat_id,))
            else:
                cursor.execute("DELETE FROM cloned_messages")
            conn.commit()

    # User authorization methods
    def add_authorized_user(self, user_id: int, added_by: Optional[int] = None, username: Optional[str] = None) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO authorized_users (user_id, added_by, username)
                VALUES (?, ?, ?)
            """, (user_id, added_by, username))
            conn.commit()
            return True

    def remove_authorized_user(self, user_id: int) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM authorized_users WHERE user_id = ?", (user_id,))
            conn.commit()
            return cursor.rowcount > 0

    def get_authorized_users(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id, added_by, username, created_at FROM authorized_users ORDER BY created_at ASC")
            return cursor.fetchall()

    def is_user_authorized(self, user_id: int) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM authorized_users WHERE user_id = ?", (user_id,))
            return cursor.fetchone() is not None

    # State key-value storage methods
    def set_state(self, key: str, value: str):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO cloner_state (key, value)
                VALUES (?, ?)
            """, (key, str(value)))
            conn.commit()

    def get_state(self, key: str) -> Optional[str]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM cloner_state WHERE key = ?", (key,))
            row = cursor.fetchone()
            return row[0] if row else None

    # Persistent User Setup Profiles (Multi-setup support per user)
    def add_user_setup(self, user_id: int, setup_name: str, source_peer: str, dest_peer: str, source_title: Optional[str] = None, dest_title: Optional[str] = None) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO user_setups (user_id, setup_name, source_peer, dest_peer, source_title, dest_title)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (user_id, setup_name, source_peer, dest_peer, source_title, dest_title))
            conn.commit()
            return cursor.lastrowid

    def get_user_setups(self, user_id: int) -> list:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, setup_name, source_peer, dest_peer, source_title, dest_title, created_at
                FROM user_setups
                WHERE user_id = ?
                ORDER BY id ASC
            """, (user_id,))
            rows = cursor.fetchall()
            return [
                {
                    "id": r[0],
                    "setup_name": r[1],
                    "source_peer": r[2],
                    "dest_peer": r[3],
                    "source_title": r[4] or r[2],
                    "dest_title": r[5] or r[3],
                    "created_at": r[6]
                }
                for r in rows
            ]

    def get_user_setup_by_id(self, setup_id: int, user_id: int) -> Optional[dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, setup_name, source_peer, dest_peer, source_title, dest_title, created_at
                FROM user_setups
                WHERE id = ? AND user_id = ?
            """, (setup_id, user_id))
            r = cursor.fetchone()
            if r:
                return {
                    "id": r[0],
                    "setup_name": r[1],
                    "source_peer": r[2],
                    "dest_peer": r[3],
                    "source_title": r[4] or r[2],
                    "dest_title": r[5] or r[3],
                    "created_at": r[6]
                }
            return None

    def delete_user_setup(self, setup_id: int, user_id: int) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM user_setups WHERE id = ? AND user_id = ?", (setup_id, user_id))
            conn.commit()
            return cursor.rowcount > 0

    def get_user_setup(self, user_id: int) -> Optional[dict]:
        setups = self.get_user_setups(user_id)
        if setups:
            return setups[-1]
        return None

    def save_user_setup(self, user_id: int, source_peer: str, dest_peer: str, source_title: Optional[str] = None, dest_title: Optional[str] = None, setup_name: Optional[str] = None) -> int:
        name = setup_name or f"{source_title or source_peer} ➔ {dest_title or dest_peer}"
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id FROM user_setups
                WHERE user_id = ? AND source_peer = ? AND dest_peer = ?
            """, (user_id, str(source_peer), str(dest_peer)))
            row = cursor.fetchone()
            if row:
                cursor.execute("""
                    UPDATE user_setups
                    SET source_title = ?, dest_title = ?, setup_name = ?
                    WHERE id = ?
                """, (source_title, dest_title, name, row[0]))
                conn.commit()
                return row[0]
            else:
                return self.add_user_setup(user_id, name, str(source_peer), str(dest_peer), source_title, dest_title)

    # Caption rules management
    def add_caption_rule(self, rule_type: str, pattern: str, replacement: str = "") -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO caption_rules (rule_type, pattern, replacement)
                VALUES (?, ?, ?)
            """, (rule_type, pattern, replacement))
            conn.commit()
            return cursor.lastrowid

    def remove_caption_rule(self, rule_id: int) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM caption_rules WHERE id = ?", (rule_id,))
            conn.commit()
            return cursor.rowcount > 0

    def get_caption_rules(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, rule_type, pattern, replacement, created_at FROM caption_rules ORDER BY id ASC")
            return cursor.fetchall()

    def clear_caption_rules(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM caption_rules")
            conn.commit()

    def is_strip_all_captions(self) -> bool:
        return self.get_state("strip_all_captions") == "1"

    def set_strip_all_captions(self, enabled: bool):
        self.set_state("strip_all_captions", "1" if enabled else "0")

    def apply_caption_rules(self, text: Optional[str]) -> Optional[str]:
        if not text:
            return text
        if self.is_strip_all_captions():
            return ""

        rules = self.get_caption_rules()
        modified = text
        for r in rules:
            rid, rtype, pat, rep, _ = r
            if rtype == "remove":
                modified = modified.replace(pat, "")
            elif rtype == "replace":
                modified = modified.replace(pat, rep)
            elif rtype == "prefix":
                modified = pat + modified
            elif rtype == "suffix":
                modified = modified + pat

        return modified.strip()

    # User session methods (Multi-account login)
    def save_user_session(self, user_id: int, session_string: str, phone_number: Optional[str] = None, first_name: Optional[str] = None, username: Optional[str] = None):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO user_sessions (user_id, session_string, phone_number, first_name, username)
                VALUES (?, ?, ?, ?, ?)
            """, (user_id, session_string, phone_number, first_name, username))
            conn.commit()

    def get_user_session(self, user_id: int) -> Optional[str]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT session_string FROM user_sessions WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            return row[0] if row else None

    def get_user_session_info(self, user_id: int) -> Optional[dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id, session_string, phone_number, first_name, username, created_at FROM user_sessions WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            if row:
                return {
                    "user_id": row[0],
                    "session_string": row[1],
                    "phone_number": row[2],
                    "first_name": row[3],
                    "username": row[4],
                    "created_at": row[5]
                }
            return None

    def delete_user_session(self, user_id: int) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM user_sessions WHERE user_id = ?", (user_id,))
            conn.commit()
            return cursor.rowcount > 0

