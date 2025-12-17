import sqlite3
import time
import json

class LogStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _get_conn(self):
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def _init_db(self):
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS log_entries (
                log_index  INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp  REAL NOT NULL,
                entry_type TEXT NOT NULL,
                payload    TEXT NOT NULL
            )
            """
        )
        conn.commit()
        conn.close()

    def append_entry(self, entry_type: str, payload: dict) -> int:
        # ... (Paste the existing append_entry code here) ...
        # Simplified for brevity:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO log_entries (timestamp, entry_type, payload) VALUES (?, ?, ?)",
            (time.time(), entry_type, json.dumps(payload)),
        )
        conn.commit()
        idx = cur.lastrowid
        conn.close()
        return idx

    def get_entries_after(self, last_index: int):
        # ... (Paste existing get_entries_after code here) ...
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT log_index, entry_type, payload FROM log_entries WHERE log_index > ? ORDER BY log_index ASC",
            (last_index,),
        )
        rows = cur.fetchall()
        conn.close()
        
        entries = []
        for idx, etype, payload_json in rows:
            try:
                payload = json.loads(payload_json)
            except:
                payload = {"raw": payload_json}
            entries.append({"log_index": idx, "entry_type": etype, "payload": payload})
        return entries

    def latest_index(self) -> int:
        # ... (Paste existing latest_index code here) ...
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT MAX(log_index) FROM log_entries")
        row = cur.fetchone()
        conn.close()
        return row[0] if row and row[0] is not None else 0