from datetime import datetime, timedelta
from storage.db import get_connection
import uuid

LOCK_ID = "scheduler_leader"
LOCK_TTL_SECONDS = 10

class LeaderElection:
    def __init__(self):
        self.conn = get_connection()
        self.owner_id = str(uuid.uuid4())

    def try_acquire_leadership(self) -> bool:
        now = datetime.utcnow()
        expires_at = now + timedelta(seconds=LOCK_TTL_SECONDS)
        cursor = self.conn.cursor()

        # Try to insert lock if it doesn't exist
        cursor.execute("""
            INSERT OR IGNORE INTO leader_lock (id, owner_id, locked_at, expires_at)
            VALUES (?, ?, ?, ?)
        """, (LOCK_ID, self.owner_id, now.isoformat(), expires_at.isoformat()))
        self.conn.commit()

        # Try to take over if expired OR already ours
        cursor.execute("""
            UPDATE leader_lock
            SET owner_id = ?,
                locked_at = ?,
                expires_at = ?
            WHERE id = ?
              AND (expires_at < ? OR owner_id = ?)
        """, (
            self.owner_id,
            now.isoformat(),
            expires_at.isoformat(),
            LOCK_ID,
            now.isoformat(),
            self.owner_id
        ))
        self.conn.commit()

        # Verify ownership
        cursor.execute(
            "SELECT owner_id FROM leader_lock WHERE id = ?",
            (LOCK_ID,)
        )
        row = cursor.fetchone()
        return row and row["owner_id"] == self.owner_id

    def renew_leadership(self) -> bool:
        now = datetime.utcnow()
        new_expires_at = now + timedelta(seconds=LOCK_TTL_SECONDS)

        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE leader_lock
            SET expires_at = ?
            WHERE id = ? AND owner_id = ?
        """, (new_expires_at.isoformat(), LOCK_ID, self.owner_id))
        self.conn.commit()

        return cursor.rowcount == 1
