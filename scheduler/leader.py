from datetime import datetime, timedelta
from storage.db import get_connection

LOCK_ID = "scheduler_leader"
LOCK_TTL_SECONDS = 10

class LeaderElection:
    def __init__(self):
        self.conn = get_connection()

    def try_acquire_leadership(self):
        """
        Attempt to acquire the leadership lock.
        Returns True if this instance became leader, False otherwise.
        """
        now = datetime.utcnow()
        expires_at = now + timedelta(seconds=LOCK_TTL_SECONDS)

        cursor = self.conn.cursor()
        
        cursor.execute("""
            INSERT OR IGNORE INTO leader_lock (id, locked_at, expires_at)
            VALUES (?, ?, ?)
        """, (LOCK_ID, now.isoformat(), expires_at.isoformat()))
        self.conn.commit()

        # Check if lock belongs to this instance
        cursor.execute("SELECT locked_at, expires_at FROM leader_lock WHERE id = ?", (LOCK_ID,))
        row = cursor.fetchone()
        if row:
            row_expires_at = datetime.fromisoformat(row["expires_at"])
            # If lock expired, take it
            if row_expires_at < now:
                cursor.execute("""
                    UPDATE leader_lock
                    SET locked_at = ?, expires_at = ?
                    WHERE id = ? AND expires_at < ?
                """, (now.isoformat(), expires_at.isoformat(), LOCK_ID, now.isoformat()))
                self.conn.commit()
                cursor.execute("SELECT expires_at FROM leader_lock WHERE id = ?", (LOCK_ID,))
                row = cursor.fetchone()
                row_expires_at = datetime.fromisoformat(row["expires_at"])
            
            # If expires_at matches our intended expiration → we are leader
            if row_expires_at == expires_at:
                return True
        return False

    def renew_leadership(self):
        """
        Renew the leadership before TTL expires.
        Should only be called by the current leader.
        """
        now = datetime.utcnow()
        new_expires_at = now + timedelta(seconds=LOCK_TTL_SECONDS)
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE leader_lock
            SET expires_at = ?
            WHERE id = ?
        """, (new_expires_at.isoformat(), LOCK_ID))
        self.conn.commit()
