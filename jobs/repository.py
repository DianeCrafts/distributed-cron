from storage.db import get_connection
from jobs.models import Job
from datetime import datetime, timedelta
import sqlite3
import uuid

LEASE_SECONDS = 30
MAX_ATTEMPTS = 5

class JobRepository:
    def __init__(self):
        self.conn = get_connection()
        self.conn.row_factory = sqlite3.Row

    # -------------------------
    # Jobs table methods
    # -------------------------
    def load_jobs(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT id, interval_seconds, last_run_time, next_run_time, paused
            FROM jobs
        """)
        rows = cursor.fetchall()

        jobs = []
        for row in rows:
            job = Job(
                job_id=row["id"],
                interval_seconds=row["interval_seconds"],
                task=lambda id=row["id"]: print(f"Executing {id}"),
                paused=row["paused"] or 0
            )

            job.last_run_time = (
                datetime.fromisoformat(row["last_run_time"])
                if row["last_run_time"] else None
            )

            job.next_run_time = (
                datetime.fromisoformat(row["next_run_time"])
                if row["next_run_time"] else None
            )

            jobs.append(job)

        return jobs


    def update_last_run(self, job_id, last_run_time):
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE jobs SET last_run_time = ? WHERE id = ?",
            (last_run_time.isoformat(), job_id),
        )
        self.conn.commit()

    def add_job(self, job_id, interval_seconds):
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO jobs (id, interval_seconds) VALUES (?, ?)",
            (job_id, interval_seconds),
        )
        self.conn.commit()

    def pause_job(self, job_id):
        cursor = self.conn.cursor()
        cursor.execute("UPDATE jobs SET paused = 1 WHERE id = ?", (job_id,))
        self.conn.commit()

    def resume_job(self, job_id):
        cursor = self.conn.cursor()
        cursor.execute("UPDATE jobs SET paused = 0 WHERE id = ?", (job_id,))
        self.conn.commit()

    # -------------------------
    # Queue helpers
    # -------------------------
    def has_running_instance(self, job_id):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 1 FROM job_queue
            WHERE job_id = ?
            AND status = 'in_progress'
            LIMIT 1
        """, (job_id,))
        return cursor.fetchone() is not None


    def enqueue_job(self, job_id):
        # Only prevent parallel execution, not backlog
        if self.has_running_instance(job_id):
            return False

        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO job_queue (job_id, enqueued_at, status)
            VALUES (?, ?, 'pending')
        """, (job_id, datetime.utcnow().isoformat()))
        self.conn.commit()
        return True


    # -------------------------
    # Leasing / claiming jobs
    # -------------------------
    def fetch_next_job(self, worker_id: str):
        """
        Claim one pending job and lease it.
        Uses a transaction to reduce race conditions.
        """
        now = datetime.utcnow()
        lease_expires = now + timedelta(seconds=LEASE_SECONDS)

        cursor = self.conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")

        # Pick the next pending row
        cursor.execute("""
            SELECT id, job_id
            FROM job_queue
            WHERE status = 'pending'
            ORDER BY enqueued_at
            LIMIT 1
        """)
        row = cursor.fetchone()
        if not row:
            self.conn.commit()
            return None

        # Claim it (only if still pending)
        cursor.execute("""
            UPDATE job_queue
            SET status='in_progress',
                started_at=?,
                worker_id=?,
                lease_expires_at=?
            WHERE id=? AND status='pending'
        """, (now.isoformat(), worker_id, lease_expires.isoformat(), row["id"]))

        if cursor.rowcount == 0:
            # someone else took it
            self.conn.commit()
            return None

        self.conn.commit()

        # Return the claimed job
        return {"id": row["id"], "job_id": row["job_id"]}

    def renew_lease(self, queue_id: int, worker_id: str):
        now = datetime.utcnow()
        lease_expires = now + timedelta(seconds=LEASE_SECONDS)
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE job_queue
            SET lease_expires_at = ?
            WHERE id = ? AND status='in_progress' AND worker_id = ?
        """, (lease_expires.isoformat(), queue_id, worker_id))
        self.conn.commit()
        return cursor.rowcount == 1

    def mark_job_done(self, queue_id: int, worker_id: str):
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE job_queue
            SET status='done', executed_at=?
            WHERE id=? AND status='in_progress' AND worker_id=?
        """, (datetime.utcnow().isoformat(), queue_id, worker_id))
        self.conn.commit()
        return cursor.rowcount == 1

    def mark_job_failed(self, queue_id: int, worker_id: str, error: str):
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE job_queue
            SET status='failed', last_error=?, executed_at=?
            WHERE id=? AND status='in_progress' AND worker_id=?
        """, (error, datetime.utcnow().isoformat(), queue_id, worker_id))
        self.conn.commit()
        return cursor.rowcount == 1

    def reclaim_stale_in_progress(self):
        """
        Requeue jobs whose lease expired.
        Optionally mark as failed after MAX_ATTEMPTS.
        """
        now = datetime.utcnow()
        cursor = self.conn.cursor()

        # 1) mark too-many-attempts as failed
        cursor.execute("""
            UPDATE job_queue
            SET status='failed',
                last_error='Max attempts exceeded',
                executed_at=?
            WHERE status='in_progress'
            AND lease_expires_at IS NOT NULL
            AND lease_expires_at < ?
            AND COALESCE(attempts, 0) >= ?
        """, (now.isoformat(), now.isoformat(), MAX_ATTEMPTS))

        # 2) requeue expired leases
        cursor.execute("""
            UPDATE job_queue
            SET status='pending',
                worker_id=NULL,
                started_at=NULL,
                lease_expires_at=NULL,
                attempts=COALESCE(attempts, 0) + 1
            WHERE status='in_progress'
            AND lease_expires_at IS NOT NULL
            AND lease_expires_at < ?
            AND COALESCE(attempts, 0) < ?
        """, (now.isoformat(), MAX_ATTEMPTS))

        self.conn.commit()


    def advance_next_run(self, job_id, interval_seconds):
        next_run = datetime.utcnow() + timedelta(seconds=interval_seconds)
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE jobs
            SET next_run_time = ?
            WHERE id = ?
        """, (next_run.isoformat(), job_id))
        self.conn.commit()

    def delete_job(self, job_id: str):
        cursor = self.conn.cursor()

        # Remove pending jobs (safe)
        cursor.execute("""
            DELETE FROM job_queue
            WHERE job_id = ? AND status = 'pending'
        """, (job_id,))

        # Remove job definition
        cursor.execute("""
            DELETE FROM jobs
            WHERE id = ?
        """, (job_id,))

        self.conn.commit()


    # def list_queue_rows(self, limit: int = 50, status: str | None = None):
    #     cursor = self.conn.cursor()

    #     if status:
    #         cursor.execute("""
    #             SELECT id, job_id, status, attempts, worker_id,
    #                 enqueued_at, started_at, executed_at, last_error
    #             FROM job_queue
    #             WHERE status = ?
    #             ORDER BY enqueued_at DESC
    #             LIMIT ?
    #         """, (status, limit))
    #     else:
    #         cursor.execute("""
    #             SELECT id, job_id, status, attempts, worker_id,
    #                 enqueued_at, started_at, executed_at, last_error
    #             FROM job_queue
    #             ORDER BY enqueued_at DESC
    #             LIMIT ?
    #         """, (limit,))

    #     return cursor.fetchall()


    def list_jobs_rows(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT id,
                interval_seconds,
                paused,
                next_run_time,
                last_run_time
            FROM jobs
            ORDER BY id
        """)
        return cursor.fetchall()
    
    def queue_counts(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT status, COUNT(*) as c
            FROM job_queue
            GROUP BY status
        """)
        rows = cursor.fetchall()
        return {row["status"]: row["c"] for row in rows}




    def list_queue_rows(self, limit: int = 50, status: str | None = None):
        cursor = self.conn.cursor()

        if status:
            cursor.execute("""
                SELECT id,
                    job_id,
                    status,
                    attempts,
                    worker_id,
                    enqueued_at,
                    started_at,
                    executed_at,
                    last_error
                FROM job_queue
                WHERE status = ?
                ORDER BY enqueued_at DESC
                LIMIT ?
            """, (status, limit))
        else:
            cursor.execute("""
                SELECT id,
                    job_id,
                    status,
                    attempts,
                    worker_id,
                    enqueued_at,
                    started_at,
                    executed_at,
                    last_error
                FROM job_queue
                ORDER BY enqueued_at DESC
                LIMIT ?
            """, (limit,))

        return cursor.fetchall()

    def close(self):
        self.conn.close()
