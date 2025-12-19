from storage.db import get_connection
from jobs.models import Job
from datetime import datetime
import sqlite3

class JobRepository:
    def __init__(self):
        self.conn = get_connection()
        self.conn.row_factory = sqlite3.Row

    def load_jobs(self):
        """Load all jobs from DB as Job objects"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, interval_seconds, last_run_time FROM jobs")
        rows = cursor.fetchall()

        jobs = []
        for row in rows:
            last_run = None
            if row["last_run_time"]:
                last_run = datetime.fromisoformat(row["last_run_time"])
            job = Job(
                job_id=row["id"],
                interval_seconds=row["interval_seconds"],
                task=lambda id=row["id"]: print(f"Executing {id} (default task)"),
            )
            job.last_run_time = last_run
            jobs.append(job)
        return jobs

    def update_last_run(self, job_id, last_run_time):
        """Update the last_run_time of a job"""
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE jobs SET last_run_time = ? WHERE id = ?",
            (last_run_time.isoformat(), job_id),
        )
        self.conn.commit()

    def add_job(self, job_id, interval_seconds):
        """Insert a new job if it doesn't exist"""
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO jobs (id, interval_seconds) VALUES (?, ?)",
            (job_id, interval_seconds),
        )
        self.conn.commit()



    # -------------------------
    # Job Queue Methods
    # -------------------------
    def enqueue_job(self, job_id):
        """Add a job to the queue"""
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO job_queue (job_id, enqueued_at) VALUES (?, ?)",
            (job_id, datetime.utcnow().isoformat())
        )
        self.conn.commit()

    def fetch_next_job_for_worker(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT id, job_id FROM job_queue
            WHERE status = 'pending'
            ORDER BY enqueued_at
            LIMIT 1
        """)
        row = cursor.fetchone()
        if row:
            cursor.execute("""
                UPDATE job_queue
                SET status = 'in_progress'
                WHERE id = ? AND status = 'pending'
            """, (row["id"],))
            self.conn.commit()
            return row
        return None


    def mark_job_done(self, queue_id):
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE job_queue
            SET status = 'done', executed_at = ?
            WHERE id = ?
        """, (datetime.utcnow().isoformat(), queue_id))
        self.conn.commit()

    
    def close(self):
        self.conn.close()



