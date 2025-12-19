import time
from jobs.repository import JobRepository
from jobs.models import Job
from datetime import datetime

class Worker:
    def __init__(self):
        self.repo = JobRepository()
        # In-memory mapping of job_id -> task
        self.task_map = {
            "job_a": lambda: print(f"[{datetime.now()}] Executing task_a"),
            "job_b": lambda: print(f"[{datetime.now()}] Executing task_b"),
        }

    def start(self):
        print(f"[{datetime.now()}] Worker started")
        try:
            while True:
                job_row = self.repo.fetch_next_job()
                if job_row:
                    queue_id = job_row["id"]
                    job_id = job_row["job_id"]
                    task = self.task_map.get(job_id, lambda: print(f"[{datetime.now()}] Executing {job_id}"))
                    print(f"[{datetime.now()}] Worker picked job: {job_id}")
                    task()  # Execute the task
                    self.repo.mark_job_executed(queue_id)
                else:
                    time.sleep(1)  # No job in queue
        except KeyboardInterrupt:
            print("\nWorker stopped manually.")
