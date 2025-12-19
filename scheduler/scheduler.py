import time
from datetime import datetime

class Scheduler:
    def __init__(self, jobs, repository):
        self.jobs = jobs 
        self.repo = repository

    def start(self):
        print(f"[{datetime.now()}] Scheduler started")
        try:
            while True:
                for job in self.jobs:
                    if job.is_due():
                        job.run()
                        self.repo.update_last_run(job.job_id, job.last_run_time)
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nScheduler stopped manually.")
