from datetime import datetime, timedelta

from datetime import datetime, timedelta

class Job:
    def __init__(self, job_id, interval_seconds, task, paused=0):
        self.job_id = job_id
        self.interval_seconds = interval_seconds
        self.task = task
        self.paused = paused
        self.last_run_time = None
        self.next_run_time = None

    def is_due(self):
        if self.next_run_time is None:
            return True
        return datetime.utcnow() >= self.next_run_time


    def run(self):
        """Run the job and update last run time"""
        print(f"[{datetime.now()}] Running job: {self.job_id}")
        self.task()
        self.last_run_time = datetime.now()
