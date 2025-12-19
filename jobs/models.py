from datetime import datetime, timedelta

class Job:
    def __init__(self, job_id, interval_seconds, task):
        """
        job_id: unique string identifier
        interval_seconds: how often to run the job
        task: a callable function to execute
        """
        self.job_id = job_id
        self.interval_seconds = interval_seconds
        self.task = task
        self.last_run_time = None

    def is_due(self):
        """Check if the job is due to run"""
        if self.last_run_time is None:
            return True
        return (datetime.now() - self.last_run_time) >= timedelta(seconds=self.interval_seconds)

    def run(self):
        """Run the job and update last run time"""
        print(f"[{datetime.now()}] Running job: {self.job_id}")
        self.task()
        self.last_run_time = datetime.now()
