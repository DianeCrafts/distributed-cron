import time
from datetime import datetime

class Scheduler:
    def __init__(self, jobs):
        self.jobs = jobs

    def start(self):
        print(f"[{datetime.now()}] Scheduler started")
        try:
            while True:
                for job in self.jobs:
                    if job.is_due():
                        job.run()
                time.sleep(1) 
        except KeyboardInterrupt:
            print("\nScheduler stopped manually.")
