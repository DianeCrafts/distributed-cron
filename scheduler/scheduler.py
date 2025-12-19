import time
from datetime import datetime

class Scheduler:
    def __init__(self, jobs, repository, leader_election):
        self.jobs = jobs
        self.repo = repository
        self.leader = leader_election

    def start(self):
        print(f"[{datetime.now()}] Scheduler started")
        try:
            while True:
                if self.leader.try_acquire_leadership():
                    # Only leader schedules jobs
                    for job in self.jobs:
                        if job.is_due():
                            job.run()
                            self.repo.update_last_run(job.job_id, job.last_run_time)
                    # Renew leadership periodically
                    self.leader.renew_leadership()
                else:
                    print(f"[{datetime.now()}] Not leader, waiting...")
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nScheduler stopped manually.")
