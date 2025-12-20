import time
from datetime import datetime

class Scheduler:
    def __init__(self, jobs, repository, leader_election):
        # jobs param kept for backward compatibility, but no longer used
        self.repo = repository
        self.leader = leader_election

    def start(self):
        print(f"[{datetime.now()}] Scheduler started")
        try:
            while True:
                if self.leader.try_acquire_leadership():
                    # 1️⃣ reclaim stuck jobs (crashed workers)
                    self.repo.reclaim_stale_in_progress()

                    # 2️⃣ ALWAYS reload jobs from DB (source of truth)
                    jobs = self.repo.load_jobs()

                    for job in jobs:
                        # 3️⃣ pause support
                        if getattr(job, "paused", 0) == 1:
                            continue

                        # 4️⃣ interval logic
                        if job.is_due():
                            enqueued = self.repo.enqueue_job(job.job_id)
                            if enqueued:
                                self.repo.advance_next_run(job.job_id, job.interval_seconds)
                                print(f"[{datetime.now()}] Enqueued job: {job.job_id}")

                    # 5️⃣ keep leadership alive
                    self.leader.renew_leadership()
                else:
                    print(f"[{datetime.now()}] Not leader, waiting...")

                time.sleep(1)
        except KeyboardInterrupt:
            print("\nScheduler stopped manually.")
