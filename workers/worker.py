import time
import uuid
from jobs.repository import JobRepository
from datetime import datetime

class Worker:
    def __init__(self):
        self.repo = JobRepository()
        self.worker_id = str(uuid.uuid4())

        self.task_map = {
            "job_a": lambda: print(f"[{datetime.now()}] Executing task_a"),
            "job_b": lambda: print(f"[{datetime.now()}] Executing task_b"),
        }

    def start(self):
        print(f"[{datetime.now()}] Worker started worker_id={self.worker_id}")
        try:
            while True:
                # reclaim any expired leases (workers can do it too)
                self.repo.reclaim_stale_in_progress()

                job_row = self.repo.fetch_next_job(worker_id=self.worker_id)
                if not job_row:
                    time.sleep(1)
                    continue

                queue_id = job_row["id"]
                job_id = job_row["job_id"]
                print(f"[{datetime.now()}] Worker picked job: {job_id} (queue_id={queue_id})")

                task = self.task_map.get(job_id, lambda: print(f"[{datetime.now()}] Executing {job_id}"))

                try:
                    # Run the task
                    task()

                    # simulate processing
                    for _ in range(5):
                        time.sleep(1)
                        # renew lease while running (important for long tasks)
                        if not self.repo.renew_lease(queue_id, self.worker_id):
                            print(f"[{datetime.now()}] Lost lease for queue_id={queue_id}. Stopping work.")
                            raise RuntimeError("Lost lease")

                    ok = self.repo.mark_job_done(queue_id, self.worker_id)
                    if ok:
                        self.repo.update_last_run(job_id, datetime.utcnow())
                    else:
                        print(f"[{datetime.now()}] Could not mark done (lost lease?) queue_id={queue_id}")

                except Exception as e:
                    self.repo.mark_job_failed(queue_id, self.worker_id, repr(e))
                    print(f"[{datetime.now()}] Job failed: {job_id} error={e}")

        except KeyboardInterrupt:
            print("\nWorker stopped manually.")
