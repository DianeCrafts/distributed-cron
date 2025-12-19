from jobs.models import Job
from jobs.repository import JobRepository
from scheduler.scheduler import Scheduler


repo = JobRepository()

repo.add_job("job_a", 5)
repo.add_job("job_b", 10)


jobs = repo.load_jobs()


scheduler = Scheduler(jobs=jobs, repository=repo)

if __name__ == "__main__":
    scheduler.start()
