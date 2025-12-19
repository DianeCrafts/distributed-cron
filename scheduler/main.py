from jobs.repository import JobRepository
from scheduler.scheduler import Scheduler
from scheduler.leader import LeaderElection

# Repository
repo = JobRepository()
repo.add_job("job_a", 5)
repo.add_job("job_b", 10)
jobs = repo.load_jobs()

# Leader election
leader = LeaderElection()

# Scheduler
scheduler = Scheduler(jobs=jobs, repository=repo, leader_election=leader)

if __name__ == "__main__":
    scheduler.start()
