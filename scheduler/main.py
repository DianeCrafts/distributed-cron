from jobs.models import Job
from scheduler.scheduler import Scheduler
from datetime import datetime


def task_a():
    print("Task A executed!")

def task_b():
    print("Task B executed!")


job1 = Job(job_id="job_a", interval_seconds=5, task=task_a)
job2 = Job(job_id="job_b", interval_seconds=10, task=task_b)


scheduler = Scheduler(jobs=[job1, job2])

if __name__ == "__main__":
    scheduler.start()
