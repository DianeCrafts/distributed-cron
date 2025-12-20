# Distributed Cron Scheduler

A fault-tolerant, distributed cron scheduler designed to guarantee exactly-once execution of scheduled jobs, even in the presence of crashes, restarts, and multiple servers.

Traditional cron jobs break down in distributed systems. This project solves that problem by providing a production-grade scheduling system that works safely across many nodes.

## Why This Exists
In a single-server world, cron is simple.
In a distributed world:
- Servers crash
- Multiple instances run the same job
- Jobs may run twice or not at all
- Manual coordination is fragile
- This scheduler ensures one job runs once, no more, no less.

## Key Features
### Persistent Jobs
Jobs are stored durably so schedules survive restarts and crashes.

### Leader Election
Only one node acts as the scheduler leader at any time, preventing duplicate job dispatching.

### Distributed-Safe Scheduling

Scheduling decisions are coordinated across nodes using distributed locks / consensus primitives.

### Worker Pool

Jobs are executed by a configurable pool of workers for controlled concurrency.

### Exactly-Once Execution

Each job execution is guaranteed to run once and only once, even if nodes fail mid-run.

### Crash Recovery

If a worker or leader crashes, in-progress jobs are safely recovered and retried without duplication.
### High-Level Architecture
```bash
+------------------+
| Leader Node |
|------------------|
| Schedules jobs |
| Assigns work |
+--------+---------+
|
v
+------------------+
| Worker Pool |
|------------------|
| Executes jobs |
| Reports status |
+------------------+
|
v
+------------------+
| Persistent Store |
|------------------|
| Jobs & state |
+------------------+
```


## Use Cases
- Billing & Payments: Prevent double-charging customers when multiple servers are running.

- Scheduled Emails & Notifications: Send emails or notifications exactly once at the scheduled time.

- Data Processing Pipelines: Run daily or hourly aggregation jobs without overlap or data corruption.

- Background Maintenance Tasks: Safely perform cleanup, backups, or migrations in distributed environments.

- Microservices Infrastructure: Replace fragile per-service cron jobs with a centralized, reliable scheduler.


## Setup & Running the Scheduler

### Prerequisites

- A shared persistent store (e.g. PostgreSQL, MySQL, Redis, etc.)
- Multiple application instances (local or deployed)
- Network connectivity between nodes

### running workers:
```bash
python -m workers.main 
```
### running the scheduler:
```bash
python -m scheduler.main
```
