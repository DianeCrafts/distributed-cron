-- =========================
-- JOB DEFINITIONS
-- =========================
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    interval_seconds INTEGER NOT NULL,

    -- execution history (worker-owned)
    last_run_time TEXT,

    -- scheduling control (scheduler-owned)
    next_run_time TEXT,

    -- pause / resume
    paused INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_jobs_paused
ON jobs(paused);



-- =========================
-- LEADER ELECTION (SCHEDULER)
-- =========================
CREATE TABLE IF NOT EXISTS leader_lock (
    id TEXT PRIMARY KEY,

    -- who owns leadership
    owner_id TEXT NOT NULL,

    -- bookkeeping
    locked_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);



-- =========================
-- JOB EXECUTION QUEUE
-- =========================
CREATE TABLE IF NOT EXISTS job_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- job reference
    job_id TEXT NOT NULL,

    -- lifecycle timestamps
    enqueued_at TEXT NOT NULL,
    started_at TEXT,
    executed_at TEXT,

    -- execution state
    status TEXT NOT NULL DEFAULT 'pending',

    -- worker leasing
    worker_id TEXT,
    lease_expires_at TEXT,

    -- retries / errors
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);

-- =========================
-- INDEXES (HOT PATHS)
-- =========================

-- worker fetch: oldest pending job
CREATE INDEX IF NOT EXISTS idx_job_queue_status_enqueued
ON job_queue(status, enqueued_at);

-- lease reclaim
CREATE INDEX IF NOT EXISTS idx_job_queue_lease
ON job_queue(status, lease_expires_at);

-- prevent parallel execution per job
CREATE INDEX IF NOT EXISTS idx_job_queue_job_status
ON job_queue(job_id, status);

-- observability / debugging
CREATE INDEX IF NOT EXISTS idx_job_queue_worker
ON job_queue(worker_id);
