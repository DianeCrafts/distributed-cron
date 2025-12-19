CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    interval_seconds INTEGER NOT NULL,
    last_run_time TEXT
);


CREATE TABLE IF NOT EXISTS leader_lock (
    id TEXT PRIMARY KEY,
    locked_at TEXT,
    expires_at TEXT
);


CREATE TABLE IF NOT EXISTS job_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    enqueued_at TEXT NOT NULL,
    executed_at TEXT
);
