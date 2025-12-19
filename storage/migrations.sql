CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    interval_seconds INTEGER NOT NULL,
    last_run_time TEXT
);
