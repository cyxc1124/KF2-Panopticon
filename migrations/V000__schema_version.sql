-- Schema Migration Version Tracking Table
-- This table tracks which migrations have been applied

CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR(50) PRIMARY KEY,
    description TEXT NOT NULL,
    checksum VARCHAR(64),
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_time_ms INTEGER,
    success BOOLEAN DEFAULT TRUE
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_schema_migrations_executed_at ON schema_migrations(executed_at);
CREATE INDEX IF NOT EXISTS idx_schema_migrations_success ON schema_migrations(success);

