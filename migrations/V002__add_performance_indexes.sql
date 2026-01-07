-- Performance Optimization: Add indexes for frequently queried columns
-- This migration adds indexes to improve query performance

-- Indexes for fact_active (current online players)
CREATE INDEX IF NOT EXISTS idx_fact_active_player_id ON fact_active(player_id);
CREATE INDEX IF NOT EXISTS idx_fact_active_server_id ON fact_active(server_id);

-- Indexes for fact_history (player session history)
CREATE INDEX IF NOT EXISTS idx_fact_history_player_id ON fact_history(player_id);
CREATE INDEX IF NOT EXISTS idx_fact_history_server_id ON fact_history(server_id);
CREATE INDEX IF NOT EXISTS idx_fact_history_session_start ON fact_history(session_start);

-- Indexes for dim_servers (server information)
CREATE INDEX IF NOT EXISTS idx_dim_servers_player_count ON dim_servers(player_count);
CREATE INDEX IF NOT EXISTS idx_dim_servers_last_seen ON dim_servers(last_seen);

-- Indexes for dim_players (player information)
CREATE INDEX IF NOT EXISTS idx_dim_players_last_seen ON dim_players(last_seen);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_fact_history_player_session ON fact_history(player_id, session_start);
CREATE INDEX IF NOT EXISTS idx_fact_server_history_timestamp ON fact_server_history(timestamp);

