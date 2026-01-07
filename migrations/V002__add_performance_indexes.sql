-- Performance Optimization: Add additional indexes for query performance
-- Note: V001 already includes basic indexes, this adds supplementary ones

-- Additional composite index for player name searches (case-insensitive)
CREATE INDEX IF NOT EXISTS idx_dim_players_name_lower ON dim_players(LOWER(name));

-- Index for server operator queries
CREATE INDEX IF NOT EXISTS idx_dim_servers_operator_player ON dim_servers(operator_name, player_count);

-- Index for map-based queries
CREATE INDEX IF NOT EXISTS idx_dim_maps_name ON dim_maps(name);

-- Indexes for daily aggregation tables (composite indexes for common query patterns)
CREATE INDEX IF NOT EXISTS idx_operator_daily_operator_day ON fact_operator_daily(operator_name, day DESC);
CREATE INDEX IF NOT EXISTS idx_map_daily_map_day ON fact_map_daily(map_id, day DESC);
CREATE INDEX IF NOT EXISTS idx_server_daily_server_day ON fact_server_daily(server_id, day DESC);
CREATE INDEX IF NOT EXISTS idx_player_daily_player_day ON fact_player_daily(player_id, day DESC);

