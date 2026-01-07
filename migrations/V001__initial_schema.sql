-- KF2-Panopticon PostgreSQL 数据库初始化脚本
-- 创建数据库: CREATE DATABASE kf2_panopticon;
-- 创建用户: CREATE USER kf2user WITH PASSWORD 'your_password';
-- 授权: GRANT ALL PRIVILEGES ON DATABASE kf2_panopticon TO kf2user;
-- 执行: psql -U kf2user -d kf2_panopticon -f init_postgresql.sql

-- ==================== 维度表 ====================

-- 1. 服务器表
CREATE TABLE IF NOT EXISTS dim_servers (
    id SERIAL PRIMARY KEY,
    ip_address VARCHAR(50) NOT NULL,
    query_port INTEGER NOT NULL,
    game_port INTEGER,
    name TEXT,
    current_map_id INTEGER,
    player_count INTEGER DEFAULT 0,
    map_start TIMESTAMP,
    last_seen TIMESTAMP,
    current_session_uuid VARCHAR(36),
    operator_name TEXT,
    location TEXT,
    UNIQUE(ip_address, query_port)
);

-- 2. 地图表
CREATE TABLE IF NOT EXISTS dim_maps (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL
);

-- 3. 玩家表
CREATE TABLE IF NOT EXISTS dim_players (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL
);

-- ==================== 事实表 ====================

-- 4. 服务器历史表
CREATE TABLE IF NOT EXISTS fact_server_history (
    id SERIAL PRIMARY KEY,
    server_id INTEGER REFERENCES dim_servers(id),
    map_id INTEGER REFERENCES dim_maps(id),
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    reason VARCHAR(50),
    session_uuid VARCHAR(36),
    calculated_duration INTEGER DEFAULT 0
);

-- 5. 活跃会话表
CREATE TABLE IF NOT EXISTS fact_active (
    server_id INTEGER REFERENCES dim_servers(id),
    player_id INTEGER REFERENCES dim_players(id),
    map_id INTEGER REFERENCES dim_maps(id),
    score INTEGER,
    duration REAL,
    calculated_duration INTEGER DEFAULT 0,
    first_seen TIMESTAMP,
    last_seen TIMESTAMP,
    session_uuid VARCHAR(36),
    PRIMARY KEY (server_id, player_id)
);

-- 6. 会话历史表
CREATE TABLE IF NOT EXISTS fact_history (
    id SERIAL PRIMARY KEY,
    server_id INTEGER REFERENCES dim_servers(id),
    player_id INTEGER REFERENCES dim_players(id),
    map_id INTEGER REFERENCES dim_maps(id),
    final_score INTEGER,
    total_time REAL,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    session_uuid VARCHAR(36),
    calculated_duration INTEGER DEFAULT 0
);

-- 7. 全局统计表
CREATE TABLE IF NOT EXISTS fact_global_stats (
    id SERIAL PRIMARY KEY,
    scan_time TIMESTAMP,
    active_servers INTEGER,
    active_players INTEGER
);

-- ==================== 聚合表 ====================

-- 元数据键值表
CREATE TABLE IF NOT EXISTS meta_kv (
    key VARCHAR(255) PRIMARY KEY,
    value TEXT
);

-- 运营商每日统计
CREATE TABLE IF NOT EXISTS fact_operator_daily (
    day DATE,
    operator_name TEXT,
    server_count INTEGER NOT NULL,
    unique_players INTEGER NOT NULL,
    total_playtime_seconds INTEGER NOT NULL,
    last_contact TIMESTAMP,
    PRIMARY KEY (day, operator_name)
);

-- 地图每日统计
CREATE TABLE IF NOT EXISTS fact_map_daily (
    day DATE,
    map_id INTEGER,
    session_count INTEGER NOT NULL,
    total_seconds INTEGER NOT NULL,
    PRIMARY KEY (day, map_id)
);

-- 服务器每日统计
CREATE TABLE IF NOT EXISTS fact_server_daily (
    day DATE,
    server_id INTEGER,
    session_count INTEGER NOT NULL,
    total_seconds INTEGER NOT NULL,
    PRIMARY KEY (day, server_id)
);

-- 玩家每日统计
CREATE TABLE IF NOT EXISTS fact_player_daily (
    day DATE,
    player_id INTEGER,
    session_count INTEGER NOT NULL,
    total_seconds INTEGER NOT NULL,
    PRIMARY KEY (day, player_id)
);

-- 流量每日统计
CREATE TABLE IF NOT EXISTS fact_traffic_daily (
    day DATE PRIMARY KEY,
    unique_players INTEGER NOT NULL
);

-- ==================== 索引 ====================

-- fact_history 表索引
CREATE INDEX IF NOT EXISTS idx_fact_history_server_id ON fact_history(server_id);
CREATE INDEX IF NOT EXISTS idx_fact_history_player_id ON fact_history(player_id);
CREATE INDEX IF NOT EXISTS idx_fact_history_session_uuid ON fact_history(session_uuid);
CREATE INDEX IF NOT EXISTS idx_fact_history_session_start ON fact_history(session_start DESC);
CREATE INDEX IF NOT EXISTS idx_fact_history_server_start ON fact_history(server_id, session_start DESC);
CREATE INDEX IF NOT EXISTS idx_fact_history_player_start ON fact_history(player_id, session_start DESC);
CREATE INDEX IF NOT EXISTS idx_fact_history_map_id ON fact_history(map_id);

-- fact_server_history 表索引
CREATE INDEX IF NOT EXISTS idx_fact_server_history_server_id ON fact_server_history(server_id);
CREATE INDEX IF NOT EXISTS idx_fact_server_history_map_id ON fact_server_history(map_id);
CREATE INDEX IF NOT EXISTS idx_fact_server_history_start ON fact_server_history(session_start DESC);
CREATE INDEX IF NOT EXISTS idx_fact_server_history_session_uuid ON fact_server_history(session_uuid);

-- dim_servers 表索引
CREATE INDEX IF NOT EXISTS idx_dim_servers_operator ON dim_servers(operator_name);
CREATE INDEX IF NOT EXISTS idx_dim_servers_last_seen ON dim_servers(last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_dim_servers_player_count ON dim_servers(player_count DESC);

-- fact_global_stats 表索引
CREATE INDEX IF NOT EXISTS idx_fact_global_stats_scan_time ON fact_global_stats(scan_time DESC);

-- fact_active 表索引
CREATE INDEX IF NOT EXISTS idx_fact_active_last_seen ON fact_active(last_seen);

-- 聚合表索引
CREATE INDEX IF NOT EXISTS idx_operator_daily_day ON fact_operator_daily(day);
CREATE INDEX IF NOT EXISTS idx_operator_daily_operator ON fact_operator_daily(operator_name);
CREATE INDEX IF NOT EXISTS idx_map_daily_day ON fact_map_daily(day);
CREATE INDEX IF NOT EXISTS idx_server_daily_day ON fact_server_daily(day);
CREATE INDEX IF NOT EXISTS idx_player_daily_day ON fact_player_daily(day);

-- ==================== 完成 ====================
-- 分析表以优化查询计划
ANALYZE;

-- 显示表列表
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

