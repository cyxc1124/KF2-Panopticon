-- KF2-Panopticon SQLite 数据库初始化脚本
-- 执行: python init_db.py (自动检测 SQLite)
-- 或在代码中调用: from app.models.init_db import init_database; init_database()

-- ==================== 维度表 ====================

-- 1. 服务器表
CREATE TABLE IF NOT EXISTS dim_servers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ip_address TEXT NOT NULL,
    query_port INTEGER NOT NULL,
    game_port INTEGER,
    name TEXT,
    current_map_id INTEGER,
    player_count INTEGER DEFAULT 0,
    map_start DATETIME,
    last_seen DATETIME,
    current_session_uuid TEXT,
    operator_name TEXT,
    location TEXT,
    UNIQUE(ip_address, query_port)
);

-- 2. 地图表
CREATE TABLE IF NOT EXISTS dim_maps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);

-- 3. 玩家表
CREATE TABLE IF NOT EXISTS dim_players (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);

-- ==================== 事实表 ====================

-- 4. 服务器历史表
CREATE TABLE IF NOT EXISTS fact_server_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    server_id INTEGER,
    map_id INTEGER,
    session_start DATETIME,
    session_end DATETIME,
    reason TEXT,
    session_uuid TEXT,
    calculated_duration INTEGER DEFAULT 0,
    FOREIGN KEY (server_id) REFERENCES dim_servers(id),
    FOREIGN KEY (map_id) REFERENCES dim_maps(id)
);

-- 5. 活跃会话表
CREATE TABLE IF NOT EXISTS fact_active (
    server_id INTEGER,
    player_id INTEGER,
    map_id INTEGER,
    score INTEGER,
    duration REAL,
    calculated_duration INTEGER DEFAULT 0,
    first_seen DATETIME,
    last_seen DATETIME,
    session_uuid TEXT,
    PRIMARY KEY (server_id, player_id),
    FOREIGN KEY (server_id) REFERENCES dim_servers(id),
    FOREIGN KEY (player_id) REFERENCES dim_players(id),
    FOREIGN KEY (map_id) REFERENCES dim_maps(id)
);

-- 6. 会话历史表
CREATE TABLE IF NOT EXISTS fact_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    server_id INTEGER,
    player_id INTEGER,
    map_id INTEGER,
    final_score INTEGER,
    total_time REAL,
    session_start DATETIME,
    session_end DATETIME,
    session_uuid TEXT,
    calculated_duration INTEGER DEFAULT 0,
    FOREIGN KEY (server_id) REFERENCES dim_servers(id),
    FOREIGN KEY (player_id) REFERENCES dim_players(id)
);

-- 7. 全局统计表
CREATE TABLE IF NOT EXISTS fact_global_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_time DATETIME,
    active_servers INTEGER,
    active_players INTEGER
);

-- ==================== 聚合表 ====================

-- 元数据键值表
CREATE TABLE IF NOT EXISTS meta_kv (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- 运营商每日统计
CREATE TABLE IF NOT EXISTS fact_operator_daily (
    day DATE,
    operator_name TEXT,
    server_count INTEGER NOT NULL,
    unique_players INTEGER NOT NULL,
    total_playtime_seconds INTEGER NOT NULL,
    last_contact DATETIME,
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

-- 聚合表索引
CREATE INDEX IF NOT EXISTS idx_operator_daily_day ON fact_operator_daily(day);
CREATE INDEX IF NOT EXISTS idx_operator_daily_operator ON fact_operator_daily(operator_name);
CREATE INDEX IF NOT EXISTS idx_map_daily_day ON fact_map_daily(day);
CREATE INDEX IF NOT EXISTS idx_server_daily_day ON fact_server_daily(day);
CREATE INDEX IF NOT EXISTS idx_player_daily_day ON fact_player_daily(day);

-- fact_history 表索引（性能优化）
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

-- ==================== 完成 ====================
-- 启用外键约束
PRAGMA foreign_keys = ON;

-- 启用 WAL 模式（性能优化）
PRAGMA journal_mode = WAL;

-- 优化设置
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = -10000;
PRAGMA temp_store = MEMORY;

