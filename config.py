"""
KF2-Panopticon 配置文件
在此文件中设置你的配置,避免直接修改源代码
"""
import os

# ==================== Steam API 配置 ====================
# 在此处设置你的 Steam Web API Key
# 获取地址: https://steamcommunity.com/dev/apikey
STEAM_KEY = ""  # 请填写你的 Steam API Key

# ==================== 数据库配置 ====================
# 数据库类型: 'sqlite' 或 'postgresql'
DB_TYPE = os.environ.get('DB_TYPE', 'sqlite')

# SQLite 配置
DB_FILE = os.path.join(os.path.dirname(__file__), "kf2_panopticon.db")

# PostgreSQL 配置
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = int(os.environ.get('POSTGRES_PORT', '5432'))
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'kf2_panopticon')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'kf2user')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', '')

# ==================== Web 服务器配置 ====================
# Flask 密钥 (用于会话安全)
SECRET_KEY = os.environ.get('SECRET_KEY', 'your-secret-key-here-change-in-production')

# Web 服务器端口
WEB_PORT = 9001

# 调试模式 (生产环境请设置为 False)
DEBUG_MODE = True

# 日志文件路径
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
LOG_FILE = os.path.join(LOG_DIR, "performance_debug.jsonl")

# ==================== 查询配置 ====================
# 本地回环 IP (如果在服务器上运行)
LOCAL_LOOPBACK_IP = "127.0.0.1"

# 最大并发线程数
MAX_WORKERS = 150

# 查询超时时间 (秒)
TIMEOUT = 3.0

# 清理阈值 (分钟)
PRUNE_THRESHOLD = 6

# Steam API 配置
APP_ID = 232090  # Killing Floor 2 的 Steam App ID

# ==================== 缓存配置 ====================
# 缓存过期时间 (秒)
CACHE_TTL = 300  # 5分钟

# ==================== 分页配置 ====================
# 每页显示条目数
PER_PAGE = 50

