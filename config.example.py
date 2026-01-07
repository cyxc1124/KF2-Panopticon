"""
KF2-Panopticon 配置示例文件
复制此文件为 config.py 并填写你的配置
"""
import os

# ==================== Steam API 配置 ====================
# 在此处设置你的 Steam Web API Key
# 获取地址: https://steamcommunity.com/dev/apikey
STEAM_KEY = "YOUR_STEAM_API_KEY_HERE"  # 替换为你的 Steam API Key

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
# Flask 密钥 (用于会话安全,请在生产环境修改为随机字符串)
SECRET_KEY = os.environ.get('SECRET_KEY', 'change-this-to-a-random-secret-key-in-production')

# Web 服务器端口
WEB_PORT = 9001

# 调试模式 (生产环境请设置为 False)
DEBUG_MODE = True

# ==================== 查询配置 ====================
# 本地回环 IP (如果在游戏服务器上运行收集器)
LOCAL_LOOPBACK_IP = "127.0.0.1"

# 最大并发线程数 (根据你的网络和机器性能调整)
MAX_WORKERS = 150

# 查询超时时间 (秒)
TIMEOUT = 3.0

# 清理阈值 (分钟) - 超过此时间未见的玩家会被移到历史记录
PRUNE_THRESHOLD = 6

# Steam API 配置
APP_ID = 232090  # Killing Floor 2 的 Steam App ID

# ==================== 缓存配置 ====================
# 缓存过期时间 (秒)
CACHE_TTL = 300  # 5分钟

# ==================== 分页配置 ====================
# 每页显示条目数
PER_PAGE = 50

