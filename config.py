"""
KF2-Panopticon 配置文件 - PostgreSQL
优先从环境变量读取配置，支持容器化部署（Docker/K8s）
"""
import os
from pathlib import Path

# 基础路径
BASE_DIR = Path(__file__).resolve().parent

# 加载 .env 文件（开发环境使用）
try:
    from dotenv import load_dotenv
    env_file = BASE_DIR / '.env'
    if env_file.exists():
        load_dotenv(env_file)
        print(f"[OK] Loaded .env file: {env_file}")
except ImportError:
    # python-dotenv 未安装，使用系统环境变量
    pass

# ==================== Steam API 配置 ====================
# 在此处设置你的 Steam Web API Key，或通过环境变量 STEAM_KEY 设置
# 获取地址: https://steamcommunity.com/dev/apikey
STEAM_KEY = os.environ.get('STEAM_KEY', '')

# 如果环境变量为空且存在 .steam_key 文件（仅本地开发用）
if not STEAM_KEY:
    steam_key_file = BASE_DIR / '.steam_key'
    if steam_key_file.exists():
        STEAM_KEY = steam_key_file.read_text().strip()

# ==================== PostgreSQL 数据库配置 ====================
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = int(os.environ.get('POSTGRES_PORT', '5432'))
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'kf2_panopticon')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'kf2user')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', '')

# ==================== Web 服务器配置 ====================
# Flask 密钥 (用于会话安全，生产环境必须设置环境变量)
SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')

# Web 服务器端口
WEB_PORT = int(os.environ.get('WEB_PORT', '9001'))

# 调试模式 (生产环境请通过环境变量设置为 false)
DEBUG_MODE = os.environ.get('DEBUG_MODE', 'true').lower() in ('true', '1', 'yes')

# 日志配置
LOG_DIR = os.environ.get('LOG_DIR', str(BASE_DIR / 'logs'))
LOG_FILE = os.path.join(LOG_DIR, 'performance_debug.jsonl')

# ==================== 查询配置 ====================
# 本地回环 IP (如果在服务器上运行)
LOCAL_LOOPBACK_IP = os.environ.get('LOCAL_LOOPBACK_IP', '127.0.0.1')

# 最大并发线程数
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', '150'))

# 查询超时时间 (秒)
TIMEOUT = float(os.environ.get('TIMEOUT', '3.0'))

# 清理阈值 (分钟)
PRUNE_THRESHOLD = int(os.environ.get('PRUNE_THRESHOLD', '6'))

# Steam API 配置
APP_ID = int(os.environ.get('STEAM_APP_ID', '232090'))  # Killing Floor 2 的 Steam App ID

# ==================== 缓存配置 ====================
# 缓存过期时间 (秒)
CACHE_TTL = int(os.environ.get('CACHE_TTL', '300'))  # 5分钟

# ==================== 分页配置 ====================
# 每页显示条目数
PER_PAGE = int(os.environ.get('PER_PAGE', '50'))
