"""
Gunicorn 配置文件
用于生产环境部署
"""
import os
import multiprocessing

# 服务器套接字
bind = f"0.0.0.0:{os.environ.get('WEB_PORT', '9001')}"
backlog = 2048

# Worker 进程
workers = int(os.environ.get('GUNICORN_WORKERS', multiprocessing.cpu_count() * 2 + 1))
worker_class = 'gevent'  # 使用 gevent 异步 worker
worker_connections = 1000
max_requests = 1000  # 自动重启 worker，防止内存泄漏
max_requests_jitter = 50

# 超时
timeout = 30
graceful_timeout = 30
keepalive = 2

# 进程命名
proc_name = 'kf2-panopticon'

# 日志
accesslog = '-'  # stdout
errorlog = '-'   # stdout
loglevel = 'info'
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# 预加载应用（减少内存占用）
preload_app = True

# 启动时的钩子
def on_starting(server):
    """服务器启动时执行"""
    print("="*70)
    print("Gunicorn Starting...")
    print(f"Workers: {workers}")
    print(f"Worker Class: {worker_class}")
    print(f"Bind: {bind}")
    print("="*70)

def when_ready(server):
    """服务器就绪时执行"""
    print("[OK] Gunicorn is ready to serve requests")

def pre_fork(server, worker):
    """Fork worker 前执行"""
    pass

def post_fork(server, worker):
    """Fork worker 后执行"""
    # 预热数据库连接池（在每个 worker 中）
    from run import warmup_connection_pool
    warmup_connection_pool()

