"""
KF2-Panopticon Flask 应用工厂
"""
import os
from flask import Flask
from app.utils.cache import DataCache

# 全局缓存实例
cache = DataCache()


def create_app(config_name=None):
    """应用工厂函数"""
    app = Flask(__name__)
    
    # 加载配置
    if config_name:
        app.config.from_object(config_name)
    else:
        # 尝试从 config 模块加载
        try:
            import config
            app.config.from_object(config)
        except ImportError:
            pass
    
    # 设置密钥
    app.secret_key = app.config.get('SECRET_KEY', os.environ.get('SECRET_KEY', 'dev-secret-key'))
    
    # 注册模板过滤器
    from app.utils.helpers import format_duration, parse_location
    app.jinja_env.filters['human_time'] = format_duration
    
    # 注册蓝图
    from app.routes import main_bp, servers_bp, players_bp, factions_bp, stats_bp
    
    app.register_blueprint(main_bp)
    app.register_blueprint(servers_bp)
    app.register_blueprint(players_bp)
    app.register_blueprint(factions_bp)
    app.register_blueprint(stats_bp)
    
    # 性能监控
    register_performance_monitoring(app)
    
    return app


def register_performance_monitoring(app):
    """注册性能监控"""
    import logging
    import json
    import time
    import threading
    from flask import g, request
    
    # 配置日志
    perf_logger = logging.getLogger('performance')
    perf_logger.setLevel(logging.INFO)
    
    log_file_path = app.config.get('LOG_FILE', 'logs/performance_debug.jsonl')
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    
    file_handler = logging.FileHandler(log_file_path)
    perf_logger.addHandler(file_handler)
    
    def write_log_background(log_entry):
        """后台写入日志"""
        perf_logger.info(json.dumps(log_entry))
    
    @app.before_request
    def start_request_timer():
        """请求开始计时"""
        g.request_start_time = time.time()
        g.perf_steps = []
    
    @app.teardown_request
    def log_performance(exception=None):
        """记录性能数据"""
        if hasattr(g, 'request_start_time'):
            total_duration = (time.time() - g.request_start_time) * 1000
            
            log_entry = {
                "timestamp": time.time(),
                "endpoint": request.endpoint,
                "method": request.method,
                "total_duration_ms": round(total_duration, 2),
                "breakdown": getattr(g, 'perf_steps', [])
            }
            
            threading.Thread(target=write_log_background, args=(log_entry,)).start()

