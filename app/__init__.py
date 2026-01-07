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
    
    # 添加 datetime 格式化过滤器
    from datetime import datetime
    def format_datetime(value):
        """格式化 datetime 对象为字符串"""
        if isinstance(value, datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        return str(value) if value else ''
    
    app.jinja_env.filters['datetime_str'] = format_datetime
    
    # 注册蓝图
    from app.routes import main_bp, servers_bp, players_bp, factions_bp, stats_bp
    from app.routes.health import health_bp
    
    app.register_blueprint(main_bp)
    app.register_blueprint(servers_bp)
    app.register_blueprint(players_bp)
    app.register_blueprint(factions_bp)
    app.register_blueprint(stats_bp)
    app.register_blueprint(health_bp)
    
    # 性能监控
    register_performance_monitoring(app)
    
    return app


def register_performance_monitoring(app):
    """注册性能监控"""
    import logging
    import json
    import time
    import sys
    from flask import g, request
    
    # 配置日志输出到控制台（适用于 K8s 环境）
    perf_logger = logging.getLogger('performance')
    perf_logger.setLevel(logging.INFO)
    
    # 输出到 stdout，方便 kubectl logs 查看
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(message)s'))
    perf_logger.addHandler(console_handler)
    
    def write_log_background(log_entry):
        """输出日志到控制台"""
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
            
            # 直接输出到控制台，不需要后台线程
            write_log_background(log_entry)

