#!/usr/bin/env python3
"""
KF2-Panopticon Web 应用入口
"""
import os
import sys

# 确保可以导入 config
sys.path.insert(0, os.path.dirname(__file__))

from app import create_app
from app.services.db_service import close_db_connection

# 创建应用实例
app = create_app()

# 注册teardown处理器
app.teardown_appcontext(close_db_connection)

# 预热数据库连接池（避免第一次请求慢）
def warmup_connection_pool():
    """在应用启动时预热连接池"""
    import time
    print("[INFO] Warming up database connection pool...")
    start = time.time()
    try:
        from app.models import get_database
        db = get_database()
        # 触发连接池初始化
        db.connect()
        duration = (time.time() - start) * 1000
        print(f"[OK] Connection pool warmed up in {duration:.2f}ms")
    except Exception as e:
        print(f"[WARN] Failed to warm up connection pool: {e}")

if __name__ == '__main__':
    import config
    
    # 从配置获取端口和调试模式
    port = config.WEB_PORT
    debug = config.DEBUG_MODE
    
    print(f"""
╔═══════════════════════════════════════════════════════════╗
║   KF2-Panopticon Web Application                         ║
╚═══════════════════════════════════════════════════════════╝

Startup Info:
  - Port: {port}
  - Debug Mode: {debug}
  - Database: PostgreSQL
  - Access URL: http://localhost:{port}
  - Health Check: http://localhost:{port}/health
  - Readiness Check: http://localhost:{port}/ready

Note: Database should be initialized before starting web app.
      Run 'python init_db.py' if database is not initialized.
  
""")
    
    # 预热连接池（避免第一次请求慢）
    warmup_connection_pool()
    
    app.run(debug=debug, port=port, host='0.0.0.0')

