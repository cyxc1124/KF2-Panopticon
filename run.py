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

if __name__ == '__main__':
    import config
    from app.models.init_db import init_database, check_database_status
    
    # 从配置获取端口和调试模式
    port = config.WEB_PORT
    debug = config.DEBUG_MODE
    db_type = config.DB_TYPE
    
    print(f"""
╔═══════════════════════════════════════════════════════════╗
║   KF2-Panopticon Web Application                         ║
╚═══════════════════════════════════════════════════════════╝

启动信息:
  - 端口: {port}
  - 调试模式: {debug}
  - 数据库类型: {db_type}
  - 访问地址: http://localhost:{port}
  - 健康检查: http://localhost:{port}/health
  - 就绪检查: http://localhost:{port}/ready
  
""")
    
    # 自动初始化数据库（幂等操作）
    print("检查数据库状态...\n")
    
    # 自动初始化（PostgreSQL 和 SQLite 都支持）
    init_database()
    
    print("-" * 63)
    
    app.run(debug=debug, port=port, host='0.0.0.0')

