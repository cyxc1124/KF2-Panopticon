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
    # 从配置或环境变量获取端口
    port = app.config.get('WEB_PORT', int(os.environ.get('WEB_PORT', 9001)))
    debug = app.config.get('DEBUG_MODE', os.environ.get('FLASK_DEBUG', 'False').lower() == 'true')
    
    print(f"""
╔═══════════════════════════════════════════════════════════╗
║   KF2-Panopticon Web Application                         ║
╚═══════════════════════════════════════════════════════════╝

启动信息:
  - 端口: {port}
  - 调试模式: {debug}
  - 访问地址: http://localhost:{port}
  
""")
    
    app.run(debug=debug, port=port, host='0.0.0.0')

