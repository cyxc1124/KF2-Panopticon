"""
数据库初始化模块
自动检测并初始化数据库表结构（幂等操作）
"""
import os
import sys
from pathlib import Path
from app.models.database import get_database




def init_database(force=False):
    """
    初始化数据库表结构（幂等操作）
    
    Args:
        force: 是否强制执行（即使已初始化）
        
    Returns:
        bool: 初始化是否成功
    """
    db = get_database()
    
    print("=" * 80)
    print("Database Initialization")
    print("=" * 80)
    
    # 检查是否已初始化
    if not force:
        try:
            with db.cursor() as cur:
                cur.execute("SELECT COUNT(*) as cnt FROM meta_kv WHERE key = 'db_initialized'")
                result = cur.fetchone()
                if result and result['cnt'] > 0:
                    print("[OK] Database already initialized, skipping")
                    return True
        except Exception:
            # 表可能不存在，继续初始化
            pass
    
    print(f"\n数据库类型: {db.db_type}")
    
    if db.db_type == 'postgresql':
        return _init_postgresql(db)
    else:
        return _init_sqlite(db)


def _init_postgresql(db):
    """初始化 PostgreSQL 数据库"""
    print("\n正在初始化 PostgreSQL 数据库...")
    
    # 读取 SQL 脚本
    sql_file = Path(__file__).parent.parent.parent / 'init_postgresql.sql'
    
    if not sql_file.exists():
        print(f"[ERROR] Initialization script not found: {sql_file}")
        return False
    
    try:
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        # 执行 SQL 脚本
        conn = db.connect()
        cur = conn.cursor()
        
        # 分割并执行每个语句
        statements = []
        current_statement = []
        
        for line in sql_script.split('\n'):
            # 跳过注释和空行
            stripped = line.strip()
            if not stripped or stripped.startswith('--'):
                continue
            
            current_statement.append(line)
            
            # 如果行以分号结束，执行该语句
            if stripped.endswith(';'):
                statement = '\n'.join(current_statement)
                statements.append(statement)
                current_statement = []
        
        # 执行所有语句
        success_count = 0
        for i, statement in enumerate(statements, 1):
            try:
                cur.execute(statement)
                success_count += 1
            except Exception as e:
                # 某些语句可能失败（如显示表列表），但不影响初始化
                if 'CREATE' in statement or 'INSERT' in statement:
                    print(f"[WARN] Statement {i} execution failed: {str(e)[:50]}")
        
        conn.commit()
        
        # 标记已初始化
        cur.execute("""
            INSERT INTO meta_kv (key, value) 
            VALUES ('db_initialized', 'true') 
            ON CONFLICT (key) DO UPDATE SET value = 'true'
        """)
        conn.commit()
        
        print(f"\n[OK] PostgreSQL initialization completed!")
        print(f"   成功执行 {success_count}/{len(statements)} 条语句")
        return True
        
    except Exception as e:
        print(f"\n[ERROR] PostgreSQL initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def _init_sqlite(db):
    """初始化 SQLite 数据库"""
    print("\n正在初始化 SQLite 数据库...")
    
    # 读取 SQL 脚本
    sql_file = Path(__file__).parent.parent.parent / 'init_sqlite.sql'
    
    if not sql_file.exists():
        print(f"[ERROR] Initialization script not found: {sql_file}")
        return False
    
    try:
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        # 执行 SQL 脚本
        conn = db.connect()
        cur = conn.cursor()
        
        # 分割并执行每个语句
        statements = []
        current_statement = []
        
        for line in sql_script.split('\n'):
            # 跳过注释和空行
            stripped = line.strip()
            if not stripped or stripped.startswith('--'):
                continue
            
            current_statement.append(line)
            
            # 如果行以分号结束，执行该语句
            if stripped.endswith(';'):
                statement = '\n'.join(current_statement)
                statements.append(statement)
                current_statement = []
        
        # 执行所有语句
        success_count = 0
        for i, statement in enumerate(statements, 1):
            try:
                cur.execute(statement)
                success_count += 1
            except Exception as e:
                # 某些语句可能失败（如 PRAGMA），但不影响初始化
                if 'CREATE' in statement or 'INSERT' in statement:
                    print(f"[WARN] Statement {i} execution failed: {str(e)[:50]}")
        
        conn.commit()
        
        # 标记已初始化
        cur.execute("""
            INSERT OR IGNORE INTO meta_kv (key, value) 
            VALUES ('db_initialized', 'true')
        """)
        conn.commit()
        
        print(f"\n[OK] SQLite initialization completed!")
        print(f"   成功执行 {success_count}/{len(statements)} 条语句")
        return True
        
    except Exception as e:
        print(f"\n[ERROR] SQLite initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def check_database_status():
    """
    检查数据库状态
    
    Returns:
        dict: 数据库状态信息
    """
    db = get_database()
    status = {
        'type': db.db_type,
        'initialized': False,
        'tables': [],
        'error': None
    }
    
    try:
        conn = db.connect()
        cur = conn.cursor()
        
        # 获取表列表
        if db.db_type == 'postgresql':
            cur.execute("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'public'
                ORDER BY tablename
            """)
        else:
            cur.execute("""
                SELECT name 
                FROM sqlite_master 
                WHERE type='table' 
                ORDER BY name
            """)
        
        status['tables'] = [row[0] for row in cur.fetchall()]
        status['initialized'] = len(status['tables']) > 0
        
        # 检查初始化标记
        if 'meta_kv' in status['tables']:
            cur.execute("SELECT value FROM meta_kv WHERE key = 'db_initialized'")
            result = cur.fetchone()
            if result:
                status['db_marked_initialized'] = True
        
    except Exception as e:
        status['error'] = str(e)
    
    return status


if __name__ == '__main__':
    """命令行工具：手动初始化数据库"""
    import sys
    
    force = '--force' in sys.argv
    
    if init_database(force=force):
        print("\n" + "=" * 80)
        print("[OK] Database initialization successful!")
        print("=" * 80)
        
        # 显示状态
        status = check_database_status()
        print(f"\n数据库类型: {status['type']}")
        print(f"表数量: {len(status['tables'])}")
        print(f"表列表: {', '.join(status['tables'][:5])}...")
        
        sys.exit(0)
    else:
        print("\n" + "=" * 80)
        print("[ERROR] Database initialization failed")
        print("=" * 80)
        sys.exit(1)

