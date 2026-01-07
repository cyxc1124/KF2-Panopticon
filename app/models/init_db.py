"""
数据库初始化模块
自动检测并初始化数据库表结构（幂等操作）
"""
import os
import sys
from pathlib import Path
from app.models.database import get_database


def _ensure_postgresql_database_exists():
    """
    确保 PostgreSQL 数据库存在，如果不存在则创建
    
    Returns:
        bool: 数据库是否存在或创建成功
    """
    try:
        import config
        import psycopg2
        from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
        
        # 连接到默认的 postgres 数据库
        conn = psycopg2.connect(
            host=config.POSTGRES_HOST,
            port=config.POSTGRES_PORT,
            user=config.POSTGRES_USER,
            password=config.POSTGRES_PASSWORD,
            database='postgres'  # 连接到默认数据库
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # 检查目标数据库是否存在
        cur.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (config.POSTGRES_DB,)
        )
        
        database_created = False
        if cur.fetchone():
            print(f"[OK] Database '{config.POSTGRES_DB}' already exists")
        else:
            # 数据库不存在，创建它
            print(f"[INFO] Database '{config.POSTGRES_DB}' does not exist, creating...")
            cur.execute(f'CREATE DATABASE "{config.POSTGRES_DB}"')
            print(f"[OK] Database '{config.POSTGRES_DB}' created successfully")
            database_created = True
        
        cur.close()
        conn.close()
        
        # 如果创建了新数据库，需要重置全局连接池
        if database_created:
            print("[INFO] Resetting connection pool after database creation...")
            _reset_connection_pool()
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to ensure database exists: {e}")
        print("\nPlease manually create the database:")
        print(f"  psql -U {config.POSTGRES_USER} -h {config.POSTGRES_HOST} -c \"CREATE DATABASE {config.POSTGRES_DB};\"")
        return False


def _reset_connection_pool():
    """重置全局连接池（在创建数据库后调用）"""
    try:
        from app.models import database
        
        # 关闭现有连接池
        if database._connection_pool is not None:
            print("[DEBUG] Closing existing connection pool...")
            database._connection_pool.closeall()
            database._connection_pool = None
            print("[DEBUG] Connection pool reset successfully")
    except Exception as e:
        print(f"[WARN] Failed to reset connection pool: {e}")


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
    
    # 确保数据库存在
    if not _ensure_postgresql_database_exists():
        return False
    
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
    
    # 初始化 PostgreSQL
    return _init_postgresql(db)


def _init_postgresql(db):
    """初始化 PostgreSQL 数据库"""
    print("\nInitializing PostgreSQL database...")
    
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
        
        # 设置为 AUTOCOMMIT 模式，避免事务中止问题
        import psycopg2
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        
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
        failed_count = 0
        for i, statement in enumerate(statements, 1):
            try:
                cur.execute(statement)
                success_count += 1
            except Exception as e:
                failed_count += 1
                error_msg = str(e)
                # 只对关键语句显示警告
                if any(keyword in statement.upper() for keyword in ['CREATE TABLE', 'CREATE INDEX', 'INSERT INTO']):
                    # 如果是 "already exists" 类型的错误，说明是幂等操作，不显示警告
                    if 'already exists' not in error_msg.lower():
                        print(f"[WARN] Statement {i} failed: {error_msg[:80]}")
        
        # 标记已初始化
        try:
            cur.execute("""
                INSERT INTO meta_kv (key, value) 
                VALUES ('db_initialized', 'true') 
                ON CONFLICT (key) DO UPDATE SET value = 'true'
            """)
        except Exception as e:
            print(f"[WARN] Could not mark as initialized: {e}")
        
        cur.close()
        
        print(f"\n[OK] PostgreSQL initialization completed!")
        print(f"   Successful: {success_count}/{len(statements)} statements")
        if failed_count > 0:
            print(f"   Failed: {failed_count} statements (may be expected for idempotent operations)")
        return True
        
    except Exception as e:
        print(f"\n[ERROR] PostgreSQL initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def check_database_status():
    """
    检查数据库状态
    
    Returns:
        dict: 包含数据库状态信息的字典
    """
    db = get_database()
    status = {
        'type': 'postgresql',
        'initialized': False,
        'tables': [],
        'error': None
    }
    
    try:
        with db.cursor() as cur:
            # 检查是否已初始化
            try:
                cur.execute("SELECT COUNT(*) as cnt FROM meta_kv WHERE key = 'db_initialized'")
                result = cur.fetchone()
                if result and result['cnt'] > 0:
                    status['initialized'] = True
            except:
                pass
            
            # 获取表列表
            cur.execute("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'public'
                ORDER BY tablename
            """)
            status['tables'] = [row['tablename'] for row in cur.fetchall()]
            
    except Exception as e:
        status['error'] = str(e)
    
    return status
