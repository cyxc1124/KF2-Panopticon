"""
数据库抽象层 - 支持 SQLite 和 PostgreSQL
"""
import os
from contextlib import contextmanager

try:
    import config
except ImportError:
    config = None

class DatabaseConfig:
    """数据库配置"""
    def __init__(self):
        # 数据库类型: 'sqlite' 或 'postgresql'
        self.db_type = os.environ.get('DB_TYPE', getattr(config, 'DB_TYPE', 'sqlite')).lower()
        
        # SQLite 配置
        self.sqlite_db_file = getattr(config, 'DB_FILE', 'kf2_panopticon.db')
        
        # PostgreSQL 配置
        self.pg_host = os.environ.get('POSTGRES_HOST', getattr(config, 'POSTGRES_HOST', 'localhost'))
        self.pg_port = int(os.environ.get('POSTGRES_PORT', getattr(config, 'POSTGRES_PORT', 5432)))
        self.pg_database = os.environ.get('POSTGRES_DB', getattr(config, 'POSTGRES_DB', 'kf2_panopticon'))
        self.pg_user = os.environ.get('POSTGRES_USER', getattr(config, 'POSTGRES_USER', 'kf2user'))
        self.pg_password = os.environ.get('POSTGRES_PASSWORD', getattr(config, 'POSTGRES_PASSWORD', ''))
        
    def get_connection_string(self):
        """获取数据库连接字符串"""
        if self.db_type == 'postgresql':
            return f"host={self.pg_host} port={self.pg_port} dbname={self.pg_database} user={self.pg_user} password={self.pg_password}"
        return self.sqlite_db_file


class DatabaseAdapter:
    """数据库适配器 - 统一 SQLite 和 PostgreSQL 的接口差异"""
    
    def __init__(self, db_type='sqlite'):
        self.db_type = db_type
        
    def get_driver(self):
        """获取数据库驱动模块"""
        if self.db_type == 'postgresql':
            import psycopg2
            import psycopg2.extras
            return psycopg2
        else:
            import sqlite3
            return sqlite3
    
    def connect(self, connection_string):
        """创建数据库连接"""
        driver = self.get_driver()
        
        if self.db_type == 'postgresql':
            conn = driver.connect(connection_string)
            # 使用 RealDictCursor 使返回结果类似 sqlite3.Row
            return conn
        else:
            conn = driver.connect(connection_string)
            conn.row_factory = driver.Row
            return conn
    
    def get_cursor(self, conn):
        """获取游标"""
        if self.db_type == 'postgresql':
            import psycopg2.extras
            return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            return conn.cursor()
    
    def get_placeholder(self):
        """获取参数占位符"""
        if self.db_type == 'postgresql':
            return '%s'  # PostgreSQL 使用 %s
        else:
            return '?'   # SQLite 使用 ?
    
    def convert_placeholders(self, sql, params=None):
        """转换 SQL 中的占位符"""
        if self.db_type == 'postgresql':
            # 将 ? 替换为 %s
            sql = sql.replace('?', '%s')
        return sql, params
    
    def adapt_sql(self, sql):
        """适配不同数据库的 SQL 语法差异"""
        if self.db_type == 'postgresql':
            # PostgreSQL 使用 SERIAL 而不是 AUTOINCREMENT
            sql = sql.replace('AUTOINCREMENT', '')
            sql = sql.replace('INTEGER PRIMARY KEY AUTOINCREMENT', 'SERIAL PRIMARY KEY')
            
            # PostgreSQL 的 datetime 函数不同
            sql = sql.replace('datetime(', 'TIMESTAMP ')
            sql = sql.replace("datetime('now'", "NOW(")
            sql = sql.replace("date('now'", "CURRENT_DATE")
            sql = sql.replace('strftime(', 'to_char(')
            
            # INSERT OR IGNORE -> INSERT ... ON CONFLICT DO NOTHING
            if 'INSERT OR IGNORE INTO' in sql:
                # 需要知道唯一约束才能完全转换，这里做简单处理
                sql = sql.replace('INSERT OR IGNORE INTO', 'INSERT INTO')
                # 实际使用时需要添加 ON CONFLICT DO NOTHING
        
        return sql
    
    def enable_foreign_keys(self, conn):
        """启用外键约束"""
        if self.db_type == 'sqlite':
            conn.execute("PRAGMA foreign_keys = ON;")
    
    def optimize_connection(self, conn):
        """优化数据库连接"""
        if self.db_type == 'sqlite':
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-10000")  # 40MB
            conn.execute("PRAGMA temp_store=MEMORY")
        elif self.db_type == 'postgresql':
            # PostgreSQL 的优化通常在 postgresql.conf 中配置
            pass


class Database:
    """统一的数据库接口"""
    
    def __init__(self, config=None):
        self.config = config or DatabaseConfig()
        self.adapter = DatabaseAdapter(self.config.db_type)
        self._connection = None
    
    def connect(self):
        """建立数据库连接"""
        if self._connection is None:
            conn_string = self.config.get_connection_string()
            self._connection = self.adapter.connect(conn_string)
            self.adapter.enable_foreign_keys(self._connection)
            self.adapter.optimize_connection(self._connection)
        return self._connection
    
    def close(self):
        """关闭数据库连接"""
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def cursor(self):
        """获取游标"""
        conn = self.connect()
        return self.adapter.get_cursor(conn)
    
    def execute(self, sql, params=None):
        """执行 SQL"""
        conn = self.connect()
        sql = self.adapter.adapt_sql(sql)
        sql, params = self.adapter.convert_placeholders(sql, params)
        
        cur = self.adapter.get_cursor(conn)
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        return cur
    
    def executemany(self, sql, params_list):
        """批量执行 SQL"""
        conn = self.connect()
        sql = self.adapter.adapt_sql(sql)
        
        cur = self.adapter.get_cursor(conn)
        cur.executemany(sql, params_list)
        return cur
    
    def commit(self):
        """提交事务"""
        if self._connection:
            self._connection.commit()
    
    def rollback(self):
        """回滚事务"""
        if self._connection:
            self._connection.rollback()
    
    @contextmanager
    def transaction(self):
        """事务上下文管理器"""
        try:
            yield self
            self.commit()
        except Exception as e:
            self.rollback()
            raise e
    
    @property
    def db_type(self):
        """获取数据库类型"""
        return self.config.db_type


# 全局数据库实例
_db_instance = None

def get_database():
    """获取数据库单例"""
    global _db_instance
    if _db_instance is None:
        _db_instance = Database()
    return _db_instance


# 兼容旧代码的辅助函数
def get_connection():
    """获取数据库连接（兼容旧代码）"""
    db = get_database()
    return db.connect()

