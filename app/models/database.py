"""
数据库抽象层 - PostgreSQL
"""
import os
import threading
from contextlib import contextmanager

try:
    import config
except ImportError:
    config = None


class DatabaseConfig:
    """PostgreSQL 数据库配置"""
    def __init__(self):
        # PostgreSQL 配置
        self.pg_host = os.environ.get('POSTGRES_HOST', getattr(config, 'POSTGRES_HOST', 'localhost'))
        self.pg_port = int(os.environ.get('POSTGRES_PORT', getattr(config, 'POSTGRES_PORT', 5432)))
        self.pg_database = os.environ.get('POSTGRES_DB', getattr(config, 'POSTGRES_DB', 'kf2_panopticon'))
        self.pg_user = os.environ.get('POSTGRES_USER', getattr(config, 'POSTGRES_USER', 'kf2user'))
        self.pg_password = os.environ.get('POSTGRES_PASSWORD', getattr(config, 'POSTGRES_PASSWORD', ''))
        
    def get_connection_string(self):
        """获取 PostgreSQL 连接字符串"""
        return (
            f"host={self.pg_host} "
            f"port={self.pg_port} "
            f"dbname={self.pg_database} "
            f"user={self.pg_user} "
            f"password={self.pg_password}"
        )


class Database:
    """PostgreSQL 数据库接口（线程安全）"""
    
    def __init__(self, config=None):
        self.config = config or DatabaseConfig()
        self.db_type = 'postgresql'
        # 使用 threading.local() 存储每个线程的连接
        self._local = threading.local()
    
    def connect(self):
        """建立数据库连接（线程安全）"""
        if not hasattr(self._local, 'connection') or self._local.connection is None:
            import psycopg2
            conn_string = self.config.get_connection_string()
            self._local.connection = psycopg2.connect(conn_string)
        return self._local.connection
    
    def close(self):
        """关闭当前线程的数据库连接"""
        if hasattr(self._local, 'connection') and self._local.connection:
            self._local.connection.close()
            self._local.connection = None
    
    @contextmanager
    def cursor(self):
        """获取游标（支持上下文管理器）"""
        conn = self.connect()
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield cur
        finally:
            cur.close()
    
    def get_cursor(self):
        """直接获取游标（不使用上下文管理器）"""
        conn = self.connect()
        import psycopg2.extras
        return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    def commit(self):
        """提交事务"""
        if hasattr(self._local, 'connection') and self._local.connection:
            self._local.connection.commit()
    
    def rollback(self):
        """回滚事务"""
        if hasattr(self._local, 'connection') and self._local.connection:
            self._local.connection.rollback()


# 全局数据库实例
_db_instance = None

def get_database():
    """获取全局数据库实例（单例模式）"""
    global _db_instance
    if _db_instance is None:
        _db_instance = Database()
    return _db_instance
