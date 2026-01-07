"""
数据库抽象层 - PostgreSQL (with connection pooling)
"""
import os
import threading
from contextlib import contextmanager

try:
    import config
except ImportError:
    config = None

# 全局连接池（线程安全）
_connection_pool = None
_pool_lock = threading.Lock()


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
    """PostgreSQL 数据库接口（使用连接池）"""
    
    def __init__(self, config=None):
        self.config = config or DatabaseConfig()
        self.db_type = 'postgresql'
        # 使用 threading.local() 存储每个线程当前使用的连接
        self._local = threading.local()
        # 初始化连接池
        self._init_pool()
    
    def _init_pool(self):
        """初始化连接池（全局单例）"""
        global _connection_pool
        if _connection_pool is None:
            with _pool_lock:
                if _connection_pool is None:  # Double-check locking
                    import time
                    from psycopg2 import pool
                    start = time.time()
                    
                    # 从环境变量读取连接池配置
                    min_conn = int(os.environ.get('DB_POOL_MIN', '2'))
                    max_conn = int(os.environ.get('DB_POOL_MAX', '10'))
                    
                    _connection_pool = pool.ThreadedConnectionPool(
                        minconn=min_conn,
                        maxconn=max_conn,
                        host=self.config.pg_host,
                        port=self.config.pg_port,
                        database=self.config.pg_database,
                        user=self.config.pg_user,
                        password=self.config.pg_password
                    )
                    duration = (time.time() - start) * 1000
                    print(f"[INFO] Connection pool initialized: min={min_conn}, max={max_conn}, duration={duration:.2f}ms")
    
    def connect(self):
        """从连接池获取连接（线程安全）"""
        import time
        
        # 检查是否已有连接
        has_connection = hasattr(self._local, 'connection')
        is_none = self._local.connection is None if has_connection else True
        is_closed = self._local.connection.closed if (has_connection and not is_none) else True
        
        if not has_connection or is_none or is_closed:
            start = time.time()
            print(f"[DEBUG] Thread {threading.current_thread().name}: Requesting new connection from pool...")
            self._local.connection = _connection_pool.getconn()
            duration = (time.time() - start) * 1000
            print(f"[DEBUG] Thread {threading.current_thread().name}: Got connection from pool in {duration:.2f}ms (new={not has_connection}, closed={is_closed})")
        else:
            print(f"[DEBUG] Thread {threading.current_thread().name}: Reusing existing connection")
        
        return self._local.connection
    
    def close(self):
        """将连接归还到连接池（而不是真正关闭）"""
        if hasattr(self._local, 'connection') and self._local.connection and not self._local.connection.closed:
            _connection_pool.putconn(self._local.connection)
            print(f"[DEBUG] Thread {threading.current_thread().name}: Returned connection to pool")
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
