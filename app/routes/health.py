"""
健康检查路由 - 用于容器编排平台（K8s/Docker）
"""
from flask import Blueprint, jsonify
from app.models.database import get_database
import time

health_bp = Blueprint('health', __name__)

# 健康检查专用连接（不频繁关闭）
_health_check_db = None

def init_health_check_connection():
    """
    初始化健康检查专用连接
    在应用启动时调用，避免第一次健康检查慢
    """
    global _health_check_db
    print("[INFO] Initializing health check connection...")
    start = time.time()
    _health_check_db = get_database()
    # 预先建立连接
    conn = _health_check_db.connect()
    # 验证连接可用
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()
    duration = (time.time() - start) * 1000
    print(f"[OK] Health check connection initialized in {duration:.2f}ms")

def get_health_check_connection():
    """
    获取健康检查专用连接
    避免每次探测都重新获取连接
    """
    global _health_check_db
    if _health_check_db is None:
        # 如果未初始化，立即初始化（降级处理）
        init_health_check_connection()
    return _health_check_db


@health_bp.route('/health')
def health_check():
    """
    存活性检查 (Liveness Probe)
    用于判断应用是否还在运行
    """
    return jsonify({
        "status": "healthy",
        "timestamp": time.time()
    }), 200


@health_bp.route('/ready')
def readiness_check():
    """
    就绪性检查 (Readiness Probe)
    用于判断应用是否准备好接收流量（检查数据库连接）
    
    优化：使用专用持久连接，避免每次探测都重新获取连接
    """
    try:
        # 使用健康检查专用连接（持久化，不频繁关闭）
        db = get_health_check_connection()
        conn = db.connect()
        
        # 快速检查：验证连接是否存活
        if conn.closed:
            raise Exception("Database connection is closed")
        
        # 简单查询验证连接可用（使用超时）
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            if result is None:
                raise Exception("Database query returned no result")
        
        return jsonify({
            "status": "ready",
            "database": "connected",
            "timestamp": time.time()
        }), 200
    
    except Exception as e:
        # 如果连接失败，重置健康检查连接（下次会重新初始化）
        global _health_check_db
        print(f"[WARN] Health check failed: {e}, resetting connection...")
        _health_check_db = None
        
        return jsonify({
            "status": "not ready",
            "database": "disconnected",
            "error": str(e),
            "timestamp": time.time()
        }), 503

