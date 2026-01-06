"""
健康检查路由 - 用于容器编排平台（K8s/Docker）
"""
from flask import Blueprint, jsonify
from app.models.database import get_database
import time

health_bp = Blueprint('health', __name__)


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
    """
    try:
        db = get_database()
        with db.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        
        return jsonify({
            "status": "ready",
            "database": "connected",
            "timestamp": time.time()
        }), 200
    
    except Exception as e:
        return jsonify({
            "status": "not ready",
            "database": "disconnected",
            "error": str(e),
            "timestamp": time.time()
        }), 503

