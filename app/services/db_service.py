"""数据库服务层"""
from flask import g
from app.models import get_database


def get_db_connection():
    """获取数据库连接（Flask g对象缓存）"""
    db = getattr(g, '_database', None)
    if db is None:
        db_instance = get_database()
        db = g._database = db_instance.connect()
    return db


def close_db_connection(exception):
    """关闭数据库连接"""
    db = g.pop('_database', None)
    if db is not None:
        db.close()


def get_global_stats(cur):
    """获取全局统计数据"""
    try:
        stats = {
            'players': cur.execute("SELECT COUNT(*) FROM fact_active").fetchone()[0],
            'active_servers': cur.execute("SELECT COUNT(*) FROM dim_servers WHERE player_count > 0").fetchone()[0],
            'total_servers': cur.execute("SELECT COUNT(*) FROM dim_servers").fetchone()[0]
        }
        if stats['total_servers'] > 0:
            stats['occupancy'] = round((stats['active_servers'] / stats['total_servers']) * 100, 1)
        else:
            stats['occupancy'] = 0
        return stats
    except Exception:
        return {'players': 0, 'active_servers': 0, 'total_servers': 0, 'occupancy': 0}

