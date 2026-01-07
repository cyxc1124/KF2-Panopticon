"""数据库服务层"""
from flask import g
from app.models import get_database


def get_db_connection():
    """获取数据库连接（Flask g对象缓存）"""
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = get_database()
    return db


def close_db_connection(exception):
    """关闭数据库连接"""
    db = g.pop('_database', None)
    if db is not None:
        db.close()


def get_global_stats(cur):
    """获取全局统计数据"""
    try:
        cur.execute("SELECT COUNT(*) as count FROM fact_active")
        players_row = cur.fetchone()
        players = players_row['count'] if players_row else 0
        
        cur.execute("SELECT COUNT(*) as count FROM dim_servers WHERE player_count > 0")
        active_servers_row = cur.fetchone()
        active_servers = active_servers_row['count'] if active_servers_row else 0
        
        cur.execute("SELECT COUNT(*) as count FROM dim_servers")
        total_servers_row = cur.fetchone()
        total_servers = total_servers_row['count'] if total_servers_row else 0
        
        stats = {
            'players': players,
            'active_servers': active_servers,
            'total_servers': total_servers
        }
        
        if stats['total_servers'] > 0:
            stats['occupancy'] = round((stats['active_servers'] / stats['total_servers']) * 100, 1)
        else:
            stats['occupancy'] = 0
        return stats
    except Exception:
        return {'players': 0, 'active_servers': 0, 'total_servers': 0, 'occupancy': 0}

