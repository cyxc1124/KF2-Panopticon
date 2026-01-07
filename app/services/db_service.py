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
        # 优化：合并为一个查询，减少数据库往返次数
        cur.execute("""
            SELECT 
                (SELECT COUNT(*) FROM fact_active) as active_players,
                (SELECT COUNT(*) FROM dim_servers WHERE player_count > 0) as active_servers,
                (SELECT COUNT(*) FROM dim_servers) as total_servers
        """)
        row = cur.fetchone()
        
        stats = {
            'players': row['active_players'] if row else 0,
            'active_servers': row['active_servers'] if row else 0,
            'total_servers': row['total_servers'] if row else 0
        }
        
        if stats['total_servers'] > 0:
            stats['occupancy'] = round((stats['active_servers'] / stats['total_servers']) * 100, 1)
        else:
            stats['occupancy'] = 0
        
        return stats
    except Exception:
        return {'players': 0, 'active_servers': 0, 'total_servers': 0, 'occupancy': 0}

