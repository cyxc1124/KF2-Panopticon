"""统计路由蓝图"""
from flask import Blueprint, render_template
from app.services.db_service import get_db_connection
from app.utils import StepTimer
from app import cache

stats_bp = Blueprint('stats', __name__)

@stats_bp.route('/stats')
def statistics():
    """统计页面"""
    with StepTimer("Check Cache"):
        cached_stats = cache.get('stats_page')
    
    if cached_stats:
        map_stats, daily_traffic, server_stats, player_rows, chart_24h, chart_30d, chart_history = cached_stats
        with StepTimer("Cache Hit Processing"):
            pass
    else:
        cur = get_db_connection().cursor()
        
        with StepTimer("Query: Map Stats"):
            map_stats = cur.execute("""
                SELECT
                    m.name AS map,
                    SUM(d.session_count) AS session_count,
                    SUM(d.total_seconds) AS total_seconds
                FROM fact_map_daily d
                JOIN dim_maps m ON d.map_id = m.id
                WHERE d.day >= date('now', '-30 days')
                GROUP BY d.map_id
                ORDER BY total_seconds DESC
                LIMIT 10;
            """).fetchall()

        with StepTimer("Query: Daily Traffic"):
            daily_traffic = cur.execute("""
                SELECT day, unique_players
                FROM fact_traffic_daily
                WHERE day >= date('now', '-30 days')
                ORDER BY day ASC;
            """).fetchall()

        with StepTimer("Query: Top Servers"):
            server_rows = cur.execute("""
                SELECT
                    s.id,
                    s.name,
                    s.ip_address,
                    s.game_port,
                    s.query_port,
                    SUM(d.session_count) AS session_count,
                    SUM(d.total_seconds) AS total_seconds
                FROM fact_server_daily d
                JOIN dim_servers s ON d.server_id = s.id
                WHERE d.day >= date('now', '-30 days')
                GROUP BY d.server_id
                ORDER BY total_seconds DESC
                LIMIT 10;
            """).fetchall()

            server_stats = []
            for row in server_rows:
                d = dict(row)
                if d['game_port'] and d['game_port'] > 0:
                    d['address'] = f"{d['ip_address']}:{d['game_port']}"
                else:
                    d['address'] = f"{d['ip_address']}:{d['query_port']}"
                server_stats.append(d)

        with StepTimer("Query: Top Players"):
            player_rows = cur.execute("""
                SELECT
                    p.id,
                    p.name,
                    SUM(d.session_count) AS session_count,
                    SUM(d.total_seconds) AS total_seconds
                FROM fact_player_daily d
                JOIN dim_players p ON d.player_id = p.id
                WHERE d.day >= date('now', '-30 days')
                GROUP BY d.player_id
                ORDER BY total_seconds DESC
                LIMIT 10;
            """).fetchall()

        with StepTimer("Query: Chart 24h"):
            chart_24h = cur.execute("""
                SELECT scan_time, active_players, active_servers
                FROM fact_global_stats
                WHERE scan_time > datetime('now', '-24 hours')
                ORDER BY scan_time ASC
            """).fetchall()

        with StepTimer("Query: Chart 30d"):
            chart_30d = cur.execute("""
                SELECT 
                    datetime((strftime('%s', scan_time) / 14400) * 14400, 'unixepoch') as time_bucket,
                    ROUND(AVG(active_players), 1) as avg_players,
                    ROUND(AVG(active_servers), 1) as avg_servers
                FROM fact_global_stats
                WHERE scan_time > datetime('now', '-30 days')
                GROUP BY time_bucket
                ORDER BY time_bucket ASC
            """).fetchall()

        with StepTimer("Query: Chart History"):
            chart_history = cur.execute("""
                SELECT 
                    date(scan_time) as day,
                    ROUND(AVG(active_players), 1) as avg_players,
                    MAX(active_players) as max_players
                FROM fact_global_stats
                GROUP BY day
                ORDER BY day ASC
            """).fetchall()
        
        with StepTimer("Data Formatting"):
            map_stats = [dict(r) for r in map_stats]
            daily_traffic = [dict(r) for r in daily_traffic]
            player_rows = [dict(r) for r in player_rows]
            chart_24h = [dict(r) for r in chart_24h]
            chart_30d = [dict(r) for r in chart_30d]
            chart_history = [dict(r) for r in chart_history]
        
        cache.set('stats_page', (map_stats, daily_traffic, server_stats, player_rows, chart_24h, chart_30d, chart_history))

    with StepTimer("Render Template"):
        return render_template('stats.html', 
                               maps=map_stats, 
                               traffic=daily_traffic, 
                               top_servers=server_stats,
                               top_players=player_rows,
                               c_24h=chart_24h,
                               c_30d=chart_30d,
                               c_hist=chart_history)

