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
            cur.execute("""
                SELECT
                    m.name AS map,
                    SUM(d.session_count) AS session_count,
                    SUM(d.total_seconds) AS total_seconds
                FROM fact_map_daily d
                JOIN dim_maps m ON d.map_id = m.id
                WHERE d.day >= (CURRENT_DATE - INTERVAL '30 days')::DATE
                GROUP BY d.map_id, m.name
                ORDER BY total_seconds DESC
                LIMIT 10
            """)
            map_stats = cur.fetchall()

        with StepTimer("Query: Daily Traffic"):
            cur.execute("""
                SELECT day, unique_players
                FROM fact_traffic_daily
                WHERE day >= (CURRENT_DATE - INTERVAL '30 days')::DATE
                ORDER BY day ASC
            """)
            daily_traffic = cur.fetchall()

        with StepTimer("Query: Top Servers"):
            cur.execute("""
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
                WHERE d.day >= (CURRENT_DATE - INTERVAL '30 days')::DATE
                GROUP BY d.server_id, s.id, s.name, s.ip_address, s.game_port, s.query_port
                ORDER BY total_seconds DESC
                LIMIT 10
            """)
            server_rows = cur.fetchall()

            server_stats = []
            for row in server_rows:
                d = {**row}
                if d['game_port'] and d['game_port'] > 0:
                    d['address'] = f"{d['ip_address']}:{d['game_port']}"
                else:
                    d['address'] = f"{d['ip_address']}:{d['query_port']}"
                server_stats.append(d)

        with StepTimer("Query: Top Players"):
            cur.execute("""
                SELECT
                    p.id,
                    p.name,
                    SUM(d.session_count) AS session_count,
                    SUM(d.total_seconds) AS total_seconds
                FROM fact_player_daily d
                JOIN dim_players p ON d.player_id = p.id
                WHERE d.day >= (CURRENT_DATE - INTERVAL '30 days')::DATE
                GROUP BY d.player_id, p.id, p.name
                ORDER BY total_seconds DESC
                LIMIT 10
            """)
            player_rows = cur.fetchall()

        with StepTimer("Query: Chart 24h"):
            cur.execute("""
                SELECT scan_time, active_players, active_servers
                FROM fact_global_stats
                WHERE scan_time > NOW() - INTERVAL '24 hours'
                ORDER BY scan_time ASC
            """)
            chart_24h = cur.fetchall()

        with StepTimer("Query: Chart 30d"):
            cur.execute("""
                SELECT 
                    TO_TIMESTAMP(FLOOR(EXTRACT(EPOCH FROM scan_time) / 14400) * 14400) as time_bucket,
                    ROUND(AVG(active_players), 1) as avg_players,
                    ROUND(AVG(active_servers), 1) as avg_servers
                FROM fact_global_stats
                WHERE scan_time > NOW() - INTERVAL '30 days'
                GROUP BY time_bucket
                ORDER BY time_bucket ASC
            """)
            chart_30d = cur.fetchall()

        with StepTimer("Query: Chart History"):
            cur.execute("""
                SELECT 
                    scan_time::DATE as day,
                    ROUND(AVG(active_players), 1) as avg_players,
                    MAX(active_players) as max_players
                FROM fact_global_stats
                GROUP BY day
                ORDER BY day ASC
            """)
            chart_history = cur.fetchall()
        
        with StepTimer("Data Formatting"):
            map_stats = [{**r} for r in map_stats]
            daily_traffic = [{**r} for r in daily_traffic]
            player_rows = [{**r} for r in player_rows]
            chart_24h = [{**r} for r in chart_24h]
            chart_30d = [{**r} for r in chart_30d]
            chart_history = [{**r} for r in chart_history]
        
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

