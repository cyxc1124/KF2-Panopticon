"""派系路由蓝图"""
from flask import Blueprint, render_template
from app.services.db_service import get_db_connection, get_global_stats
from app.utils import StepTimer
from app import cache

factions_bp = Blueprint('factions', __name__)

@factions_bp.route('/factions')
def factions():
    """派系统计页"""
    db = get_db_connection()
    stats = get_global_stats(db.cursor())

    with StepTimer("Check Cache"):
        cached_data = cache.get('factions_page')
    
    if cached_data:
        live_top_5, month_rows, all_time_rows, chart_data = cached_data
        with StepTimer("Cache Hit Processing"):
            pass 
    else:
        with StepTimer("Query: Live Top 5"):
            live_top_5 = db.execute("""
                SELECT 
                    operator_name,
                    SUM(player_count) as current_players,
                    COUNT(id) as active_servers
                FROM dim_servers
                WHERE operator_name IS NOT NULL 
                  AND operator_name != 'Unknown' 
                  AND player_count > 0
                GROUP BY operator_name
                ORDER BY current_players DESC
                LIMIT 6
            """).fetchall()

        with StepTimer("Query: Last 30 Days (HEAVY)"):
            month_rows = db.execute("""
                SELECT
                    operator_name,
                    SUM(server_count) AS server_count,
                    SUM(unique_players) AS unique_players,
                    SUM(total_playtime_seconds) AS total_playtime_seconds,
                    MAX(last_contact) AS last_contact
                FROM fact_operator_daily
                WHERE day >= date('now', '-30 days')
                GROUP BY operator_name
                ORDER BY unique_players DESC;
            """).fetchall()

        with StepTimer("Query: All Time (HEAVY)"):
            all_time_rows = db.execute("""
                SELECT
                    operator_name,
                    SUM(unique_players) AS unique_players,
                    SUM(total_playtime_seconds) AS total_playtime_seconds
                FROM fact_operator_daily
                GROUP BY operator_name
                ORDER BY total_playtime_seconds DESC
                LIMIT 50;
            """).fetchall()

        with StepTimer("Data Processing"):
            live_top_5 = [dict(r) for r in live_top_5]
            month_rows = [dict(r) for r in month_rows]
            all_time_rows = [dict(r) for r in all_time_rows]

            top_10_month = month_rows[:10]
            chart_data = {
                'labels': [r['operator_name'] for r in top_10_month],
                'players': [r['unique_players'] for r in top_10_month],
                'hours': [round(r['total_playtime_seconds'] / 3600) if r['total_playtime_seconds'] else 0 for r in top_10_month]
            }
        
        cache.set('factions_page', (live_top_5, month_rows, all_time_rows, chart_data))

    with StepTimer("Render Template"):
        return render_template('factions.html', 
                               stats=stats,
                               live_top_5=live_top_5,
                               month_data=month_rows,
                               all_time_data=all_time_rows,
                               chart_data=chart_data)

