"""服务器路由蓝图"""
from flask import Blueprint, render_template, request
from app.services.db_service import get_db_connection
from app.utils import StepTimer, parse_location, get_pagination

servers_bp = Blueprint('servers', __name__)


def get_match_history(db, server_id, page, per_page):
    """获取服务器比赛历史"""
    with StepTimer("Match History Query"):
        offset = (page - 1) * per_page
        total_sessions_row = db.execute(
            "SELECT COUNT(DISTINCT session_uuid) FROM fact_history WHERE server_id = ? AND session_uuid IS NOT NULL",
            (server_id,)
        ).fetchone()
        total_sessions = total_sessions_row[0] if total_sessions_row else 0
        
        session_rows = db.execute("""
            SELECT 
                h.session_uuid,
                m.name as map_name,
                MIN(h.session_start) as start_time,
                MAX(h.session_end) as end_time,
                (strftime('%s', MAX(h.session_end)) - strftime('%s', MIN(h.session_start))) as match_duration,
                COUNT(DISTINCT h.player_id) as player_count,
                SUM(h.final_score) as total_match_score
            FROM fact_history h
            JOIN dim_maps m ON h.map_id = m.id
            WHERE h.server_id = ? AND h.session_uuid IS NOT NULL
            GROUP BY h.session_uuid
            ORDER BY start_time DESC
            LIMIT ? OFFSET ?
        """, (server_id, per_page, offset)).fetchall()
        
        matches = []
        if not session_rows:
            return matches, 0

        uuids = [row['session_uuid'] for row in session_rows]
        placeholders = ','.join(['?'] * len(uuids))
        
        roster_rows = db.execute(f"""
            SELECT h.session_uuid, p.id as player_id, p.name, h.final_score, h.total_time
            FROM fact_history h
            JOIN dim_players p ON h.player_id = p.id
            WHERE h.session_uuid IN ({placeholders})
            ORDER BY h.final_score DESC
        """, uuids).fetchall()
        
        roster_map = {}
        for r in roster_rows:
            uid = r['session_uuid']
            if uid not in roster_map:
                roster_map[uid] = []
            roster_map[uid].append(dict(r))
            
        for s in session_rows:
            match = dict(s)
            match['roster'] = roster_map.get(s['session_uuid'], [])
            matches.append(match)
            
        return matches, total_sessions


@servers_bp.route('/server/<int:server_id>')
def server_detail(server_id):
    """服务器详情页"""
    db = get_db_connection()
    page = request.args.get('page', 1, type=int)
    
    with StepTimer("Server Info Query"):
        server = db.execute("SELECT * FROM dim_servers WHERE id = ?", (server_id,)).fetchone()
        if not server:
            return "Server not found.", 404
        s_dict = dict(server)

        if s_dict['game_port'] and s_dict['game_port'] > 0:
            s_dict['display_addr'] = f"{s_dict['ip_address']}:{s_dict['game_port']}"
        else:
            s_dict['display_addr'] = f"{s_dict['ip_address']}:{s_dict['query_port']}"
            
        geo = parse_location(s_dict.get('location'))
        s_dict['flag'] = geo['flag']
        s_dict['city'] = geo['city']

    with StepTimer("Active Players Query"):
        active_players = db.execute("""
            SELECT p.id as player_id, p.name, a.score, a.calculated_duration as duration, a.first_seen
            FROM fact_active a
            JOIN dim_players p ON a.player_id = p.id
            WHERE a.server_id = ?
            ORDER BY a.score DESC
        """, (server_id,)).fetchall()

    matches, total_count = get_match_history(db, server_id, page, 15)
    pagination = get_pagination(total_count, page, 15)

    with StepTimer("Map Stats Query"):
        map_rows = db.execute("""
            SELECT m.name, COUNT(h.id) as count
            FROM fact_server_history h
            JOIN dim_maps m ON h.map_id = m.id
            WHERE h.server_id = ?
            GROUP BY m.name
            ORDER BY count DESC
            LIMIT 5
        """, (server_id,)).fetchall()
    
    chart_map_labels = [row['name'] for row in map_rows]
    chart_map_data = [row['count'] for row in map_rows]

    with StepTimer("Traffic Stats Query"):
        traffic_rows = db.execute("""
            SELECT strftime('%H', session_start) as hour, COUNT(*) as count
            FROM fact_history
            WHERE server_id = ? AND session_start > date('now', '-30 days')
            GROUP BY hour
            ORDER BY hour ASC
        """, (server_id,)).fetchall()
        
        traffic_dict = {int(row['hour']): row['count'] for row in traffic_rows}
        chart_traffic_data = [traffic_dict.get(h, 0) for h in range(24)]

    with StepTimer("Render Template"):
        return render_template('server_detail.html', 
                               server=s_dict, 
                               active_players=active_players, 
                               matches=matches,
                               pagination=pagination,
                               chart_map_labels=chart_map_labels,
                               chart_map_data=chart_map_data,
                               chart_traffic_data=chart_traffic_data)

