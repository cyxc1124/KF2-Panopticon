"""玩家路由蓝图"""
from flask import Blueprint, render_template, request
from app.services.db_service import get_db_connection, get_global_stats
from app.utils import StepTimer, parse_location, get_pagination

players_bp = Blueprint('players', __name__)

PER_PAGE = 50

@players_bp.route('/players')
def players():
    """玩家列表页"""
    db = get_db_connection()
    cur = db.cursor()
    stats = get_global_stats(cur)
    
    with StepTimer("Players List Query"):
        players_rows = cur.execute("""
            SELECT 
                dp.id as player_id,
                dp.name as player_name, 
                fa.score, 
                fa.calculated_duration as duration,
                fa.last_seen, 
                ds.id as server_id,
                ds.name as server_name, 
                ds.ip_address,
                ds.game_port,
                ds.query_port,
                ds.location,
                dm.name as map
            FROM fact_active fa
            JOIN dim_players dp ON fa.player_id = dp.id
            JOIN dim_servers ds ON fa.server_id = ds.id
            LEFT JOIN dim_maps dm ON fa.map_id = dm.id
            ORDER BY fa.score DESC
        """).fetchall()

    with StepTimer("Geo Resolution Loop"):
        players_data = []
        for row in players_rows:
            p = dict(row)
            if p['game_port'] and p['game_port'] > 0:
                p['address'] = f"{p['ip_address']}:{p['game_port']}"
            else:
                p['address'] = f"{p['ip_address']}:{p['query_port']}"
                
            geo = parse_location(p.get('location'))
            p['flag'] = geo['flag']
            p['city'] = geo['city']
            players_data.append(p)

    with StepTimer("Render Template"):
        return render_template('index.html', stats=stats, players=players_data)


@players_bp.route('/player/<int:player_id>')
def player_detail(player_id):
    """玩家详情页"""
    db = get_db_connection()
    page = request.args.get('page', 1, type=int)
    offset = (page - 1) * PER_PAGE

    with StepTimer("Player Info Query"):
        player = db.execute("SELECT * FROM dim_players WHERE id = ?", (player_id,)).fetchone()
        if not player:
            return "Player not found.", 404

        count = db.execute("SELECT COUNT(*) FROM fact_history WHERE player_id = ?", (player_id,)).fetchone()[0]
    
    with StepTimer("Player History Query"):
        history_rows = db.execute("""
            SELECT 
                s.id as server_id, 
                s.name as server_name, 
                h.session_start, 
                h.total_time, 
                h.final_score, 
                s.ip_address,
                s.game_port,
                s.query_port,
                s.location
            FROM fact_history h
            JOIN dim_servers s ON h.server_id = s.id
            WHERE h.player_id = ?
            ORDER BY h.session_start DESC
            LIMIT ? OFFSET ?
        """, (player_id, PER_PAGE, offset)).fetchall()

    history = []
    for row in history_rows:
        h = dict(row)
        if h['game_port'] and h['game_port'] > 0:
            h['address'] = f"{h['ip_address']}:{h['game_port']}"
        else:
            h['address'] = f"{h['ip_address']}:{h['query_port']}"
        
        geo = parse_location(h.get('location'))
        h['flag'] = geo['flag']
        h['city'] = geo['city']
        history.append(h)

    with StepTimer("Teammates Query"):
        teammates = db.execute("""
            SELECT 
                p2.id,
                p2.name,
                COUNT(DISTINCT h1.session_uuid) as matches_together,
                SUM(h2.total_time) as total_time_together
            FROM fact_history h1
            JOIN fact_history h2 ON h1.session_uuid = h2.session_uuid
            JOIN dim_players p2 ON h2.player_id = p2.id
            WHERE h1.player_id = ?      
              AND h2.player_id != ?     
              AND h1.session_uuid IS NOT NULL
            GROUP BY p2.id
            ORDER BY matches_together DESC
            LIMIT 20
        """, (player_id, player_id)).fetchall()

    with StepTimer("Allegiances Query"):
        allegiances = db.execute("""
            SELECT 
                s.operator_name,
                COUNT(h.id) as sessions_played,
                SUM(h.calculated_duration) as time_played
            FROM fact_history h
            JOIN dim_servers s ON h.server_id = s.id
            WHERE h.player_id = ? 
              AND s.operator_name IS NOT NULL 
              AND s.operator_name != 'Unknown'
            GROUP BY s.operator_name
            ORDER BY sessions_played DESC
        """, (player_id,)).fetchall()

    pagination = get_pagination(count, page, PER_PAGE)
    
    with StepTimer("Render Template"):
        return render_template('player_detail.html', 
                               player=player, 
                               history=history, 
                               teammates=teammates,
                               allegiances=allegiances,
                               pagination=pagination)
