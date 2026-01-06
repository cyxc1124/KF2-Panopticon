"""主路由 - 首页、搜索"""
from flask import Blueprint, render_template, request
from app.services.db_service import get_db_connection, get_global_stats
from app.utils import StepTimer, parse_location

main_bp = Blueprint('main', __name__)


@main_bp.route('/')
def index():
    """首页 - 服务器列表"""
    db = get_db_connection()
    cur = db.cursor()
    stats = get_global_stats(cur)
    
    # 获取派系过滤参数
    target_faction = request.args.get('faction')
    
    # 构建查询
    base_query = """
        SELECT 
            s.id, s.ip_address, s.query_port, s.game_port, s.name, 
            s.player_count, s.last_seen, s.operator_name, s.location,
            m.name as map
        FROM dim_servers s
        LEFT JOIN dim_maps m ON s.current_map_id = m.id
    """
    
    params = []
    if target_faction:
        base_query += " WHERE s.operator_name = ?"
        params.append(target_faction)
        
    base_query += " ORDER BY s.player_count DESC"
    
    with StepTimer("Servers List Query"):
        server_rows = cur.execute(base_query, params).fetchall()
    
    with StepTimer("Process/Geo Resolution"):
        servers_list = []
        for row in server_rows:
            s = dict(row)
            if s['game_port'] and s['game_port'] > 0:
                s['display_addr'] = f"{s['ip_address']}:{s['game_port']}"
                s['is_fallback'] = False
            else:
                s['display_addr'] = f"{s['ip_address']}:{s['query_port']}"
                s['is_fallback'] = True
                
            geo = parse_location(s.get('location')) 
            s['flag'] = geo['flag']
            s['city'] = geo['city']
            servers_list.append(s)

    with StepTimer("Render Template"):
        return render_template('servers.html', servers=servers_list, stats=stats, current_faction=target_faction)


@main_bp.route('/search')
def search():
    """全局搜索"""
    with StepTimer("Search Execution"):
        q = request.args.get('q', '').strip()
        if not q or len(q) < 2:
            return render_template('search_results.html', query=q, players=[], servers=[])

        db = get_db_connection()
        wildcard_q = f"%{q}%"

        players = db.execute("""
            SELECT id, name FROM dim_players WHERE name LIKE ? ORDER BY length(name) ASC LIMIT 50
        """, (wildcard_q,)).fetchall()

        server_rows = db.execute("""
            SELECT id, name, ip_address, game_port, query_port, last_seen, location
            FROM dim_servers 
            WHERE name LIKE ? OR (ip_address || ':' || game_port) LIKE ? 
            ORDER BY last_seen DESC LIMIT 50
        """, (wildcard_q, wildcard_q)).fetchall()

        servers = []
        for row in server_rows:
            s = dict(row)
            if s['game_port'] and s['game_port'] > 0:
                s['address'] = f"{s['ip_address']}:{s['game_port']}"
            else:
                s['address'] = f"{s['ip_address']}:{s['query_port']}"
            
            geo = parse_location(s.get('location'))
            s['flag'] = geo['flag']
            s['city'] = geo['city']
            servers.append(s)

        return render_template('search_results.html', query=q, players=players, servers=servers)

