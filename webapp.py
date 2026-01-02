import os
import sqlite3
import ipaddress
import math
import time
import json
import threading
import logging
from functools import lru_cache
from flask import Flask, render_template, g, request

app = Flask(__name__)

# POINT THIS TO YOUR STAR SCHEMA DB
DB_FILE = r"C:\apps\Webapp\kf2_panopticon_v3_star.db"
PER_PAGE = 50

# Set a Secret Key for Sessions
app.secret_key = os.environ.get('SECRET_KEY', 'CHasdfasdfasdfaw4ezxcvgxhccycxvgSH_12345') 

# --- LOGGING SETUP ---
perf_logger = logging.getLogger('performance')
perf_logger.setLevel(logging.INFO)

# Hardcoded log path
log_file_path = r"C:\apps\Webapp\logs\performance_debug.jsonl"

# Ensure the logs directory exists
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

file_handler = logging.FileHandler(log_file_path)
perf_logger.addHandler(file_handler)

# --- PERFORMANCE MONITORING TOOLS ---
class StepTimer:
    """Context manager to measure execution time of a block."""
    def __init__(self, step_name):
        self.step_name = step_name

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = time.time()
        duration_ms = (end_time - self.start_time) * 1000
        if not hasattr(g, 'perf_steps'):
            g.perf_steps = []
        g.perf_steps.append({
            "step": self.step_name,
            "duration_ms": round(duration_ms, 2)
        })

def write_log_background(log_entry):
    """Writes the log in a thread to prevent blocking the response."""
    perf_logger.info(json.dumps(log_entry))

@app.before_request
def start_request_timer():
    g.request_start_time = time.time()
    g.perf_steps = []

@app.teardown_request
def log_performance(exception=None):
    """Logs the performance data after the request context is torn down."""
    if hasattr(g, 'request_start_time'):
        total_duration = (time.time() - g.request_start_time) * 1000
        
        log_entry = {
            "timestamp": time.time(),
            "endpoint": request.endpoint,
            "method": request.method,
            "total_duration_ms": round(total_duration, 2),
            "breakdown": getattr(g, 'perf_steps', [])
        }
        
        threading.Thread(target=write_log_background, args=(log_entry,)).start()

# --- MEMORY CACHE SYSTEM ---
class DataCache:
    def __init__(self):
        self.store = {}
        self.ttl = 300  # 5 Minutes Cache Duration

    def get(self, key):
        if key in self.store:
            data, timestamp = self.store[key]
            if time.time() - timestamp < self.ttl:
                return data
        return None

    def set(self, key, data):
        self.store[key] = (data, time.time())

# Initialize Global Cache
cache = DataCache()

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DB_FILE)
        db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

# --- Helpers ---
def get_pagination(count, page, per_page):
    return {
        "total": count,
        "pages": math.ceil(count / per_page),
        "current": page,
        "has_next": page < math.ceil(count / per_page),
        "has_prev": page > 1,
        "next_num": page + 1,
        "prev_num": page - 1
    }

def parse_location(loc_str):
    """
    Parses the cached 'City, Country' string from the DB.
    Zero DB hits. Pure string manipulation. Fast.
    """
    if not loc_str or loc_str == 'Unknown':
        return {"flag": "unknown", "city": "Unknown"}
    
    parts = loc_str.split(',')
    if len(parts) >= 2:
        return {"city": parts[0].strip(), "flag": parts[1].strip().lower()}
    elif len(parts) == 1:
        # Sometimes it might just be the country code or name
        return {"city": "Unknown", "flag": parts[0].strip().lower()}
    
    return {"flag": "unknown", "city": "Unknown"}

@app.template_filter('human_time')
def format_duration(seconds):
    if not seconds: return "0m"
    try:
        seconds = int(seconds)
    except ValueError: return "0m"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{h}h {m}m"
    return f"{m}m"

def get_global_stats(cur):
    with StepTimer("Global Stats Query"):
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
        except sqlite3.OperationalError:
            return {'players': 0, 'active_servers': 0, 'total_servers': 0, 'occupancy': 0}

# --- ROUTES ---
@app.route('/')
def servers():
    cur = get_db().cursor()
    stats = get_global_stats(cur)
    
    # --- UNRESTRICTED VIEW ---
    target_faction = request.args.get('faction')
    
    # Updated to fetch 'location' directly
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
                
            # FAST PARSE - NO DB LOOKUP
            geo = parse_location(s.get('location')) 
            s['flag'] = geo['flag']
            s['city'] = geo['city']
            servers_list.append(s)

    with StepTimer("Render Template"):
        return render_template('servers.html', servers=servers_list, stats=stats, current_faction=target_faction)
    
@app.route('/factions')
def factions():
    db = get_db()
    stats = get_global_stats(db.cursor())

    # --- CACHED SECTION START ---
    with StepTimer("Check Cache"):
        cached_data = cache.get('factions_page')
    
    if cached_data:
        live_top_5, month_rows, all_time_rows, chart_data = cached_data
        with StepTimer("Cache Hit Processing"):
            pass 
    else:
        # 1. LIVE TOP 5 (Fast)
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

        # 2. LAST 30 DAYS (Heavy) - UPDATED to use calculated_duration
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

        # 3. ALL TIME (Very Heavy) - UPDATED to use calculated_duration
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
            # Convert Row objects to dicts so they can be cached safely
            live_top_5 = [dict(r) for r in live_top_5]
            month_rows = [dict(r) for r in month_rows]
            all_time_rows = [dict(r) for r in all_time_rows]

            # Prepare Chart Data
            top_10_month = month_rows[:10]
            chart_data = {
                'labels': [r['operator_name'] for r in top_10_month],
                'players': [r['unique_players'] for r in top_10_month],
                'hours': [round(r['total_playtime_seconds'] / 3600) if r['total_playtime_seconds'] else 0 for r in top_10_month]
            }
        
        cache.set('factions_page', (live_top_5, month_rows, all_time_rows, chart_data))
    # --- CACHED SECTION END ---

    with StepTimer("Render Template"):
        return render_template('factions.html', 
                               stats=stats,
                               live_top_5=live_top_5,
                               month_data=month_rows,
                               all_time_data=all_time_rows,
                               chart_data=chart_data)
                            
@app.route('/players')
def players():
    cur = get_db().cursor()
    stats = get_global_stats(cur)
    
    with StepTimer("Players List Query"):
        # Optimization: Limit to top 100 active players to prevent massive load
        # UPDATED: Use fa.calculated_duration and ds.location
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
                
            # FAST PARSE - NO DB LOOKUP
            geo = parse_location(p.get('location'))
            p['flag'] = geo['flag']
            p['city'] = geo['city']
            players_data.append(p)

    with StepTimer("Render Template"):
        return render_template('index.html', stats=stats, players=players_data)

# --- HELPER FUNCTION ---
def get_match_history(db, server_id, page, per_page):
    with StepTimer("Match History Query"):
        offset = (page - 1) * per_page
        total_sessions_row = db.execute("SELECT COUNT(DISTINCT session_uuid) FROM fact_history WHERE server_id = ? AND session_uuid IS NOT NULL", (server_id,)).fetchone()
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
        if not session_rows: return matches, 0

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
            if uid not in roster_map: roster_map[uid] = []
            roster_map[uid].append(dict(r))
            
        for s in session_rows:
            match = dict(s)
            match['roster'] = roster_map.get(s['session_uuid'], [])
            matches.append(match)
            
        return matches, total_sessions

# --- ROUTES ---
@app.route('/server/<int:server_id>')
def server_detail(server_id):
    db = get_db()
    page = request.args.get('page', 1, type=int)
    
    with StepTimer("Server Info Query"):
        # Updated to fetch location
        server = db.execute("SELECT * FROM dim_servers WHERE id = ?", (server_id,)).fetchone()
        if not server: return "Server not found.", 404
        s_dict = dict(server)

        if s_dict['game_port'] and s_dict['game_port'] > 0:
            s_dict['display_addr'] = f"{s_dict['ip_address']}:{s_dict['game_port']}"
        else:
            s_dict['display_addr'] = f"{s_dict['ip_address']}:{s_dict['query_port']}"
            
        # Parse location for the template
        geo = parse_location(s_dict.get('location'))
        s_dict['flag'] = geo['flag']
        s_dict['city'] = geo['city']

    with StepTimer("Active Players Query"):
        # UPDATED: Use calculated_duration
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
        # UPDATED: Use calculated_duration from fact_server_history
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

@app.route('/player/<int:player_id>')
def player_detail(player_id):
    db = get_db()
    page = request.args.get('page', 1, type=int)
    offset = (page - 1) * PER_PAGE

    with StepTimer("Player Info Query"):
        player = db.execute("SELECT * FROM dim_players WHERE id = ?", (player_id,)).fetchone()
        if not player: return "Player not found.", 404

        count = db.execute("SELECT COUNT(*) FROM fact_history WHERE player_id = ?", (player_id,)).fetchone()[0]
    
    with StepTimer("Player History Query"):
        # Updated to fetch location
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
        
        # Parse location
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
        # UPDATED: Use h.calculated_duration
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

@app.route('/search')
def global_search():
    with StepTimer("Search Execution"):
        q = request.args.get('q', '').strip()
        if not q or len(q) < 2:
            return render_template('search_results.html', query=q, players=[], servers=[])

        db = get_db()
        wildcard_q = f"%{q}%"

        players = db.execute("""
            SELECT id, name FROM dim_players WHERE name LIKE ? ORDER BY length(name) ASC LIMIT 50
        """, (wildcard_q,)).fetchall()

        # Updated to fetch location
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
            
            # Parse location
            geo = parse_location(s.get('location'))
            s['flag'] = geo['flag']
            s['city'] = geo['city']
            servers.append(s)

        return render_template('search_results.html', query=q, players=players, servers=servers)

@app.route('/stats')
def statistics():
    # --- CACHED SECTION START ---
    with StepTimer("Check Cache"):
        cached_stats = cache.get('stats_page')
    
    if cached_stats:
        map_stats, daily_traffic, server_stats, player_rows, chart_24h, chart_30d, chart_history = cached_stats
        with StepTimer("Cache Hit Processing"):
            pass
    else:
        cur = get_db().cursor()
        
        with StepTimer("Query: Map Stats"):
            # UPDATED: Use calculated_duration from fact_server_history
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
            # UPDATED: Use h.calculated_duration
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
            # UPDATED: Use h.calculated_duration
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
            # Convert all Rows to dicts
            map_stats = [dict(r) for r in map_stats]
            daily_traffic = [dict(r) for r in daily_traffic]
            player_rows = [dict(r) for r in player_rows]
            chart_24h = [dict(r) for r in chart_24h]
            chart_30d = [dict(r) for r in chart_30d]
            chart_history = [dict(r) for r in chart_history]
        
        cache.set('stats_page', (map_stats, daily_traffic, server_stats, player_rows, chart_24h, chart_30d, chart_history))
    # --- CACHED SECTION END ---

    with StepTimer("Render Template"):
        return render_template('stats.html', 
                               maps=map_stats, 
                               traffic=daily_traffic, 
                               top_servers=server_stats,
                               top_players=player_rows,
                               c_24h=chart_24h,
                               c_30d=chart_30d,
                               c_hist=chart_history)

if __name__ == '__main__':
    app.run(debug=False, port=9001)