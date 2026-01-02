import requests
import socket
import struct
import sqlite3
import concurrent.futures
import time
import uuid
import re 
import ipaddress # <--- Added for IP Math
from datetime import datetime, timedelta

# --- Configuration ---
STEAM_KEY = ""
APP_ID = 232090
API_URL = f"https://api.steampowered.com/IGameServersService/GetServerList/v1/?key={STEAM_KEY}&limit=50000&filter=\\appid\\{APP_ID}"
DB_FILE = r"C:\apps\Webapp\kf2_panopticon_v3_star.db"

# The Narcissus Shim
LOCAL_LOOPBACK_IP = "127.0.0.1" 

A2S_INFO = b"\xff\xff\xff\xff\x54\x53\x6f\x75\x72\x63\x65\x20\x45\x6e\x67\x69\x6e\x65\x20\x51\x75\x65\x72\x79\x00"
A2S_PLAYER_CHALLENGE = b"\xff\xff\xff\xff\x55\xff\xff\xff\xff"
A2S_PLAYER_HEADER = b"\xff\xff\xff\xff\x55"

MAX_WORKERS = 150
TIMEOUT = 3.0
PRUNE_THRESHOLD = 6

# --- Fix for Python 3.12+ Datetime warnings ---
def adapt_date_iso(val): return val.isoformat(sep=' ')
sqlite3.register_adapter(datetime, adapt_date_iso)
# -----------------------------------------------

# --- FACTION INTELLIGENCE MODULE ---
def get_fallback_country(raw_name):
    geo_pattern = r'\b(us|eu|cn|ru|de|au|uk|fr|jp|kr|tw|sg|br|es|th|vn|nl)\b'
    match = re.search(geo_pattern, raw_name, re.IGNORECASE)
    if match:
        return f"Unknown [{match.group(1).upper()}]"
    return "Unknown"

def extract_domain_name(raw_name):
    match = re.search(r'([a-zA-Z0-9-]{2,})\.(com|net|org|tk|ru|de|eu|gg|host|cloud|xyz|info)\b', raw_name.lower())
    if match:
        return match.group(1).title()
    return None

def clean_server_name(raw_name, ip_address):

    if not raw_name: return ip_address
    name = raw_name.lower()

    # --- 0. VIP LIST ---
    VIP_PATTERNS = {
        r'simpleserver': "SimpleServer (TH)",
        r'valeria': "Valeria & Friends",
        r'nekoha': "Nekoha Club",    
        r'\bbaz\b': "BAz",
        r'\bkf-?fr\b': "KF-FR",
        r'\bkf-?br\b': "KF-BR",
        r'jp\s?\|': "JP Server",
        r'\bsg-?servers?\b': "SG-Servers",
        r'\bhuwhyte\b': "Huwhyte",
        r'\btripwire\b': "Tripwire Official",
        r'\bbloodhounds\b': "Bloodhounds",
        r'\bkog\b': "KoG Clan",
        r'\bcyxc\b': "Cyxc",
        r'\bamursk\b': "Amursk",
        r'\bmadhouse\b': "MadHouse",
        r'\bamerica latina brasil\b': "America Latina Brasil",
        r'\blarge\s?farva\b': "Large Farva",
        r'\bpunchguts\b': "Punchguts",
        r'\bspb-?gs\b': "SPB-GS",
        r'\bextreme\s?server\b': "Extreme Server",
        r'\bpowerbits\b': "Powerbits",
        r'\bwilnet\b': "Wilnet Gaming",
        r'\bnerdit\b': "Nerdit",
        r'\bmod-?eu\b': "Mod-EU",
        r'\bnfo(?:servers)?\b': "NFO Servers",
        r'\bdslive\b': "DSLive",
        r'\bzgaming\b': "ZGaming",
        r'\[kr\]\s+public\s+server': "[KR] Public Server",
        r'\bkf2\.eu\b': "KF2.eu SuperPerkTraining",
        r'\btwilight realm\b': "Twilight Realm",
        r'\bthe alley\b': "The Alley",
        r'^cd\s?#\d+': "Legs CD",
        r'the\s?outpost': "The Outpost",
        r'sora-?iro': "Sora-Iro (JP)", 
        
        # --- ASIAN / SPECIAL CHARACTER FACTIONS ---
        r'뽀이뿨이\s?poi': "POI (Korea)",
        r'猛男妙妙屋': "Mengnan (CN)",
        r'烂番茄菜篮子': "Rotten Tomato (CN)",
        r'孤风娱乐': "Gufeng Entertainment",
        r'禁忌边境线': "Forbidden Borderline",
        r'医疗大小姐': "Medical Miss",
        r'土豆服务器': "Potato Server (CN)",
        r'ナツ': "Natsu",
        r'诗人\s?rpg': "Poet RPG",
        r'缅北腰花': "Myanmar Kidney Assoc",
        r'大布笑传': "Dabu Laughing",
        r'柚子': "Youzi",
        r'离离原上咪': "Lili Plain",
    }

    for pattern, faction in VIP_PATTERNS.items():
        if re.search(pattern, name):
            return faction

    # --- 0.5 DOMAIN RESCUE ---
    domain_faction = extract_domain_name(raw_name)
    if domain_faction:
        return domain_faction

    # --- 1. REMOVE WEB TRASH ---
    name = re.sub(r'https?://\S+|www\.\S+|discord\.gg/\S+', '', name)
    name = re.sub(r'\.(com|net|org|tk|ru|de|eu|gg|host|cloud|xyz|info)\b', '', name)
    name = re.sub(r'\bqq\d+\b', '', name)

    # --- 2. REMOVE UUIDs ---
    name = re.sub(r'#[a-f0-9-]{10,}', '', name)

    # --- 2.5 EARLY PIPE SPLIT (ENHANCED) ---
    # Normalize weird pipes to standard pipe
    name = name.replace('¦', '|').replace('｜', '|').replace('│', '|')
    if '|' in name:
        name = name.split('|')[0]

    # --- 3. THE KILL LIST ---
    KILL_PATTERNS = [
        r'\b(us|eu|cn|ru|de|au|uk|fr|jp|kr|tw|sg|br|es|th|vn|nl)\b',
        r'\b(east|west|north|south|central|global|international)\b',
        r'\b(dallas|seattle|miami|chicago|new\s?york|london|tokyo|santiago|montreal|sydney|paris|frankfurt|singapore|los\s?angeles)\b',
        r'\btakeover\b', r'\bstandby\b', r'\bidle\b', r'\bafk\b',
        r'\branked\b', r'\bunranked\b', r'\bwhitelist(?:ed)?\b', r'\bprivate\b',
        r'\bpassword(?:ed)?\b', r'\bpublic\b', r'\bdedicated\b', r'\bofficial\b',
        r'\bby\b', 
        r'\bendless\b', r'\bsurvival\b', r'\bobjective\b', r'\bholdout\b', r'\bversus\b',
        r'\bweekly\b', r'\boutbreak\b', r'\bwave\b', r'\bclassic\b',
        r'\bcd\b', r'\bcontrolled\s?difficulty\b', r'\bprecision\b', r'\bspam\b',
        r'\bzerg\s?mode\b', 
        r'\bhoe\+{0,4}\b', r'\bhell\s?on\s?earth\b', r'\bsuicidal\b', r'\bhard\b',
        r'\bnormal\b', r'\bbeginner\b', r'\bgod\s?mode\b', r'\bdifficulty\b',
        r'\bextreme\b', r'\binsane\b', r'\bvery\b',
        r'\btick(?:rate)?\b', r'\bhz\b', r'\bfps\b', r'\bm\.2\b', r'\bssd\b', r'\bnvme\b',
        r'\blow\s?ping\b', r'\bfast\s?dl\b', r'\bredirect\b', r'\blatency\b',
        r'\bslot\b', r'\bplayer\b', r'\b\d{1,3}p\b',
        r'\bcustom\b', r'\bmap(?:s)?\b', r'\bvanilla\b', r'\bworkshop\b',
        r'\brpg(?:mod)?\b', r'\bzedternal(?:reborn)?\b', r'\breborn\b',
        r'\bno\s?edars?\b', r'\bno\s?qps?\b', r'\bmax\s?spawn\b',
        r'\bweapon(?:s)?\b', r'\bzed(?:s)?\b', r'\bdlc\b', r'\bshared\b',
        r'\bperk(?:s)?\b', r'\blevel(?:s)?\b', r'\blvl\b', r'\bxp\b', r'\bprestige\b',
        r'\bdosh\b', r'\bvault\b', r'\bfriendly\s?fire\b', r'\bff\b',
        r'\brampage(?:mod)?\b', r'\band\s?more\b',
        r'\bkilling floor 2(?: server)?\b', r'\bkf2(?: server)?\b', r'\bserver\b',
        r'\blong\b', r'\bshort\b', r'\bmedium\b', r'\bauto\b', r'\breset\b', r'\bnew\b'
    ]
    
    name = re.sub("|".join(KILL_PATTERNS), ' ', name)

    # --- 4. CLEANUP ---
    name = re.sub(r'#\d+', ' ', name) 
    name = re.sub(r'\b\d+\b', ' ', name) 
    name = re.sub(r'[^\w\s]', ' ', name) 
    name = re.sub(r'\s+', ' ', name).strip()

    # --- 5. THE FAILSAFE ---
    if len(name) < 2:
        fallback = get_fallback_country(raw_name)
        if fallback != "Unknown":
            return fallback
        return f"{ip_address}"

    return name.title()

def resolve_geo_db(conn, ip_str):
    """Resolves IP to City, Code using DB ip_ranges."""
    try:
        ip_int = int(ipaddress.IPv4Address(ip_str))
        cur = conn.execute("""
            SELECT city_name, country_code
            FROM ip_ranges 
            WHERE ip_to >= ? 
            ORDER BY ip_to ASC 
            LIMIT 1
        """, (ip_int,))
        row = cur.fetchone()
        if row and row[0] and row[1]:
            return f"{row[0]}, {row[1]}"
        elif row and row[1]:
             return row[1]
    except:
        pass
    return "Unknown"
    
def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        
        # 1. SERVERS (Updated with operator_name AND location)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_servers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ip_address TEXT NOT NULL,
                query_port INTEGER NOT NULL,
                game_port INTEGER,
                name TEXT,
                current_map_id INTEGER,
                player_count INTEGER DEFAULT 0,
                map_start DATETIME,
                last_seen DATETIME,
                current_session_uuid TEXT,
                operator_name TEXT,
                location TEXT,  -- <--- Added Column
                UNIQUE(ip_address, query_port)
            )
        """)

        # 2. MAPS
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_maps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )
        """)

        # 3. PLAYERS
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )
        """)

        # 4. SERVER HISTORY (Updated with calculated_duration)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_server_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                server_id INTEGER,
                map_id INTEGER,
                session_start DATETIME,
                session_end DATETIME,
                reason TEXT,
                session_uuid TEXT,
                calculated_duration INTEGER DEFAULT 0, -- <--- Added Column
                FOREIGN KEY (server_id) REFERENCES dim_servers(id),
                FOREIGN KEY (map_id) REFERENCES dim_maps(id)
            )
        """)

        # 5. ACTIVE SESSIONS (Updated with calculated_duration)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_active (
                server_id INTEGER,
                player_id INTEGER,
                map_id INTEGER,
                score INTEGER,
                duration REAL,
                calculated_duration INTEGER DEFAULT 0, -- <--- Added Column
                first_seen DATETIME,
                last_seen DATETIME,
                session_uuid TEXT,
                PRIMARY KEY (server_id, player_id),
                FOREIGN KEY (server_id) REFERENCES dim_servers(id),
                FOREIGN KEY (player_id) REFERENCES dim_players(id),
                FOREIGN KEY (map_id) REFERENCES dim_maps(id)
            )
        """)

        # 6. SESSION HISTORY (Updated with calculated_duration)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                server_id INTEGER,
                player_id INTEGER,
                map_id INTEGER,
                final_score INTEGER,
                total_time REAL,
                session_start DATETIME,
                session_end DATETIME,
                session_uuid TEXT,
                calculated_duration INTEGER DEFAULT 0, -- <--- Added Column
                FOREIGN KEY (server_id) REFERENCES dim_servers(id),
                FOREIGN KEY (player_id) REFERENCES dim_players(id)
            )
        """)

        # 7. GLOBAL STATS
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_global_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_time DATETIME,
                active_servers INTEGER,
                active_players INTEGER
            )
        """)
        # --- ROLLUPS / MATERIALIZED STATS ---
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meta_kv (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_operator_daily (
                day DATE,
                operator_name TEXT,
                server_count INTEGER NOT NULL,
                unique_players INTEGER NOT NULL,
                total_playtime_seconds INTEGER NOT NULL,
                last_contact DATETIME,
                PRIMARY KEY (day, operator_name)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_operator_daily_day ON fact_operator_daily(day)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_operator_daily_operator ON fact_operator_daily(operator_name)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_map_daily (
                day DATE,
                map_id INTEGER,
                session_count INTEGER NOT NULL,
                total_seconds INTEGER NOT NULL,
                PRIMARY KEY (day, map_id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_map_daily_day ON fact_map_daily(day)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_server_daily (
                day DATE,
                server_id INTEGER,
                session_count INTEGER NOT NULL,
                total_seconds INTEGER NOT NULL,
                PRIMARY KEY (day, server_id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_server_daily_day ON fact_server_daily(day)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_player_daily (
                day DATE,
                player_id INTEGER,
                session_count INTEGER NOT NULL,
                total_seconds INTEGER NOT NULL,
                PRIMARY KEY (day, player_id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_player_daily_day ON fact_player_daily(day)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_traffic_daily (
                day DATE PRIMARY KEY,
                unique_players INTEGER NOT NULL
            )
        """)
        
        conn.commit()

# --- Parsing ---
def read_string(data, pos):
    try:
        end = data.find(b'\x00', pos)
        if end == -1: return "", pos
        return data[pos:end].decode('utf-8', errors='ignore'), end + 1
    except: return "Unknown", pos + 1

def parse_iso_time(time_str):
    try:
        if not time_str: return datetime.utcnow() # <--- CHANGED: .now() to .utcnow()
        return datetime.fromisoformat(time_str)
    except ValueError: return datetime.utcnow()   # <--- CHANGED: .now() to .utcnow()

def get_public_ip():
    try:
        return requests.get('https://ifconfig.me/ip', timeout=5).text.strip()
    except: return None

def query_server(server_addr):
    try:
        ip, query_port = server_addr.split(':')
        addr = (ip, int(query_port))
        query_port = int(query_port)
    except: return None
    
    game_port = None # Will try to discover real port
    
    results = {
        "addr": server_addr, 
        "name": None, 
        "map": "", 
        "player_list": [],
        "header_count": 0,
        "query_port": query_port,
        "game_port": game_port
    }
    
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(TIMEOUT)
        try:
            # 1. A2S_INFO
            sock.sendto(A2S_INFO, addr)
            resp = sock.recv(4096)
            
            if resp.startswith(b'\xff\xff\xff\xff\x41'):
                sock.sendto(A2S_INFO + resp[5:], addr)
                resp = sock.recv(4096)
                
            if resp.startswith(b'\xff\xff\xff\xff\x49'): 
                name, pos = read_string(resp, 6)
                map_name, pos = read_string(resp, pos)
                folder, pos = read_string(resp, pos)
                game, pos = read_string(resp, pos)
                
                pos += 2 # Skip ID
                if pos < len(resp):
                    results["header_count"] = resp[pos]

                pos += 1 
                if pos < len(resp):
                    edf = resp[pos]
                    pos += 1
                    if edf & 0x80:
                        if pos + 2 <= len(resp):
                            game_port = struct.unpack('<H', resp[pos:pos+2])[0]
                            results["game_port"] = game_port
                
                results["name"] = name
                results["map"] = map_name
            else: return None

            # 2. A2S_PLAYERS
            sock.sendto(A2S_PLAYER_CHALLENGE, addr)
            resp = sock.recv(4096)
            if resp.startswith(b'\xff\xff\xff\xff\x41'):
                sock.sendto(A2S_PLAYER_HEADER + resp[5:], addr)
                resp = sock.recv(4096)
            
            if resp.startswith(b'\xff\xff\xff\xff\x44'):
                num = resp[5]
                pos = 6
                slot = 0
                for _ in range(num):
                    if pos >= len(resp): break
                    pos += 1 # Skip Index
                    
                    p_name, pos = read_string(resp, pos)
                    
                    if pos + 8 > len(resp): break
                    score, dur = struct.unpack('<if', resp[pos:pos+8])
                    pos += 8
                    
                    # Handle Ghost Players
                    clean = p_name.strip() if p_name else ""
                    if not clean:
                        clean = f"[UNNAMED:{ip}:{query_port}:{slot}]"
                    
                    results["player_list"].append({"name":clean,"score":score,"dur":dur})
                    slot += 1
        except: pass
        
    return results if results["name"] else None
def _kv_get(conn, key):
    row = conn.execute("SELECT value FROM meta_kv WHERE key = ?", (key,)).fetchone()
    return row[0] if row else None

def _kv_set(conn, key, value):
    conn.execute("""
        INSERT INTO meta_kv (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """, (key, value))

def backfill_rollups(conn):
    """
    One-time backfill over all history.
    This can take a while depending on fact_history size, but you only do it once.
    """
    done = _kv_get(conn, "rollups_backfilled")
    if done == "1":
        return

    # Operator daily (factions)
    conn.execute("DELETE FROM fact_operator_daily")
    conn.execute("""
        INSERT INTO fact_operator_daily (day, operator_name, server_count, unique_players, total_playtime_seconds, last_contact)
        SELECT
            date(h.session_start) AS day,
            s.operator_name,
            COUNT(DISTINCT h.server_id) AS server_count,
            COUNT(DISTINCT h.player_id) AS unique_players,
            COALESCE(SUM(h.calculated_duration), 0) AS total_playtime_seconds,
            MAX(h.session_end) AS last_contact
        FROM fact_history h
        JOIN dim_servers s ON h.server_id = s.id
        WHERE s.operator_name IS NOT NULL
          AND s.operator_name != 'Unknown'
        GROUP BY day, s.operator_name
    """)

    # Map daily (stats) from fact_server_history
    conn.execute("DELETE FROM fact_map_daily")
    conn.execute("""
        INSERT INTO fact_map_daily (day, map_id, session_count, total_seconds)
        SELECT
            date(f.session_start) AS day,
            f.map_id,
            COUNT(f.id) AS session_count,
            COALESCE(SUM(f.calculated_duration), 0) AS total_seconds
        FROM fact_server_history f
        WHERE f.map_id IS NOT NULL
        GROUP BY day, f.map_id
    """)

    # Server daily (stats) from fact_history
    conn.execute("DELETE FROM fact_server_daily")
    conn.execute("""
        INSERT INTO fact_server_daily (day, server_id, session_count, total_seconds)
        SELECT
            date(h.session_start) AS day,
            h.server_id,
            COUNT(h.id) AS session_count,
            COALESCE(SUM(h.calculated_duration), 0) AS total_seconds
        FROM fact_history h
        WHERE h.server_id IS NOT NULL
        GROUP BY day, h.server_id
    """)

    # Player daily (stats) from fact_history
    conn.execute("DELETE FROM fact_player_daily")
    conn.execute("""
        INSERT INTO fact_player_daily (day, player_id, session_count, total_seconds)
        SELECT
            date(h.session_start) AS day,
            h.player_id,
            COUNT(h.id) AS session_count,
            COALESCE(SUM(h.calculated_duration), 0) AS total_seconds
        FROM fact_history h
        WHERE h.player_id IS NOT NULL
        GROUP BY day, h.player_id
    """)

    # Daily traffic (unique players/day)
    conn.execute("DELETE FROM fact_traffic_daily")
    conn.execute("""
        INSERT INTO fact_traffic_daily (day, unique_players)
        SELECT
            date(h.session_start) AS day,
            COUNT(DISTINCT h.player_id) AS unique_players
        FROM fact_history h
        WHERE h.player_id IS NOT NULL
        GROUP BY day
    """)

    _kv_set(conn, "rollups_backfilled", "1")

def refresh_recent_rollups(conn, scan_time, days_back=1):
    """
    Recompute rollups for today and the previous day (default),
    because new history rows only arrive for recent timestamps.
    """
    # We refresh for [today - days_back, today]
    # Example days_back=1 -> yesterday + today
    start_day = (scan_time - timedelta(days=days_back)).strftime("%Y-%m-%d")
    end_day = scan_time.strftime("%Y-%m-%d")

    # Operator daily
    conn.execute("DELETE FROM fact_operator_daily WHERE day BETWEEN ? AND ?", (start_day, end_day))
    conn.execute("""
        INSERT INTO fact_operator_daily (day, operator_name, server_count, unique_players, total_playtime_seconds, last_contact)
        SELECT
            date(h.session_start) AS day,
            s.operator_name,
            COUNT(DISTINCT h.server_id) AS server_count,
            COUNT(DISTINCT h.player_id) AS unique_players,
            COALESCE(SUM(h.calculated_duration), 0) AS total_playtime_seconds,
            MAX(h.session_end) AS last_contact
        FROM fact_history h
        JOIN dim_servers s ON h.server_id = s.id
        WHERE date(h.session_start) BETWEEN ? AND ?
          AND s.operator_name IS NOT NULL
          AND s.operator_name != 'Unknown'
        GROUP BY day, s.operator_name
    """, (start_day, end_day))

    # Map daily
    conn.execute("DELETE FROM fact_map_daily WHERE day BETWEEN ? AND ?", (start_day, end_day))
    conn.execute("""
        INSERT INTO fact_map_daily (day, map_id, session_count, total_seconds)
        SELECT
            date(f.session_start) AS day,
            f.map_id,
            COUNT(f.id) AS session_count,
            COALESCE(SUM(f.calculated_duration), 0) AS total_seconds
        FROM fact_server_history f
        WHERE date(f.session_start) BETWEEN ? AND ?
          AND f.map_id IS NOT NULL
        GROUP BY day, f.map_id
    """, (start_day, end_day))

    # Server daily
    conn.execute("DELETE FROM fact_server_daily WHERE day BETWEEN ? AND ?", (start_day, end_day))
    conn.execute("""
        INSERT INTO fact_server_daily (day, server_id, session_count, total_seconds)
        SELECT
            date(h.session_start) AS day,
            h.server_id,
            COUNT(h.id) AS session_count,
            COALESCE(SUM(h.calculated_duration), 0) AS total_seconds
        FROM fact_history h
        WHERE date(h.session_start) BETWEEN ? AND ?
          AND h.server_id IS NOT NULL
        GROUP BY day, h.server_id
    """, (start_day, end_day))

    # Player daily
    conn.execute("DELETE FROM fact_player_daily WHERE day BETWEEN ? AND ?", (start_day, end_day))
    conn.execute("""
        INSERT INTO fact_player_daily (day, player_id, session_count, total_seconds)
        SELECT
            date(h.session_start) AS day,
            h.player_id,
            COUNT(h.id) AS session_count,
            COALESCE(SUM(h.calculated_duration), 0) AS total_seconds
        FROM fact_history h
        WHERE date(h.session_start) BETWEEN ? AND ?
          AND h.player_id IS NOT NULL
        GROUP BY day, h.player_id
    """, (start_day, end_day))

    # Daily traffic
    conn.execute("DELETE FROM fact_traffic_daily WHERE day BETWEEN ? AND ?", (start_day, end_day))
    conn.execute("""
        INSERT INTO fact_traffic_daily (day, unique_players)
        SELECT
            date(h.session_start) AS day,
            COUNT(DISTINCT h.player_id) AS unique_players
        FROM fact_history h
        WHERE date(h.session_start) BETWEEN ? AND ?
          AND h.player_id IS NOT NULL
        GROUP BY day
    """, (start_day, end_day))


def main():
    start_time = time.time()
    scan_time = datetime.utcnow() # <--- CHANGED: .now() to .utcnow()
    print(f"--- [ SCAN STARTED: {scan_time.strftime('%H:%M:%S')} ] ---")
    
    init_db()

    public_ip = get_public_ip()
    if public_ip:
        print(f"[*] Identity Confirmed: {public_ip}")

    try:
        r = requests.get(API_URL, timeout=10)
        addrs = [s['addr'] for s in r.json().get("response", {}).get("servers", [])]
        print(f"[*] Targets Acquired: {len(addrs)}")
    except Exception as e:
        print(f"[!] Steam API Error: {e}")
        return

    valid_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for public_addr in addrs:
            target_ip_port = public_addr
            if public_ip and public_addr.startswith(public_ip):
                try:
                    _, port = public_addr.split(':')
                    target_ip_port = f"{LOCAL_LOOPBACK_IP}:{port}"
                except: pass

            future = executor.submit(query_server, target_ip_port)
            futures[future] = public_addr

        for f in concurrent.futures.as_completed(futures):
            original_public_addr = futures[f] # <--- This is the Real Public IP
            try:
                res = f.result()
                if res:
                    # FIX: The query used 127.0.0.1, but the Database needs the Public IP.
                    # We overwrite the 'addr' field in the result with the original target.
                    res['addr'] = original_public_addr 
                    
                    valid_results.append(res)
            except: pass
    
    print(f"[*] Processing {len(valid_results)} responses...")

    # --- CALC TOTALS ---
    total_active_servers = len(valid_results)
    total_active_players = sum(s["header_count"] for s in valid_results)
    # -------------------

    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        
        map_cache = {row[1]: row[0] for row in conn.execute("SELECT id, name FROM dim_maps").fetchall()}
        player_cache = {row[1]: row[0] for row in conn.execute("SELECT id, name FROM dim_players").fetchall()}
        
        server_cache = {}
        # New Cache Key Format: "IP:QueryPort"
        # FIX: Loop is correctly indented
        for row in conn.execute("SELECT id, ip_address, query_port, game_port, current_map_id, map_start, player_count, last_seen, name, current_session_uuid, operator_name FROM dim_servers").fetchall():
            cache_key = f"{row[1]}:{row[2]}" # IP:QueryPort
            server_cache[cache_key] = {
                'id': row[0], 
                'game_port': row[3],
                'map_id': row[4], 
                'map_start': parse_iso_time(row[5]), 
                'count': row[6], 
                'last_seen': row[7],
                'name': row[8],
                'session_uuid': row[9],
                'operator_name': row[10]
            }

        def get_map_id(m_name):
            if m_name in map_cache: return map_cache[m_name]
            cur = conn.execute("INSERT OR IGNORE INTO dim_maps (name) VALUES (?)", (m_name,))
            mid = cur.lastrowid or conn.execute("SELECT id FROM dim_maps WHERE name = ?", (m_name,)).fetchone()[0]
            map_cache[m_name] = mid
            return mid

        def get_player_id(p_name):
            if p_name in player_cache: return player_cache[p_name]
            cur = conn.execute("INSERT OR IGNORE INTO dim_players (name) VALUES (?)", (p_name,))
            pid = cur.lastrowid or conn.execute("SELECT id FROM dim_players WHERE name = ?", (p_name,)).fetchone()[0]
            player_cache[p_name] = pid
            return pid

        prune_limit = (scan_time - timedelta(minutes=PRUNE_THRESHOLD)).strftime('%Y-%m-%d %H:%M:%S')
        
        # --- [PRUNING UPDATE] Transfer calculated_duration from fact_active to fact_history ---
        conn.execute("""
            INSERT INTO fact_history (server_id, player_id, map_id, final_score, total_time, session_start, session_end, session_uuid, calculated_duration)
            SELECT server_id, player_id, map_id, score, duration, first_seen, last_seen, session_uuid, calculated_duration
            FROM fact_active
            WHERE last_seen < ?
        """, (prune_limit,))
        
        conn.execute("DELETE FROM fact_active WHERE last_seen < ?", (prune_limit,))
        
        for s in valid_results:
            current_ip = s["addr"].split(':')[0]
            current_qport = s["query_port"]
            cache_key = f"{current_ip}:{current_qport}"
            
            map_id = get_map_id(s["map"])
            
            # --- CALCULATE OPERATOR ---
            operator_name = clean_server_name(s["name"], current_ip)

            # --- GET LOCATION FROM DB ---
            # Use the helper to resolve against ip_ranges
            location_val = resolve_geo_db(conn, current_ip)
            
            # --- ATOMIC UPSERT ---
            # 1. Ensure server record exists (Using Identity: IP + QueryPort)
            # Added operator_name and location to INSERT statement
            conn.execute("""
                INSERT OR IGNORE INTO dim_servers (ip_address, query_port, game_port, name, current_map_id, last_seen, map_start, current_session_uuid, operator_name, location) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (current_ip, current_qport, s["game_port"], s["name"], map_id, scan_time, scan_time, str(uuid.uuid4()), operator_name, location_val))
            
            # 2. Retrieve authoritative ID from DB (or Cache if confident)
            if cache_key in server_cache:
                sdata = server_cache[cache_key]
                sid = sdata['id']
                db_game_port = sdata['game_port']
                db_map_id = sdata['map_id']
                db_map_start = sdata['map_start']
                db_session_uuid = sdata['session_uuid']
            else:
                # Cache miss (New IP or Cold Start). 
                # 1. Try DB lookup by IP
                row = conn.execute("SELECT id, game_port, current_session_uuid, current_map_id, map_start FROM dim_servers WHERE ip_address=? AND query_port=?", (current_ip, current_qport)).fetchone()
                
                if row:
                    sid = row[0]
                    db_game_port = row[1]
                    db_session_uuid = row[2]
                    db_map_id = row[3]
                    db_map_start = parse_iso_time(row[4])
                else:
                    # 2. Try DB lookup by Exact Name (Dynamic IP Recovery)
                    
                    # --- GENERIC NAME BLACKLIST ---
                    # These names are too common. Never merge them.
                    GENERIC_NAMES = {
                        "killing floor 2 server", "kf2 server", "kf2", "killing floor 2", 
                        "server", "dedicated server", "public server", "survival", 
                        "endless", "hard", "suicidal", "hoe", "hell on earth",
                        "gameservers.com", "linuxgsm", "nitrado.net", 
                        "kf2 server endless", "kf2 server hard and long",
                        "kf2 server long and hard", "kf2 server the zone",
                        "kf2 server very hard and long", "mgga make gaming great again"
                    }
                    
                    is_generic = s["name"].lower().strip() in GENERIC_NAMES
                    
                    # Also blacklist purely numeric names or very short names
                    if len(s["name"]) < 4 or s["name"].isdigit():
                        is_generic = True
                    # ------------------------------

                    candidates = []
                    if not is_generic:
                        candidates = conn.execute("SELECT id, game_port, current_session_uuid, current_map_id, map_start, ip_address FROM dim_servers WHERE name=?", (s["name"],)).fetchall()
                    
                    if len(candidates) >= 1:
                        # Found exactly one match. Assume it moved.
                        row = candidates[0]
                        sid = row[0]
                        db_game_port = row[1]
                        db_session_uuid = row[2]
                        db_map_id = row[3]
                        db_map_start = parse_iso_time(row[4])
                        old_ip = row[5]
                        
                        print(f"[!] Dynamic IP: {s['name']} moved from {old_ip} to {current_ip}")
                        try:
                            # Migrate record to new IP
                            conn.execute("UPDATE dim_servers SET ip_address=?, query_port=? WHERE id=?", (current_ip, current_qport, sid))
                        except sqlite3.IntegrityError:
                            # Collision (Rare): Just make a new ID
                            sid = None
                    else:
                        sid = None

                if sid is None:
                    # 3. Truly New Server. Create ID.
                    conn.execute("""
                        INSERT OR IGNORE INTO dim_servers (ip_address, query_port, game_port, name, current_map_id, last_seen, map_start, current_session_uuid, operator_name, location) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (current_ip, current_qport, s["game_port"], s["name"], map_id, scan_time, scan_time, str(uuid.uuid4()), operator_name, location_val))
                    
                    # Fetch ID again
                    sid = conn.execute("SELECT id FROM dim_servers WHERE ip_address=? AND query_port=?", (current_ip, current_qport)).fetchone()[0]
                    
                    # Default values for new server
                    db_game_port = s["game_port"]
                    db_map_id = map_id
                    db_map_start = scan_time
                    db_session_uuid = str(uuid.uuid4())

            # 3. Resolve Dynamic Data
            final_game_port = s["game_port"] if s["game_port"] else db_game_port
            
            # Logic: Determine if we need a NEW session UUID
            current_session_uuid = db_session_uuid            
            
            # 4. Map Rotation History
            if map_id != db_map_id:
                # --- UPDATE: Calculate duration in Python for server history ---
                duration_sec = int((scan_time - db_map_start).total_seconds())
                
                conn.execute("""
                    INSERT INTO fact_server_history (server_id, map_id, session_start, session_end, reason, session_uuid, calculated_duration)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (sid, db_map_id, db_map_start, scan_time, "Map Rotation", db_session_uuid, duration_sec))
                
                db_map_start = scan_time
                current_session_uuid = str(uuid.uuid4()) # New Match = New ID
                
            # --- [NEW] SECTION 4.5: MATCH RESTART DETECTION ---
            # Logic: If map is the same, but scores dropped from "High" to "Near Zero", it's a wipe.
            elif map_id == db_map_id:
                # 1. Get the aggregate score from the PREVIOUS scan (DB State)
                # We need to know what the score was before we overwrite it.
                row = conn.execute("SELECT SUM(score) FROM fact_active WHERE server_id=?", (sid,)).fetchone()
                prev_total_score = row[0] if row and row[0] else 0
                
                # 2. Calculate the aggregate score from the CURRENT scan (Live State)
                curr_total_score = sum(p['score'] for p in s['player_list'])

                # 3. The "Wipe" Thresholds
                # prev_total > 500: Ensures we don't log restarts for empty/idle servers.
                # curr_total < 200: Allows for starting cash/points, but implies a hard reset.
                if prev_total_score > 500 and curr_total_score < 200:
                    # --- UPDATE: Calculate duration in Python for server history ---
                    duration_sec = int((scan_time - db_map_start).total_seconds())
                    
                    conn.execute("""
                        INSERT INTO fact_server_history (server_id, map_id, session_start, session_end, reason, session_uuid, calculated_duration)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (sid, db_map_id, db_map_start, scan_time, "Match Restart", db_session_uuid, duration_sec))
                    
                    # CRITICAL: Reset the timer. 
                    # If we don't do this, the next "session" will look like it lasted 4 hours 
                    # instead of the 20 minutes it actually took them to fail.
                    db_map_start = scan_time
                    current_session_uuid = str(uuid.uuid4()) # Restart = New ID
            # --------------------------------------------------
            
            # 5. Update Server State
            # Added operator_name=? and location=? to SET clause
            conn.execute("""
                UPDATE dim_servers 
                SET name=?, current_map_id=?, player_count=?, map_start=?, last_seen=?, game_port=?, current_session_uuid=?, operator_name=?, location=?
                WHERE id=?
            """, (s["name"], map_id, s["header_count"], db_map_start, scan_time, final_game_port, current_session_uuid, operator_name, location_val, sid)) 
            
            # 6. Update Sessions
            for p in s["player_list"]:
                pid = get_player_id(p["name"])
                
                # --- UPDATE: Added calculated_duration math to fact_active ---
                conn.execute("""
                    INSERT INTO fact_active (server_id, player_id, map_id, score, duration, calculated_duration, first_seen, last_seen, session_uuid)
                    VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)
                    ON CONFLICT(server_id, player_id) DO UPDATE SET
                        score=excluded.score,
                        duration=excluded.duration,
                        calculated_duration=(strftime('%s', excluded.last_seen) - strftime('%s', fact_active.first_seen)),
                        map_id=excluded.map_id,
                        last_seen=excluded.last_seen,
                        session_uuid=excluded.session_uuid
                """, (sid, pid, map_id, p["score"], p["dur"], scan_time, scan_time, current_session_uuid))

        conn.execute("""
            INSERT INTO fact_global_stats (scan_time, active_servers, active_players)
            VALUES (?, ?, ?)
        """, (scan_time, total_active_servers, total_active_players))
        # --- DEAD SERVER CLEANUP ---
        # 1. Define the cutoff (15 minutes ago)
        server_timeout = (scan_time - timedelta(minutes=15))
        
        # 2. Move "Missing in Action" servers to history
        # We only archive them if they aren't already marked as empty/processed 
        # (Assuming player_count > 0 acts as our "active" flag here, 
        # otherwise you log a history entry for every 15m cycle a server stays dead)
        # --- UPDATE: Calculate duration using SQL math for dead servers ---
        conn.execute("""
            INSERT INTO fact_server_history (server_id, map_id, session_start, session_end, reason, session_uuid, calculated_duration)
            SELECT id, current_map_id, map_start, last_seen, 'Connection Lost', current_session_uuid,
                   (strftime('%s', last_seen) - strftime('%s', map_start))
            FROM dim_servers
            WHERE last_seen < ? AND player_count > 0
        """, (server_timeout,))

        # 3. Mark them as empty so they stop showing up as active
        # We also reset map_start to prevent duplicate history entries if it stays dead
        conn.execute("""
            UPDATE dim_servers
            SET player_count = 0, map_start = ?
            WHERE last_seen < ? AND player_count > 0
        """, (scan_time, server_timeout))
        # ---------------------------    
        # --- ROLLUPS ---
        backfill_rollups(conn)              # runs once, then becomes a no-op
        refresh_recent_rollups(conn, scan_time, days_back=1)  # yesterday + today
        
        conn.commit()
    
    print(f"--- [ CYCLE COMPLETE: {time.time() - start_time:.2f}s | Players: {total_active_players} ] ---")

if __name__ == "__main__":
    main()