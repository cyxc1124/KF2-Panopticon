"""路由蓝图模块"""
from app.routes.main import main_bp
from app.routes.servers import servers_bp
from app.routes.players import players_bp
from app.routes.factions import factions_bp
from app.routes.stats import stats_bp

__all__ = ['main_bp', 'servers_bp', 'players_bp', 'factions_bp', 'stats_bp']

