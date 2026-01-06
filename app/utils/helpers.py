"""辅助工具函数"""
import math
import time
from flask import g


def format_duration(seconds):
    """格式化时长为人类可读格式"""
    if not seconds:
        return "0m"
    try:
        seconds = int(seconds)
    except ValueError:
        return "0m"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}h {m}m"
    return f"{m}m"


def parse_location(loc_str):
    """
    解析地理位置字符串
    Args:
        loc_str: 格式为 "City, Country" 的字符串
    Returns:
        dict: {"city": str, "flag": str}
    """
    if not loc_str or loc_str == 'Unknown':
        return {"flag": "unknown", "city": "Unknown"}
    
    parts = loc_str.split(',')
    if len(parts) >= 2:
        return {"city": parts[0].strip(), "flag": parts[1].strip().lower()}
    elif len(parts) == 1:
        return {"city": "Unknown", "flag": parts[0].strip().lower()}
    
    return {"flag": "unknown", "city": "Unknown"}


def get_pagination(count, page, per_page):
    """
    生成分页信息
    Args:
        count: 总记录数
        page: 当前页码
        per_page: 每页记录数
    Returns:
        dict: 分页信息
    """
    total_pages = math.ceil(count / per_page)
    return {
        "total": count,
        "pages": total_pages,
        "current": page,
        "has_next": page < total_pages,
        "has_prev": page > 1,
        "next_num": page + 1,
        "prev_num": page - 1
    }


class StepTimer:
    """性能计时器上下文管理器"""
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

