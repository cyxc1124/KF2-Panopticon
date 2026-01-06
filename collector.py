#!/usr/bin/env python3
"""
KF2-Panopticon 数据收集器入口
这是原 Query.py 的简化入口
"""
import os
import sys

# 确保可以导入模块
sys.path.insert(0, os.path.dirname(__file__))

# 导入原始的Query模块（暂时保持向后兼容）
import Query

if __name__ == "__main__":
    print("""
╔═══════════════════════════════════════════════════════════╗
║   KF2-Panopticon 数据收集器                               ║
║   Data Collector                                         ║
╚═══════════════════════════════════════════════════════════╝
    """)
    Query.main()

