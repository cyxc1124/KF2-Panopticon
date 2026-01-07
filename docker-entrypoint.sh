#!/bin/bash
# KF2-Panopticon 统一入口点
# 用于 K8s 多容器部署

set -e

MODE=${1:-web}

echo "=========================================="
echo "KF2-Panopticon Container"
echo "Mode: $MODE"
echo "=========================================="

case "$MODE" in
  web)
    echo "Starting Web Application..."
    exec python run.py
    ;;
  
  collector)
    echo "Starting Data Collector..."
    exec python Query.py
    ;;
  
  init)
    echo "Initializing Database..."
    exec python init_db.py
    ;;
  
  init-force)
    echo "Force Initializing Database..."
    exec python init_db.py --force
    ;;
  
  status)
    echo "Checking Database Status..."
    exec python init_db.py --status
    ;;
  
  *)
    echo "Unknown mode: $MODE"
    echo "Available modes: web, collector, init, init-force, status"
    exit 1
    ;;
esac

