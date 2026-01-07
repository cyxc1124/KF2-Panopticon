#!/usr/bin/env python3
"""
Database Initialization CLI Tool

Usage:
  python init_db.py                  # Run pending migrations
  python init_db.py --status         # Show migration status
  python init_db.py --force          # Force re-run all migrations (legacy mode)
  python init_db.py --migrate-status # Alias for --status
"""
import sys
from app.models.init_db import init_database, check_database_status
from app.models.migrations import MigrationManager
from app.models.database import get_database


def main():
    args = sys.argv[1:]
    
    # Migration status
    if '--status' in args or '--migrate-status' in args:
        try:
            db = get_database()
            manager = MigrationManager(db)
            manager.status()
        except Exception as e:
            print(f"[ERROR] Failed to get migration status: {e}")
            sys.exit(1)
        return
    
    # Force mode (legacy: re-initialize using old method)
    if '--force' in args:
        print("[WARN] Force re-initialization mode (legacy)\n")
        success = init_database(force=True)
        
        if success:
            print("\n" + "=" * 80)
            print("[OK] Database initialization successful!")
            print("=" * 80)
            sys.exit(0)
        else:
            print("\n" + "=" * 80)
            print("[ERROR] Database initialization failed")
            print("=" * 80)
            sys.exit(1)
        return
    
    # Default: Run migrations
    print("=" * 80)
    print("Database Migration Tool")
    print("=" * 80)
    
    try:
        # 确保数据库存在（自动创建）
        from app.models.init_db import _ensure_postgresql_database_exists
        if not _ensure_postgresql_database_exists():
            print("[ERROR] Failed to ensure database exists")
            sys.exit(1)
        
        db = get_database()
        manager = MigrationManager(db)
        
        # 显示当前状态
        print("\n[INFO] Checking migration status...")
        pending = manager.get_pending_migrations()
        
        if not pending:
            print("[OK] Database is up to date, no migrations needed")
            manager.status()
            sys.exit(0)
        
        # 执行迁移
        success_count, fail_count = manager.migrate()
        
        if fail_count > 0:
            print(f"\n[ERROR] {fail_count} migration(s) failed")
            sys.exit(1)
        
        if success_count > 0:
            print(f"\n[OK] Successfully applied {success_count} migration(s)")
            manager.status()
            sys.exit(0)
    
    except FileNotFoundError as e:
        print(f"[ERROR] {e}")
        print("\nPlease ensure 'migrations' directory exists with migration files")
        sys.exit(1)
    
    except Exception as e:
        print(f"[ERROR] Migration failed: {e}")
        print("\nPlease check:")
        print("  1. Database connection config is correct (config.py or env vars)")
        print("  2. PostgreSQL service is running")
        print("  3. Database user has sufficient privileges")
        print("  4. Migration files are valid SQL")
        sys.exit(1)


if __name__ == '__main__':
    if '--help' in sys.argv or '-h' in sys.argv:
        print(__doc__)
        sys.exit(0)
    
    main()

