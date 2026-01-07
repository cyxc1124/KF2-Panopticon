#!/usr/bin/env python3
"""
Database Initialization CLI Tool

Usage:
  python init_db.py              # Initialize database (if not initialized)
  python init_db.py --force      # Force re-initialization
  python init_db.py --status     # Check database status only
"""
import sys
from app.models.init_db import init_database, check_database_status


def main():
    args = sys.argv[1:]
    
    # Check status
    if '--status' in args:
        print("=" * 80)
        print("Database Status Check")
        print("=" * 80)
        
        status = check_database_status()
        
        print(f"\nDatabase Type: {status['type']}")
        print(f"Initialized: {'Yes' if status['initialized'] else 'No'}")
        
        if status['error']:
            print(f"Error: {status['error']}")
        else:
            print(f"Table Count: {len(status['tables'])}")
            if status['tables']:
                print(f"\nTable List:")
                for table in status['tables']:
                    print(f"  - {table}")
        
        return
    
    # Initialize database
    force = '--force' in args
    
    if force:
        print("[WARN] Force re-initialization mode\n")
    
    success = init_database(force=force)
    
    if success:
        print("\n" + "=" * 80)
        print("[OK] Database initialization successful!")
        print("=" * 80)
        
        # Show final status
        status = check_database_status()
        print(f"\nDatabase Info:")
        print(f"  Type: {status['type']}")
        print(f"  Table Count: {len(status['tables'])}")
        
        sys.exit(0)
    else:
        print("\n" + "=" * 80)
        print("[ERROR] Database initialization failed")
        print("=" * 80)
        print("\nPlease check:")
        print("  1. Database connection config is correct (config.py or env vars)")
        print("  2. PostgreSQL service is running")
        print("  3. Database user has sufficient privileges")
        
        sys.exit(1)


if __name__ == '__main__':
    if '--help' in sys.argv or '-h' in sys.argv:
        print(__doc__)
        sys.exit(0)
    
    main()

