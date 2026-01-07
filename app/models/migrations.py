"""
数据库迁移管理模块
支持版本化的 SQL 迁移文件，按顺序执行
"""
import os
import re
import hashlib
import time
from pathlib import Path
from typing import List, Tuple


class Migration:
    """单个迁移文件"""
    
    def __init__(self, filepath: Path):
        self.filepath = filepath
        self.filename = filepath.name
        
        # 解析文件名：V001__initial_schema.sql
        match = re.match(r'^V(\d+)__(.+)\.sql$', self.filename)
        if not match:
            raise ValueError(f"Invalid migration filename: {self.filename}")
        
        self.version = match.group(1)
        self.description = match.group(2).replace('_', ' ')
        self.checksum = self._calculate_checksum()
    
    def _calculate_checksum(self) -> str:
        """计算文件的 MD5 校验和"""
        md5 = hashlib.md5()
        with open(self.filepath, 'rb') as f:
            md5.update(f.read())
        return md5.hexdigest()
    
    def read_sql(self) -> str:
        """读取 SQL 内容"""
        with open(self.filepath, 'r', encoding='utf-8') as f:
            return f.read()
    
    def __repr__(self):
        return f"Migration(V{self.version}: {self.description})"
    
    def __lt__(self, other):
        """用于排序"""
        return int(self.version) < int(other.version)


class MigrationManager:
    """迁移管理器"""
    
    def __init__(self, db, migrations_dir: str = 'migrations'):
        self.db = db
        self.migrations_dir = Path(migrations_dir)
        
        if not self.migrations_dir.exists():
            raise FileNotFoundError(f"Migrations directory not found: {self.migrations_dir}")
    
    def _ensure_migrations_table(self):
        """确保迁移跟踪表存在"""
        import psycopg2
        import psycopg2.extras
        from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
        
        # 执行 V000__schema_version.sql
        v000_file = self.migrations_dir / 'V000__schema_version.sql'
        if not v000_file.exists():
            print("[ERROR] V000__schema_version.sql not found")
            return
        
        with open(v000_file, 'r', encoding='utf-8') as f:
            sql = f.read()
        
        # 清理 SQL
        lines = []
        for line in sql.split('\n'):
            stripped = line.strip()
            if stripped and not stripped.startswith('--'):
                lines.append(line)
        
        cleaned_sql = '\n'.join(lines)
        
        # 分割语句
        statements = []
        for s in cleaned_sql.split(';'):
            stmt = s.strip()
            if stmt:
                statements.append(stmt)
        
        if not statements:
            print("[WARN] V000 contains no executable statements")
            return
        
        conn = self.db.connect()
        # 设置为 autocommit 模式
        old_isolation = conn.isolation_level
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        try:
            for i, stmt in enumerate(statements, 1):
                try:
                    cur = conn.cursor()
                    cur.execute(stmt)
                    cur.close()
                    print(f"[DEBUG] V000 statement {i}/{len(statements)} executed")
                except psycopg2.Error as e:
                    # 忽略 "already exists" 错误
                    if 'already exists' in str(e).lower():
                        print(f"[DEBUG] V000 statement {i}: already exists (skipped)")
                    else:
                        print(f"[ERROR] V000 statement {i} failed: {e}")
                        raise
            
            print("[OK] Migration tracking table ensured")
        except Exception as e:
            print(f"[ERROR] Failed to create migration tracking table: {e}")
        finally:
            # 恢复原来的隔离级别
            conn.set_isolation_level(old_isolation)
    
    def get_all_migrations(self) -> List[Migration]:
        """获取所有迁移文件（按版本号排序）"""
        migrations = []
        
        for filepath in self.migrations_dir.glob('V*.sql'):
            # 跳过 V000（版本跟踪表）
            if filepath.name.startswith('V000'):
                continue
            
            try:
                migration = Migration(filepath)
                migrations.append(migration)
            except ValueError as e:
                print(f"[WARN] Skipping invalid migration file: {e}")
        
        # 按版本号排序
        migrations.sort()
        return migrations
    
    def get_applied_migrations(self) -> List[Tuple[str, str]]:
        """获取已应用的迁移（返回版本号和校验和）"""
        import psycopg2.extras
        
        conn = self.db.connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        try:
            cur.execute("""
                SELECT version, checksum 
                FROM schema_migrations 
                WHERE success = TRUE
                ORDER BY version
            """)
            results = cur.fetchall()
            print(f"[DEBUG] Found {len(results)} applied migrations in database")
            return [(row['version'], row['checksum']) for row in results]
        except Exception as e:
            # 如果表不存在或查询失败，回滚并返回空列表
            print(f"[DEBUG] Failed to query applied migrations: {e}")
            try:
                conn.rollback()
            except:
                pass
            return []
        finally:
            cur.close()
    
    def get_pending_migrations(self) -> List[Migration]:
        """获取待执行的迁移"""
        all_migrations = self.get_all_migrations()
        applied = {version for version, _ in self.get_applied_migrations()}
        
        pending = [m for m in all_migrations if m.version not in applied]
        return pending
    
    def detect_legacy_database(self) -> bool:
        """
        检测是否为旧版本初始化的数据库
        返回: True 如果数据库已初始化但没有 schema_migrations 表
        """
        import psycopg2.extras
        
        conn = self.db.connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        try:
            # 检查 schema_migrations 表是否存在
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'schema_migrations'
                ) as exists
            """)
            row = cur.fetchone()
            has_migrations_table = row['exists'] if row else False
            
            if has_migrations_table:
                return False  # 已经是新系统
            
            # 检查是否有关键表（dim_servers）存在
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'dim_servers'
                ) as exists
            """)
            row = cur.fetchone()
            has_core_tables = row['exists'] if row else False
            
            return has_core_tables  # 有核心表但没有迁移表 = 旧版本数据库
            
        except Exception as e:
            print(f"[DEBUG] Legacy detection failed: {e}")
            try:
                conn.rollback()
            except:
                pass
            return False
        finally:
            cur.close()
    
    def import_legacy_database(self) -> bool:
        """
        导入旧版本数据库状态
        将 V001 标记为已执行，而不实际执行
        """
        import psycopg2.extras
        
        print("\n[INFO] Detected legacy database (initialized without migration system)")
        print("[INFO] Importing existing database state...")
        
        conn = self.db.connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        try:
            # 确保 schema_migrations 表存在
            self._ensure_migrations_table()
            
            # 查找 V001 迁移
            v001 = None
            for m in self.get_all_migrations():
                if m.version == '001':
                    v001 = m
                    break
            
            if not v001:
                print("[WARN] V001 migration not found, cannot import legacy database")
                return False
            
            # 标记 V001 为已执行（不实际执行）
            cur.execute("""
                INSERT INTO schema_migrations (version, description, checksum, execution_time_ms, success)
                VALUES (%s, %s, %s, 0, TRUE)
                ON CONFLICT (version) DO NOTHING
            """, (v001.version, v001.description + ' (imported from legacy)', v001.checksum))
            conn.commit()
            
            print(f"[OK] Successfully imported legacy database state")
            print(f"[OK] Marked V001 as executed without running SQL")
            print(f"[OK] Future migrations will run normally")
            
            return True
            
        except Exception as e:
            print(f"[ERROR] Failed to import legacy database: {e}")
            conn.rollback()
            return False
        finally:
            cur.close()
    
    def apply_migration(self, migration: Migration) -> bool:
        """应用单个迁移"""
        import psycopg2.extras
        
        print(f"\n[INFO] Applying migration V{migration.version}: {migration.description}...")
        
        conn = self.db.connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        start_time = time.time()
        success = False
        error_msg = None
        
        try:
            # 读取并执行 SQL
            sql = migration.read_sql()
            
            # 清理 SQL：移除纯注释行和空行
            lines = []
            for line in sql.split('\n'):
                stripped = line.strip()
                # 跳过空行和纯注释行
                if stripped and not stripped.startswith('--'):
                    lines.append(line)
            
            cleaned_sql = '\n'.join(lines)
            
            # 分割为多个语句（以分号分隔）
            statements = []
            for s in cleaned_sql.split(';'):
                stmt = s.strip()
                # 只保留非空且不是纯注释的语句
                if stmt and not stmt.startswith('--'):
                    statements.append(stmt)
            
            if not statements:
                print(f"[WARN] Migration V{migration.version} contains no executable statements")
                # 标记为成功但警告
                execution_time = 0
            else:
                for i, statement in enumerate(statements, 1):
                    try:
                        cur.execute(statement)
                        conn.commit()
                    except Exception as e:
                        # 对于已存在的对象，只警告不中断
                        error_str = str(e).lower()
                        if 'already exists' in error_str or 'duplicate' in error_str:
                            print(f"[WARN] Statement {i}: {e}")
                            conn.rollback()
                            continue
                        else:
                            raise
                
                execution_time = int((time.time() - start_time) * 1000)
            
            # 记录迁移成功
            cur.execute("""
                INSERT INTO schema_migrations (version, description, checksum, execution_time_ms, success)
                VALUES (%s, %s, %s, %s, TRUE)
                ON CONFLICT (version) DO UPDATE SET
                    executed_at = CURRENT_TIMESTAMP,
                    execution_time_ms = EXCLUDED.execution_time_ms,
                    success = TRUE
            """, (migration.version, migration.description, migration.checksum, execution_time))
            conn.commit()
            
            success = True
            print(f"[OK] Migration V{migration.version} applied successfully in {execution_time}ms")
            
        except Exception as e:
            error_msg = str(e)
            print(f"[ERROR] Failed to apply migration V{migration.version}: {e}")
            conn.rollback()
            
            # 记录失败
            try:
                cur.execute("""
                    INSERT INTO schema_migrations (version, description, checksum, success)
                    VALUES (%s, %s, %s, FALSE)
                    ON CONFLICT (version) DO UPDATE SET
                        executed_at = CURRENT_TIMESTAMP,
                        success = FALSE
                """, (migration.version, migration.description, migration.checksum))
                conn.commit()
            except:
                pass
        
        finally:
            cur.close()
        
        return success
    
    def migrate(self) -> Tuple[int, int]:
        """
        执行所有待执行的迁移
        返回: (成功数量, 失败数量)
        """
        # 确保迁移跟踪表存在
        self._ensure_migrations_table()
        
        # 检测并导入旧版本数据库
        if self.detect_legacy_database():
            if not self.import_legacy_database():
                print("[ERROR] Failed to import legacy database, please check manually")
                return (0, 1)
        
        # 获取待执行的迁移
        pending = self.get_pending_migrations()
        
        if not pending:
            print("[INFO] No pending migrations")
            return (0, 0)
        
        print(f"\n[INFO] Found {len(pending)} pending migration(s):")
        for m in pending:
            print(f"  - V{m.version}: {m.description}")
        
        # 执行迁移
        success_count = 0
        fail_count = 0
        
        for migration in pending:
            if self.apply_migration(migration):
                success_count += 1
            else:
                fail_count += 1
                # 遇到失败就停止（保证顺序性）
                print(f"\n[ERROR] Migration failed, stopping at V{migration.version}")
                break
        
        return (success_count, fail_count)
    
    def status(self):
        """显示迁移状态"""
        print("\n" + "="*70)
        print("Database Migration Status")
        print("="*70)
        
        all_migrations = self.get_all_migrations()
        applied = {version: checksum for version, checksum in self.get_applied_migrations()}
        
        if not all_migrations:
            print("No migrations found")
            return
        
        print(f"\nTotal migrations: {len(all_migrations)}")
        print(f"Applied: {len(applied)}")
        print(f"Pending: {len(all_migrations) - len(applied)}")
        
        print("\nMigration List:")
        print("-" * 70)
        print(f"{'Version':<10} {'Status':<10} {'Description':<40}")
        print("-" * 70)
        
        for m in all_migrations:
            if m.version in applied:
                status = "[OK]"
                # 检查校验和是否匹配
                if applied[m.version] != m.checksum:
                    status = "[WARN]"
            else:
                status = "PENDING"
            
            print(f"V{m.version:<9} {status:<10} {m.description:<40}")
        
        print("="*70 + "\n")

