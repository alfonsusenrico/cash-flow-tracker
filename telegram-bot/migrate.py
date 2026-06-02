#!/usr/bin/env python3
"""Run database migrations for the telegram bot."""
import asyncio
import sys
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).parent / "src"))

from bot.config import load_settings


async def run_migrations() -> None:
    """Apply all SQL migrations in order."""
    settings = load_settings()
    migrations_dir = Path(__file__).parent / "migrations"
    
    # Get all migration files sorted by name
    migration_files = sorted(migrations_dir.glob("*.sql"))
    
    if not migration_files:
        print("No migration files found.")
        return
    
    print(f"Found {len(migration_files)} migration file(s)")
    
    async with await psycopg.AsyncConnection.connect(settings.bot_database_url) as conn:
        # Create migrations tracking table if it doesn't exist
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_migrations (
                migration_name TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)
        
        # Get already applied migrations
        cur = await conn.execute("SELECT migration_name FROM bot_migrations")
        applied = {row[0] for row in await cur.fetchall()}
        
        # Apply pending migrations
        for migration_file in migration_files:
            migration_name = migration_file.name
            
            if migration_name in applied:
                print(f"✓ {migration_name} (already applied)")
                continue
            
            print(f"→ Applying {migration_name}...")
            sql = migration_file.read_text(encoding="utf-8")
            
            try:
                await conn.execute(sql)
                await conn.execute(
                    "INSERT INTO bot_migrations (migration_name) VALUES (%s)",
                    (migration_name,)
                )
                await conn.commit()
                print(f"✓ {migration_name} applied successfully")
            except Exception as e:
                await conn.rollback()
                print(f"✗ {migration_name} failed: {e}")
                raise
        
        print("\nAll migrations completed successfully!")


if __name__ == "__main__":
    asyncio.run(run_migrations())
