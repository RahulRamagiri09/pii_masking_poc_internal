#!/usr/bin/env python3
"""
Database verification script to check if tables were created successfully.
"""
import asyncio
from sqlalchemy import text
from pii_masking.core.database import engine

async def verify_tables():
    print("Database Table Verification")
    print("=" * 40)

    async with engine.connect() as connection:
        # Check if tables exist
        result = await connection.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """))

        tables = [row[0] for row in result.fetchall()]

        print(f"Found {len(tables)} tables:")
        for table in tables:
            print(f"  - {table}")

        # Check alembic version
        try:
            result = await connection.execute(text("SELECT version_num FROM alembic_version;"))
            version = result.fetchone()
            if version:
                print(f"\nAlembic version: {version[0]}")
            else:
                print("\nNo Alembic version found")
        except Exception as e:
            print(f"\nAlembic version table not found: {e}")

        # Check roles table structure
        if 'roles' in tables:
            result = await connection.execute(text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'roles'
                ORDER BY ordinal_position;
            """))

            print(f"\nRoles table structure:")
            for row in result.fetchall():
                print(f"  {row[0]} | {row[1]} | {'NULL' if row[2] == 'YES' else 'NOT NULL'}")

        # Check users table structure
        if 'users' in tables:
            result = await connection.execute(text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'users'
                ORDER BY ordinal_position;
            """))

            print(f"\nUsers table structure:")
            for row in result.fetchall():
                print(f"  {row[0]} | {row[1]} | {'NULL' if row[2] == 'YES' else 'NOT NULL'}")

    print("\nDatabase verification completed!")

if __name__ == "__main__":
    asyncio.run(verify_tables())