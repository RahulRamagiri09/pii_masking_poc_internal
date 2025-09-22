#!/usr/bin/env python3
"""
Check current roles and users in database.
"""
import asyncio
from pii_masking.core.database import engine
from sqlalchemy import text

async def check_data():
    print("Current Database Contents:")
    print("=" * 30)

    async with engine.connect() as conn:
        # Check roles
        result = await conn.execute(text('SELECT id, rolename FROM roles ORDER BY id;'))
        roles = result.fetchall()
        print(f"Roles ({len(roles)}):")
        for role in roles:
            print(f"  {role[0]}: {role[1]}")

        # Check users
        result = await conn.execute(text('SELECT COUNT(*) FROM users;'))
        user_count = result.fetchone()[0]
        print(f"\nUsers: {user_count}")

    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(check_data())