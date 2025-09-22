#!/usr/bin/env python3
"""
Test audit functionality in the database.
"""
import asyncio
from sqlalchemy import text
from pii_masking.core.database import engine

async def test_audit():
    print("Testing Audit Columns")
    print("=" * 30)

    async with engine.connect() as conn:
        # Check all columns in roles table including audit fields
        result = await conn.execute(text("""
            SELECT id, rolename, created_by, created_at, updated_by, updated_at, is_active
            FROM roles
            ORDER BY id;
        """))

        roles = result.fetchall()
        print(f"Found {len(roles)} roles with audit information:")
        print("-" * 100)
        print(f"{'ID':<3} {'Role Name':<20} {'Created By':<12} {'Created At':<20} {'Updated By':<12} {'Is Active'}")
        print("-" * 100)

        for role in roles:
            created_by = role[2] if role[2] else "System"
            updated_by = role[4] if role[4] else "None"
            created_at = role[3].strftime("%Y-%m-%d %H:%M") if role[3] else "Unknown"
            print(f"{role[0]:<3} {role[1]:<20} {created_by:<12} {created_at:<20} {updated_by:<12} {role[6]}")

        # Check users table as well
        result = await conn.execute(text("""
            SELECT id, username, created_by, created_at, updated_by, updated_at, is_active
            FROM users
            ORDER BY id;
        """))

        users = result.fetchall()
        print(f"\nFound {len(users)} users with audit information:")

        if users:
            print("-" * 100)
            print(f"{'ID':<3} {'Username':<20} {'Created By':<12} {'Created At':<20} {'Updated By':<12} {'Is Active'}")
            print("-" * 100)

            for user in users:
                created_by = user[2] if user[2] else "System"
                updated_by = user[4] if user[4] else "None"
                created_at = user[3].strftime("%Y-%m-%d %H:%M") if user[3] else "Unknown"
                print(f"{user[0]:<3} {user[1]:<20} {created_by:<12} {created_at:<20} {updated_by:<12} {user[6]}")
        else:
            print("No users found.")

    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(test_audit())