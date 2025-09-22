#!/usr/bin/env python3
"""
Bootstrap script to create the first admin user.

This script is used for initial setup when no users exist in the system.
Once an admin is created, all subsequent user creation must be done through the API.

Usage:
    python create_first_admin.py <username> <email> <password>

Examples:
    python create_first_admin.py admin admin@company.com SecurePassword123
    python create_first_admin.py superuser super@example.com MyPassword456
"""

import sys
import asyncio
from datetime import datetime, timezone
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from pii_masking.core.config import settings
from pii_masking.auth.security import get_password_hash


class AdminBootstrap:
    def __init__(self):
        self.engine = create_async_engine(settings.DATABASE_URL)

    async def create_first_admin(self, username: str, email: str, password: str):
        """Create the first admin user for bootstrapping the system."""
        async with self.engine.connect() as conn:
            # Check if any users already exist
            result = await conn.execute(text("SELECT COUNT(*) FROM users"))
            user_count = result.fetchone()[0]

            if user_count > 0:
                print(f"ERROR: Users already exist in the system ({user_count} users found)!")
                print("Use the API to create additional users.")
                return False

            # Check if Admin role exists
            result = await conn.execute(
                text("SELECT id FROM roles WHERE rolename = :rolename"),
                {"rolename": "Admin"}
            )
            admin_role = result.fetchone()

            if not admin_role:
                print("ERROR: Admin role not found!")
                print("Please run: python manage_roles.py create Admin")
                return False

            admin_role_id = admin_role[0]

            # Check if username or email already exists (shouldn't happen if no users exist)
            result = await conn.execute(
                text("SELECT COUNT(*) FROM users WHERE username = :username OR email = :email"),
                {"username": username, "email": email}
            )
            existing_count = result.fetchone()[0]

            if existing_count > 0:
                print(f"ERROR: Username '{username}' or email '{email}' already exists!")
                return False

            # Hash the password
            hashed_password = get_password_hash(password)

            # Create the admin user
            await conn.execute(
                text("""
                    INSERT INTO users (username, email, hashed_password, role_id, created_at, is_active)
                    VALUES (:username, :email, :hashed_password, :role_id, :created_at, :is_active)
                """),
                {
                    "username": username,
                    "email": email,
                    "hashed_password": hashed_password,
                    "role_id": admin_role_id,
                    "created_at": datetime.now(timezone.utc),
                    "is_active": True
                }
            )
            await conn.commit()
            print(f"SUCCESS: First admin user '{username}' created successfully!")
            print(f"Email: {email}")
            print(f"Role: Admin")
            print("\nYou can now login and create additional users through the API.")
            return True

    async def close(self):
        """Close the database connection."""
        await self.engine.dispose()


async def main():
    if len(sys.argv) != 4:
        print("ERROR: Invalid arguments!")
        print(__doc__)
        return

    username = sys.argv[1].strip()
    email = sys.argv[2].strip()
    password = sys.argv[3].strip()

    if not username or not email or not password:
        print("ERROR: Username, email, and password cannot be empty!")
        return

    if len(password) < 8:
        print("ERROR: Password must be at least 8 characters long!")
        return

    bootstrap = AdminBootstrap()

    try:
        await bootstrap.create_first_admin(username, email, password)
    except Exception as e:
        print(f"DATABASE ERROR: {e}")
    finally:
        await bootstrap.close()


if __name__ == "__main__":
    asyncio.run(main())