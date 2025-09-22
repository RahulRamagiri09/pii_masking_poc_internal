#!/usr/bin/env python3
"""
Standalone Role Management Script

This script allows you to manage roles directly in the database
without using the API endpoints.

Usage:
    python manage_roles.py create <rolename>    # Create a new role
    python manage_roles.py list                 # List all roles
    python manage_roles.py delete <rolename>    # Delete a role
    python manage_roles.py clear                # Delete all roles (careful!)

Examples:
    python manage_roles.py create admin
    python manage_roles.py create user
    python manage_roles.py create moderator
    python manage_roles.py list
    python manage_roles.py delete moderator
"""

import sys
import asyncio
from datetime import datetime, timezone
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from pii_masking.core.config import settings


class RoleManager:
    def __init__(self):
        self.engine = create_async_engine(settings.DATABASE_URL)

    async def create_role(self, rolename: str):
        """Create a new role directly in the database."""
        async with self.engine.connect() as conn:
            # Check if role already exists
            result = await conn.execute(
                text("SELECT COUNT(*) FROM roles WHERE rolename = :rolename"),
                {"rolename": rolename}
            )
            count = result.fetchone()[0]

            if count > 0:
                print(f"ERROR: Role '{rolename}' already exists!")
                return False

            # Insert new role
            await conn.execute(
                text("""
                    INSERT INTO roles (rolename, created_at, is_active)
                    VALUES (:rolename, :created_at, :is_active)
                """),
                {
                    "rolename": rolename,
                    "created_at": datetime.now(timezone.utc),
                    "is_active": True
                }
            )
            await conn.commit()
            print(f"SUCCESS: Role '{rolename}' created successfully!")
            return True

    async def list_roles(self):
        """List all roles in the database."""
        async with self.engine.connect() as conn:
            result = await conn.execute(
                text("SELECT id, rolename, created_at, is_active FROM roles ORDER BY id")
            )
            roles = result.fetchall()

            if not roles:
                print("No roles found in database.")
                return

            print(f"Found {len(roles)} role(s):")
            print("-" * 60)
            print(f"{'ID':<4} {'Role Name':<20} {'Created':<20} {'Active'}")
            print("-" * 60)

            for role in roles:
                created_str = role[2].strftime("%Y-%m-%d %H:%M") if role[2] else "Unknown"
                active_str = "Yes" if role[3] else "No"
                print(f"{role[0]:<4} {role[1]:<20} {created_str:<20} {active_str}")

    async def delete_role(self, rolename: str):
        """Delete a role from the database."""
        async with self.engine.connect() as conn:
            # Check if role exists
            result = await conn.execute(
                text("SELECT id FROM roles WHERE rolename = :rolename"),
                {"rolename": rolename}
            )
            role = result.fetchone()

            if not role:
                print(f"ERROR: Role '{rolename}' not found!")
                return False

            role_id = role[0]

            # Check if role is being used by any users
            result = await conn.execute(
                text("SELECT COUNT(*) FROM users WHERE role_id = :role_id"),
                {"role_id": role_id}
            )
            user_count = result.fetchone()[0]

            if user_count > 0:
                print(f"ERROR: Cannot delete role '{rolename}' - it's being used by {user_count} user(s)!")
                return False

            # Delete the role
            await conn.execute(
                text("DELETE FROM roles WHERE rolename = :rolename"),
                {"rolename": rolename}
            )
            await conn.commit()
            print(f"SUCCESS: Role '{rolename}' deleted successfully!")
            return True

    async def clear_all_roles(self):
        """Delete all roles (use with caution)."""
        async with self.engine.connect() as conn:
            # Check if any users exist
            result = await conn.execute(text("SELECT COUNT(*) FROM users"))
            user_count = result.fetchone()[0]

            if user_count > 0:
                print(f"ERROR: Cannot clear roles - {user_count} user(s) exist! Delete users first.")
                return False

            # Get count of roles to delete
            result = await conn.execute(text("SELECT COUNT(*) FROM roles"))
            role_count = result.fetchone()[0]

            if role_count == 0:
                print("No roles to delete.")
                return True

            # Confirm deletion
            response = input(f"WARNING: Are you sure you want to delete all {role_count} roles? (yes/no): ")
            if response.lower() != 'yes':
                print("Operation cancelled.")
                return False

            # Delete all roles
            await conn.execute(text("DELETE FROM roles"))
            await conn.commit()
            print(f"SUCCESS: All {role_count} roles deleted successfully!")
            return True

    async def close(self):
        """Close the database connection."""
        await self.engine.dispose()


async def main():
    if len(sys.argv) < 2:
        print("ERROR: Missing command!")
        print(__doc__)
        return

    command = sys.argv[1].lower()
    manager = RoleManager()

    try:
        if command == "create":
            if len(sys.argv) != 3:
                print("ERROR: Role name required!")
                print("Usage: python manage_roles.py create <rolename>")
                return

            rolename = sys.argv[2]
            if not rolename.strip():
                print("ERROR: Role name cannot be empty!")
                return

            await manager.create_role(rolename.strip())

        elif command == "list":
            await manager.list_roles()

        elif command == "delete":
            if len(sys.argv) != 3:
                print("ERROR: Role name required!")
                print("Usage: python manage_roles.py delete <rolename>")
                return

            rolename = sys.argv[2]
            await manager.delete_role(rolename.strip())

        elif command == "clear":
            await manager.clear_all_roles()

        else:
            print(f"ERROR: Unknown command '{command}'!")
            print(__doc__)

    except Exception as e:
        print(f"DATABASE ERROR: {e}")
    finally:
        await manager.close()


if __name__ == "__main__":
    asyncio.run(main())