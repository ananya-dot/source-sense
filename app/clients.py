"""
This file contains the client for the Supabase metadata extraction application.

Note:
- The DB_CONFIG is overriden from the base class to setup the connection string for Supabase.
- Supabase uses PostgreSQL with SSL enabled by default.
"""

from application_sdk.clients.sql import BaseSQLClient


class SupabaseClient(BaseSQLClient):
    """
    This client handles connection string generation for Supabase databases.
    Supabase provides PostgreSQL databases with SSL enabled by default.
    """

    DB_CONFIG = {
        "template": "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}?sslmode=require",
        "required": ["user", "password", "host", "port", "database"],
    }
