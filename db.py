"""Database engine for openalex-sources.

Plain SQLAlchemy (no Flask). NullPool is used because Heroku Postgres sits behind
pgbouncer and the workloads here are short-lived background jobs, not a long-lived
connection-pooling web server.
"""
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

load_dotenv()


def database_url() -> str:
    url = os.environ["DATABASE_URL"]
    # Heroku hands out postgres:// ; SQLAlchemy 2.x requires postgresql://
    return url.replace("postgres://", "postgresql://", 1)


engine = create_engine(database_url(), poolclass=NullPool, future=True)
