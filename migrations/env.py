from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine

from app.config import Settings
from app.models import Base

config = context.config
if config.config_file_name:
    fileConfig(config.config_file_name)

url = config.get_main_option("sqlalchemy.url") or Settings().database_url

with create_engine(url).connect() as connection:
    context.configure(connection=connection, target_metadata=Base.metadata)
    with context.begin_transaction():
        context.run_migrations()
