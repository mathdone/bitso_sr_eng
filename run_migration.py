from alembic.config import Config
from alembic import command

def run_migration():
    # Load Alembic configuration
    alembic_cfg = Config("alembic.ini")

    # Run the upgrade to the latest version
    command.upgrade(alembic_cfg, "head")

if __name__ == "__main__":
    run_migration()
