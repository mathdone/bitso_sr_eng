import logging
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.model import (
    Base,
    Deposit,
    Event,
    Exchange,
    ExchangesWithSharedMarkets,
    ExchangeVolume,
    MarketVolume,
    Tickers,
    User,
    UserLevel,
    Withdrawals,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_database_url():
    return "postgresql+psycopg2://bitsoproject:bitsoproject@postgres_db:5432/bitsoproject"

DATABASE_URL = get_database_url()
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

SILVER_PATH = "/data/silver"

# Models mapping to file names
TABLE_MAPPING = {
    "deposit": {
        "class": Deposit,
        "replace": True,
        },
    "event": {
        "class": Event,
        "replace": True,
        },
    "exchanges": {
        "class": Exchange,
        "replace": True,
        },
    "exchanges_with_shared_markets": {
        "class": ExchangesWithSharedMarkets,
        "replace": True,
        },
    "exchange_volume": {
        "class": ExchangeVolume,
        "replace": True,
        },
    "market_volume": {
        "class": MarketVolume,
        "replace": False,
    },
    "tickers": {
        "class": Tickers,
        "replace": True,
        },
    "user": {
        "class": User,
        "replace": True,
        },
    "user_level": {
        "class": UserLevel,
        "replace": True,
        },
    "withdrawals": {
        "class": Withdrawals,
        "replace": True,
        },
    }

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

def write_to_database(table_name, model_class, replace, **kwargs):
    """
    Reads a Parquet file and writes it to the database table mapped by the model_class.
    """
    file_path = os.path.join(SILVER_PATH, f"{table_name}/{table_name}.parquet")
    if not os.path.exists(file_path):
        logger.warning(f"File not found: {file_path}")
        return

    try:
        logger.info(f"Reading file: {file_path}")
        df = pd.read_parquet(file_path)

        records = df.where(pd.notnull(df), None).to_dict(orient="records")

        session = Session()
        if replace:
            session.execute(f'TRUNCATE TABLE "{table_name}"')
        session.bulk_insert_mappings(model_class, records)
        session.commit()
        logger.info(f"Successfully wrote {len(records)} records to {table_name}.")
    except Exception as e:
        logger.error(f"Error writing to {table_name}: {e}")
        raise
    finally:
        session.close()

with DAG(
    dag_id="silver_to_database",
    default_args=default_args,
    description="Writes silver files to the database using SQLAlchemy",
    schedule_interval="@daily",
    start_date=datetime(2024, 11, 27),
    catchup=False,
) as dag:

    for table_name, table_config in TABLE_MAPPING.items():
        PythonOperator(
            task_id=f"write_{table_name}_to_db",
            python_callable=write_to_database,
            op_kwargs={"table_name": table_name, "model_class": table_config['class'], "replace": table_config['replace']},
        )
