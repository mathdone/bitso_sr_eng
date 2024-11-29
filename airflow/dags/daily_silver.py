import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, date
import pandas as pd
import os
from pathlib import Path

from src.model import Deposit, Event, Exchange, ExchangesWithSharedMarkets, ExchangeVolume, MarketVolume, Tickers, User, UserLevel, Withdrawals

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

# Define file paths
BRONZE_PATH = "/data/bronze"
SILVER_PATH = "/data/silver"

def get_file_by_date(directory, run_date, file_extension):
    """
    Get the file from the directory corresponding to the run_date.
    """
    if isinstance(run_date, str):
        run_date = datetime.strptime(run_date, "%Y-%m-%d")  # Convert string to datetime

    date_str = run_date.strftime("%Y-%m-%d")
    file_path = Path(directory) / f"{date_str}.{file_extension}"
    if not file_path.exists():
        raise FileNotFoundError(f"No file found for date {date_str} in {directory}")
    return str(file_path)

def enforce_schema(table_name, table_schema, input_path, output_dir):
    """
    Reads the input file, enforces schema using the SQLAlchemy model, removes duplicates
    and null values based on primary keys, and writes the valid rows to a Parquet file.
    """
    try:
        output_path = os.path.join(output_dir, f"{table_name}.parquet")

        # Read input file
        df = pd.read_csv(input_path) if input_path.endswith(".csv") else pd.read_parquet(input_path)

        # Get schema column definitions and primary keys
        schema_columns = {col.name: col for col in table_schema.__table__.columns}
        primary_keys = [col.name for col in table_schema.__table__.primary_key.columns]

        # Validate and convert columns
        for col_name, col_def in schema_columns.items():
            try:
                target_type = col_def.type.python_type
                if col_name in df.columns:
                    # Handle date and datetime explicitly
                    if target_type == date:
                        df[col_name] = pd.to_datetime(df[col_name], errors="ignore").dt.date
                    elif target_type == datetime:
                        df[col_name] = pd.to_datetime(df[col_name], errors="ignore")
                    else:
                        df[col_name] = df[col_name].astype(target_type, errors="ignore")

                    logger.info(
                        f"Column: {col_name}, Target Type: {target_type}, "
                        f"Converted {df[col_name].notnull().sum()} valid values."
                    )
                else:
                    logger.warning(f"Column: {col_name} is missing in the input data.")
            except Exception as e:
                logger.error(f"Error converting column '{col_name}' - {str(e)}")
                raise

        # Remove rows with nulls in primary key columns
        df = df.dropna(subset=primary_keys)

        # Remove duplicate rows based on primary keys
        df = df.drop_duplicates(subset=primary_keys)

        # Write valid rows to a Parquet file
        df.to_parquet(output_path, index=False)
        logger.info(f"Successfully processed {input_path} to {output_path}.")
    except Exception as e:
        logger.error(f"Error processing table {table_name}: {e}")
        raise



def process_table(table_name, schema_class, file_extension, execution_date, **kwargs):
    """
    Processes a single table by reading from the bronze layer and writing to the silver layer.
    """
    input_dir = os.path.join(BRONZE_PATH, table_name)
    output_dir = os.path.join(SILVER_PATH, table_name)

    os.makedirs(output_dir, exist_ok=True)

    # Get file for the execution_date
    input_path = get_file_by_date(input_dir, execution_date, file_extension)

    enforce_schema(table_name, schema_class, input_path, output_dir)

with DAG(
    dag_id="bronze_to_silver_etl",
    default_args=default_args,
    description="ETL DAG to move data from bronze to silver layer",
    schedule_interval="@daily",
    start_date=datetime(2024, 11, 27),
    catchup=False,
) as dag:

    tables_to_process = [
        {"table_name": "deposit", "schema_class": Deposit},
        {"table_name": "event", "schema_class": Event},
        {"table_name": "exchanges", "schema_class": Exchange, "file_extension": "parquet"},
        {"table_name": "exchanges_with_shared_markets", "schema_class": ExchangesWithSharedMarkets, "file_extension": "parquet"},
        {"table_name": "exchange_volume", "schema_class": ExchangeVolume, "file_extension": "parquet"},
        {"table_name": "market_volume", "schema_class": MarketVolume, "file_extension": "parquet"},
        {"table_name": "tickers", "schema_class": Tickers, "file_extension": "parquet"},
        {"table_name": "user", "schema_class": User},
        {"table_name": "user_level", "schema_class": UserLevel},
        {"table_name": "withdrawals", "schema_class": Withdrawals},
    ]

    for table in tables_to_process:
        PythonOperator(
            task_id=f"process_{table['table_name']}",
            python_callable=process_table,
            op_kwargs={
                "table_name": table["table_name"],
                "schema_class": table["schema_class"],
                "file_extension": table.get("file_extension", "csv"),
                "execution_date": "{{ ds }}",
            },
        )
