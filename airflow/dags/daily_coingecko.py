import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
from src.api import CoinGeckoClient
import pandas as pd
import os

# Set up logging
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 27),
    'retries': 5,
    'retry_delay': timedelta(seconds=60),
    'concurrency': 1
}

dag = DAG(
    'coingecko_dag',
    default_args=default_args,
    description='Extract data from CoinGecko API',
    schedule_interval=timedelta(days=1),
)

client = CoinGeckoClient()

def get_exchanges(**kwargs):
    run_date = kwargs['ds']
    output_dir = "/data/bronze/exchanges"
    partial_path = os.path.join(output_dir, f'{run_date}_partial.parquet')
    output_path = os.path.join(output_dir, f'{run_date}.parquet')
    os.makedirs(output_dir, exist_ok=True)

    if os.path.exists(output_path):
        logger.info(f"Output file {output_path} already exists, skipping task.")
        raise AirflowSkipException(f"Output file {output_path} already exists.")

    logger.info("Starting download of exchanges")

    exchanges_data = client.fetch_exchanges_data(partial_path=partial_path)

    logger.info(f"Writing exchanges data to {output_path}")
    df_exchanges = pd.DataFrame(exchanges_data)
    df_exchanges.to_parquet(output_path, index=False)
    if os.path.exists(partial_path):
        os.remove(partial_path)

def get_all_tickers(**kwargs):
    run_date = kwargs['ds']
    output_dir = "/data/bronze/tickers"
    partial_path = os.path.join(output_dir, f'{run_date}_partial.parquet')
    output_path = os.path.join(output_dir, f'{run_date}.parquet')
    os.makedirs(output_dir, exist_ok=True)

    if os.path.exists(output_path):
        logger.info(f"Output file {output_path} already exists, skipping task.")
        raise AirflowSkipException(f"Output file {output_path} already exists.")

    exchanges_input_path = os.path.join("/data/bronze/exchanges", f'{run_date}.parquet')
    logger.info(f"Reading exchanges data from {exchanges_input_path}")
    df_exchanges = pd.read_parquet(exchanges_input_path)
    exchange_ids = df_exchanges['exchange_id'].tolist()

    logger.info("Starting download of tickers for all exchanges")

    tickers_data = client.fetch_all_tickers_data(exchange_ids, partial_path=partial_path)

    logger.info(f"Writing tickers data to {output_path}")
    df_tickers = pd.DataFrame(tickers_data)
    df_tickers.to_parquet(output_path, index=False)
    if os.path.exists(partial_path):
        os.remove(partial_path)

def get_exchanges_with_shared_markets(**kwargs):
    run_date = kwargs['ds']
    output_dir = "/data/bronze/exchanges_with_shared_markets"
    partial_path = os.path.join(output_dir, f'{run_date}_partial.parquet')
    output_path = os.path.join(output_dir, f'{run_date}.parquet')
    os.makedirs(output_dir, exist_ok=True)

    if os.path.exists(output_path):
        logger.info(f"Output file {output_path} already exists, skipping task.")
        raise AirflowSkipException(f"Output file {output_path} already exists.")

    tickers_path = os.path.join("/data/bronze/tickers", f'{run_date}.parquet')
    logger.info(f"Reading tickers data from {tickers_path}")
    df_tickers = pd.read_parquet(tickers_path)
    tickers_data = df_tickers.to_dict('records')

    exchanges_with_shared_markets = client.identify_exchanges_with_shared_markets(tickers_data, partial_path=partial_path)

    logger.info(f"Writing exchanges with shared markets data to {output_path}")
    df_exchanges_with_shared_markets = pd.DataFrame({
        'exchange_id': exchanges_with_shared_markets,
        'extracted_at': datetime.utcnow()
    })
    df_exchanges_with_shared_markets.to_parquet(output_path, index=False)
    if os.path.exists(partial_path):
        os.remove(partial_path)

def get_exchange_volume_data(**kwargs):
    run_date = kwargs['ds']
    output_dir = "/data/bronze/exchange_volume"
    partial_path = os.path.join(output_dir, f'{run_date}_partial.parquet')
    output_path = os.path.join(output_dir, f'{run_date}.parquet')
    os.makedirs(output_dir, exist_ok=True)

    if os.path.exists(output_path):
        logger.info(f"Output file {output_path} already exists, skipping task.")
        raise AirflowSkipException(f"Output file {output_path} already exists.")

    input_path = os.path.join("/data/bronze/exchanges_with_shared_markets", f'{run_date}.parquet')
    logger.info(f"Reading exchanges with shared markets from {input_path}")
    df_exchanges_with_shared_markets = pd.read_parquet(input_path)
    exchanges_with_shared_markets = df_exchanges_with_shared_markets['exchange_id'].tolist()

    logger.info("Starting download of exchange volume data")

    exchange_volume_data = client.fetch_exchange_volume_data(exchanges_with_shared_markets, partial_path=partial_path)

    logger.info(f"Writing exchange volume data to {output_path}")
    df_exchange_volume = pd.DataFrame(exchange_volume_data)
    df_exchange_volume.to_parquet(output_path, index=False)
    if os.path.exists(partial_path):
        os.remove(partial_path)

def get_market_volume_data(**kwargs):
    run_date = kwargs['ds']
    output_dir = "/data/bronze/market_volume"
    partial_path = os.path.join(output_dir, f'{run_date}_partial.parquet')
    output_path = os.path.join(output_dir, f'{run_date}.parquet')
    os.makedirs(output_dir, exist_ok=True)

    if os.path.exists(output_path):
        logger.info(f"Output file {output_path} already exists, skipping task.")
        raise AirflowSkipException(f"Output file {output_path} already exists.")

    tickers_path = os.path.join("/data/bronze/tickers", f'{run_date}.parquet')
    logger.info(f"Reading tickers data from {tickers_path}")
    df_tickers = pd.read_parquet(tickers_path)
    tickers_data = df_tickers.to_dict('records')

    logger.info("Starting download of market volume data")

    market_volume_data = client.fetch_market_volume_data(tickers_data, partial_path=partial_path)

    logger.info(f"Writing market volume data to {output_path}")
    df_market_volume = pd.DataFrame(market_volume_data)
    df_market_volume.to_parquet(output_path, index=False)
    if os.path.exists(partial_path):
        os.remove(partial_path)

get_exchanges_task = PythonOperator(
    task_id='get_exchanges',
    python_callable=get_exchanges,
    provide_context=True,
    dag=dag,
)

get_all_tickers_task = PythonOperator(
    task_id='get_all_tickers',
    python_callable=get_all_tickers,
    provide_context=True,
    dag=dag,
    trigger_rule='none_failed',
)

get_exchanges_with_shared_markets_task = PythonOperator(
    task_id='get_exchanges_with_shared_markets',
    python_callable=get_exchanges_with_shared_markets,
    provide_context=True,
    dag=dag,
    trigger_rule='none_failed',
)

get_exchange_volume_data_task = PythonOperator(
    task_id='get_exchange_volume_data',
    python_callable=get_exchange_volume_data,
    provide_context=True,
    dag=dag,
    trigger_rule='none_failed',
)

get_market_volume_data_task = PythonOperator(
    task_id='get_market_volume_data',
    python_callable=get_market_volume_data,
    provide_context=True,
    dag=dag,
    trigger_rule='none_failed',
)

get_exchanges_task >> get_all_tickers_task >> [get_exchanges_with_shared_markets_task, get_market_volume_data_task]
get_exchanges_with_shared_markets_task >> get_exchange_volume_data_task
