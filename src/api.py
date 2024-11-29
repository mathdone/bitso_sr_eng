import requests
import time
import threading
import pandas as pd
import logging
from datetime import datetime
import os

class CoinGeckoClient:
    BASE_URL = "https://api.coingecko.com/api/v3"

    def __init__(self):
        self.session = requests.Session()
        self._lock = threading.Lock()
        self._last_request_time = 0
        self.logger = logging.getLogger(__name__)

    def _request(self, method, url, **kwargs):
        with self._lock:
            now = time.time()
            elapsed = now - self._last_request_time
            min_interval = 20.0 
            sleep_time = min_interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)
            self._last_request_time = time.time()
        response = self.session.request(method, url, **kwargs)
        response.raise_for_status()
        return response

    def get_exchanges(self):
        url = f"{self.BASE_URL}/exchanges"
        response = self._request('GET', url)
        data = response.json()
        for ex in data:
            yield ex

    def get_exchange_tickers(self, exchange_id: str):
        url = f"{self.BASE_URL}/exchanges/{exchange_id}/tickers"
        response = self._request('GET', url)
        data = response.json()
        for ticker in data.get('tickers', []):
            yield ticker

    def get_exchange_volume_chart(self, exchange_id: str, days: int):
        url = f"{self.BASE_URL}/exchanges/{exchange_id}/volume_chart"
        params = {'days': days}
        response = self._request('GET', url, params=params)
        data = response.json()
        return data

    def get_market_chart(self, coin_id: str, vs_currency: str, days: int):
        url = f"{self.BASE_URL}/coins/{coin_id}/market_chart"
        params = {'vs_currency': vs_currency, 'days': days}
        response = self._request('GET', url, params=params)
        data = response.json()
        return data

    def get_coin_list(self):
        url = f"{self.BASE_URL}/coins/list"
        response = self._request('GET', url)
        data = response.json()
        return data

    def get_supported_vs_currencies(self):
        url = f"{self.BASE_URL}/simple/supported_vs_currencies"
        response = self._request('GET', url)
        data = response.json()
        return data

    # New methods with processing logic
    def fetch_exchanges_data(self, partial_path=None):
        exchanges_data = []
        processed_ids = set()

        if partial_path and os.path.exists(partial_path):
            self.logger.info(f"Found partial file at {partial_path}, resuming from partial data")
            exchanges_data = pd.read_parquet(partial_path).to_dict('records')
            processed_ids = {exchange['exchange_id'] for exchange in exchanges_data}

        try:
            for exchange in self.get_exchanges():
                exchange_id = exchange.get('id')
                self.logger.info(f"Processing exchange: {exchange_id}")
                if exchange_id in processed_ids:
                    self.logger.info(f"Skipping already processed exchange: {exchange_id}")
                    continue
                exchanges_data.append({
                    'exchange_id': exchange_id,
                    'name': exchange.get('name'),
                    'year_established': exchange.get('year_established'),
                    'country': exchange.get('country'),
                    'trust_score': exchange.get('trust_score'),
                    'trust_score_rank': exchange.get('trust_score_rank'),
                    'extracted_at': datetime.utcnow()
                })
        except Exception as e:
            self.logger.exception("An error occurred while downloading exchanges.")
            if partial_path:
                df_exchanges_partial = pd.DataFrame(exchanges_data)
                df_exchanges_partial.to_parquet(partial_path, index=False)
            raise e
        return exchanges_data

    def fetch_all_tickers_data(self, exchange_ids, partial_path=None):
        tickers_data = []
        processed_exchanges = set()

        if partial_path and os.path.exists(partial_path):
            self.logger.info(f"Found partial file at {partial_path}, resuming from partial data")
            tickers_data = pd.read_parquet(partial_path).to_dict('records')
            processed_exchanges = {ticker['exchange_id'] for ticker in tickers_data}

        try:
            for exchange_id in exchange_ids:
                self.logger.info(f"Processing exchange: {exchange_id}")
                if exchange_id in processed_exchanges:
                    self.logger.info(f"Skipping already processed exchange: {exchange_id}")
                    continue
                tickers = list(self.get_exchange_tickers(exchange_id))
                extracted_at = datetime.utcnow()
                for ticker in tickers:
                    tickers_data.append({
                        'exchange_id': exchange_id,
                        'base': ticker.get('base'),
                        'target': ticker.get('target'),
                        'market_id': f"{ticker.get('base')}_{ticker.get('target')}",
                        'volume_usd': ticker.get('converted_volume', {}).get('usd'),
                        'extracted_at': extracted_at
                    })
        except Exception as e:
            self.logger.exception("An error occurred while downloading tickers.")
            if partial_path:
                df_tickers_partial = pd.DataFrame(tickers_data)
                df_tickers_partial.to_parquet(partial_path, index=False)
            raise e
        return tickers_data

    def identify_exchanges_with_shared_markets(self, tickers_data, partial_path=None):
        exchanges_with_shared_markets = set()

        if partial_path and os.path.exists(partial_path):
            self.logger.info(f"Found partial file at {partial_path}, resuming from partial data")
            df_exchanges_partial = pd.read_parquet(partial_path)
            exchanges_with_shared_markets = set(df_exchanges_partial['exchange_id'].tolist())
        else:
            exchanges_with_shared_markets = set()

        try:
            df_tickers = pd.DataFrame(tickers_data)
            df_bitso_tickers = df_tickers[df_tickers['exchange_id'] == 'bitso']
            bitso_markets = set(df_bitso_tickers['market_id'])

            df_shared_tickers = df_tickers[df_tickers['market_id'].isin(bitso_markets)]
            exchanges_with_shared_markets.update(df_shared_tickers['exchange_id'].unique())
        except Exception as e:
            self.logger.exception("An error occurred while identifying exchanges with shared markets.")
            if partial_path:
                df_exchanges_partial = pd.DataFrame({
                    'exchange_id': list(exchanges_with_shared_markets),
                    'extracted_at': datetime.utcnow()
                })
                df_exchanges_partial.to_parquet(partial_path, index=False)
            raise e
        return list(exchanges_with_shared_markets)

    def fetch_exchange_volume_data(self, exchange_ids, partial_path=None):
        exchange_volume_data = []
        processed_exchanges = set()

        if partial_path and os.path.exists(partial_path):
            self.logger.info(f"Found partial file at {partial_path}, resuming from partial data")
            exchange_volume_data = pd.read_parquet(partial_path).to_dict('records')
            processed_exchanges = {data['exchange_id'] for data in exchange_volume_data}

        try:
            for exchange_id in exchange_ids:
                self.logger.info(f"Processing exchange: {exchange_id}")
                if exchange_id in processed_exchanges:
                    self.logger.info(f"Skipping already processed exchange: {exchange_id}")
                    continue
                volume_data = self.get_exchange_volume_chart(exchange_id, days=1)
                extracted_at = datetime.utcnow()
                for timestamp, volume_btc in volume_data:
                    date = datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
                    exchange_volume_data.append({
                        'exchange_id': exchange_id,
                        'date': date,
                        'volume_btc': volume_btc,
                        'extracted_at': extracted_at
                    })
        except Exception as e:
            self.logger.exception("An error occurred while downloading exchange volume data.")
            if partial_path:
                df_exchange_volume_partial = pd.DataFrame(exchange_volume_data)
                df_exchange_volume_partial.to_parquet(partial_path, index=False)
            raise e
        return exchange_volume_data

    def fetch_market_volume_data(self, tickers_data, partial_path=None):
        market_volume_data = []
        processed_markets = set()

        if partial_path and os.path.exists(partial_path):
            self.logger.info(f"Found partial file at {partial_path}, resuming from partial data")
            market_volume_data = pd.read_parquet(partial_path).to_dict('records')
            processed_markets = {data['market_id'] for data in market_volume_data}

        try:
            df_tickers = pd.DataFrame(tickers_data)
            market_ids = df_tickers['market_id'].unique()

            self.logger.info("Fetching coin list and supported vs currencies")
            coin_list = self.get_coin_list()
            vs_currencies = self.get_supported_vs_currencies()

            symbol_to_coin_id = {coin['symbol'].upper(): coin['id'] for coin in coin_list}

            for market_id in market_ids:
                self.logger.info(f"Processing market: {market_id}")
                if market_id in processed_markets:
                    self.logger.info(f"Skipping already processed market: {market_id}")
                    continue
                base, target = market_id.split('_')
                base_symbol = base.upper()
                target_currency = target.lower()
                coin_id = symbol_to_coin_id.get(base_symbol)
                if not coin_id:
                    self.logger.warning(f"Coin ID not found for symbol: {base_symbol}, skipping market: {market_id}")
                    continue
                if target_currency not in vs_currencies:
                    self.logger.warning(f"Target currency not supported: {target_currency}, skipping market: {market_id}")
                    continue
                data = self.get_market_chart(coin_id, vs_currency=target_currency, days=30)
                extracted_at = datetime.utcnow()
                for timestamp, volume in data.get('total_volumes', []):
                    date = datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
                    market_volume_data.append({
                        'market_id': market_id,
                        'date': date,
                        'volume_usd': volume,
                        'extracted_at': extracted_at
                    })
        except Exception as e:
            self.logger.exception("An error occurred while downloading market volume data.")
            if partial_path:
                df_partial = pd.DataFrame(market_volume_data)
                df_partial.to_parquet(partial_path, index=False)
            raise e
        return market_volume_data
