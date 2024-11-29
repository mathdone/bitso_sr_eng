from sqlalchemy import Column, Integer, String, Float, DateTime, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Deposit(Base):
    __tablename__ = 'deposit'
    
    id = Column(Integer, primary_key=True)
    event_timestamp = Column(DateTime)
    user_id = Column(String, index=True)
    amount = Column(Float)
    currency = Column(String, index=True)
    tx_status = Column(String, index=True)

class Event(Base):
    __tablename__ = 'event'
    
    id = Column(Integer, primary_key=True)
    event_timestamp = Column(DateTime)
    user_id = Column(String, index=True)
    event_name = Column(String, index=True)

class Exchange(Base):
    __tablename__ = 'exchanges'
    
    exchange_id = Column(String, primary_key=True)
    name = Column(String)
    year_established = Column(Integer)
    country = Column(String)
    trust_score = Column(Integer)
    trust_score_rank = Column(Integer)
    extracted_at = Column(DateTime)

class ExchangesWithSharedMarkets(Base):
    __tablename__ = 'exchanges_with_shared_markets'
    
    exchange_id = Column(String, primary_key=True)
    extracted_at = Column(DateTime)

class ExchangeVolume(Base):
    __tablename__ = 'exchange_volume'
    
    exchange_id = Column(String, primary_key=True)
    date = Column(Date, primary_key=True)
    volume_btc = Column(Float)
    extracted_at = Column(DateTime)

class MarketVolume(Base):
    __tablename__ = 'market_volume'
    
    market_id = Column(String, primary_key=True)
    date = Column(Date, primary_key=True)
    volume_usd = Column(Float)
    extracted_at = Column(DateTime)

class Tickers(Base):
    __tablename__ = 'tickers'
    
    exchange_id = Column(String, primary_key=True)
    base = Column(String)
    target = Column(String)
    market_id = Column(String)
    volume_usd = Column(Float)
    extracted_at = Column(DateTime)

class User(Base):
    __tablename__ = 'user'
    
    user_id = Column(String, primary_key=True)

class UserLevel(Base):
    __tablename__ = 'user_level'
    
    user_id = Column(String, primary_key=True)
    level = Column(Integer)
    event_timestamp = Column(DateTime, primary_key=True)
    jurisdiction = Column(String)

class Withdrawals(Base):
    __tablename__ = 'withdrawals'
    
    id = Column(Integer, primary_key=True)
    event_timestamp = Column(DateTime)
    user_id = Column(String, index=True)
    amount = Column(Float)
    interface = Column(String)
    currency = Column(String)
    tx_status = Column(String, index=True)
