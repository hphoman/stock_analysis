from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

user = os.getenv('USER')
password = os.getenv('PASSWORD')
host = os.getenv('Host')
port = os.getenv('PORT')
dbname = os.getenv('DBNAME')

if not user:
    raise RuntimeError("Set username in environment or .env")
if not password:
    raise RuntimeError("Set password in environment or .env")
if not host:
    raise RuntimeError("Set host name in environment or .env")
if not port:
    raise RuntimeError("Set port in environment or .env")
if not dbname:
    raise RuntimeError("Set database name in environment or .env")

database_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(database_url)

Base = declarative_base()

class Stock(Base):
    __tablename__ = "stocks"
    id = Column(Integer, primary_key=True)
    ticker_name = Column(String)
    company_name = Column(String)
    price = Column(Float)
    change = Column(Float)
    percent_change = Column(Float)
    total_volume = Column(Float)
    avg_volume = Column(Float)
    market_cap = Column(Float)
    P_E_ratio = Column(Float)
    region = Column(String)
    section = Column(String)
    industry = Column(String)
    index_fund = Column(String)
    date = Column(Date)

class Crypto(Base):
    __tablename__ = "cryptos"
    id = Column(Integer, primary_key=True)
    token = Column(String)
    name = Column(String)
    cost = Column(Float)
    change = Column(Float)
    percent_change = Column(Float)
    date = Column(Date)

class Currency(Base):
    __tablename__ = "currencies"
    id = Column(Integer, primary_key=True)
    currency_token = Column(String)
    relative_amount = Column(Float)
    date = Column(Date)
    
class Security(Base):
    __tablename__ = "securities"
    id = Column(Integer, primary_key=True)
    series_id = Column(String)
    table_name = Column(String)
    value = Column(Float)
    date = Column(Date)

def new_setup(Base, engine) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)