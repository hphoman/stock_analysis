from datetime import datetime
from dotenv import load_dotenv
import numpy as np
import os
import pandas as pd
import Scraper
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.orm import declarative_base

stock_df = Scraper.stock_scrape(7)
crypto_df = Scraper.crypto_scrape()
currency_df = Scraper.currency_scrape()
securities_df = Scraper.securities_scrape("2018-01-01")

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
