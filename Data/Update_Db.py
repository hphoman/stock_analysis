from datetime import date, timedelta
from dotenv import load_dotenv
import numpy as np
import os
import pandas as pd
from psycopg2.extensions import register_adapter, AsIs
import Schema_Init
import Scraper
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.orm import declarative_base, sessionmaker

def nan_to_none(value):
    return None if pd.isna(value) else value

load_dotenv()

email = os.getenv('EMAIL')
FRED_Api_key = os.getenv("FRED_API_KEY")

if not email:
    raise RuntimeError("Set email address in environment or .env")
if not FRED_Api_key:
        raise RuntimeError("Set FRED_Api_key in enviorment or .env file")

base = Schema_Init.Base
engine = Schema_Init.engine
Stock = Schema_Init.Stock
Crypto = Schema_Init.Crypto
Currency = Schema_Init.Currency
Security = Schema_Init.Security

Session = sessionmaker(bind=engine)
session = Session()

last_date_security = session.query(Security).order_by(Security.date.desc()).first()
updated_date = last_date_security.date + timedelta(days=1)
if updated_date < date.today():
       updated_date = date.today()

header = {'User-Agent': f'Market Scraper v1.0 ({email})'}

stock_df = Scraper.stock_scrape(header, 7)
crypto_df = Scraper.crypto_scrape(header)
currency_df = Scraper.currency_scrape()
securities_df = Scraper.securities_scrape(FRED_Api_key, str(updated_date))

register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)
register_adapter(np.float32, AsIs)

stock_update = [Stock(ticker_name=nan_to_none(row['Ticker']),
        company_name=nan_to_none(row['Company Name']),
        price=row['Intraday Price'],
        change=row['Change'],
        percent_change=row['Percent Change'],
        total_volume=row['Volume'],
        avg_volume=row['Avg Volume'],
        market_cap=row['Market Cap'],
        P_E_ratio=row['P/E Ratio'],
        region=nan_to_none(row['Region']),
        section=nan_to_none(row['Section']),
        industry=nan_to_none(row['Industry']),
        index_fund=nan_to_none(row['Index Funds']),
        date=date.today()) for _, row in stock_df.iterrows()]


print("Stock initialization ready for upload.")

crypto_update = [Crypto(token=str(nan_to_none(row['Token'])),
                        name=str(nan_to_none(row['Name'])),
                        cost=row['Cost'],
                        change=float(row['Change']),
                        percent_change=float(row['Percent Change']),
                        date=date.today()) for _,row in crypto_df.iterrows()]

print("Crypto initialization ready for upload.")

currency_update = [Currency(currency_token = nan_to_none(row['Currency']),
                            relative_amount = row['Amount'],
                            date = row['Date']) for _, row in currency_df.iterrows()]

print("Currency initialization ready for upload.")

if securities_df.empty:
        print("No new security data. Beginning update...")
else:
        security_update = [Security(series_id=str(nan_to_none(row['Series ID'])),
                table_name=nan_to_none(row['Series Title']),
                value=row['Value'],
                date=row['Date']) for _, row in securities_df.iterrows()]

        print("Security initialization ready for upload.")
print("Beginning update...")

session.add_all(stock_update)
print("Stock upload successful...")
session.add_all(crypto_update)
print("Crypto upload successful...")
session.add_all(currency_update)
print("Currency upload successful...")
if securities_df.empty:
        print("Committing Changes...")
else:
        session.add_all(security_update)
        print("Security upload successful. Comitting changes...")

session.commit()
session.close()
print("Database update successful!")