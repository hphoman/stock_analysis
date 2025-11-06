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
securities_df = Scraper.securities_scrape(None)