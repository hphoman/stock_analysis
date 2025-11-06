from bs4 import BeautifulSoup
from dotenv import load_dotenv
import numpy as np
import os
import pandas as pd
import requests 
import time

load_dotenv()
email = os.getenv('EMAIL')
header = {'User-Agent': f'Market Scraper v1.0 ({email})'}

def stock_scrape(max_page_count) -> pd.DataFrame:
    yahoo_base = "https://finance.yahoo.com/research-hub/screener/equity/?"
    stock_df = pd.DataFrame()

    for i in range(max_page_count):
        counted_string = f"{i*100}&count=100"
        yahoo_url = yahoo_base + counted_string
        yahoo_request = requests.get(yahoo_url, headers=header)

        if yahoo_request.ok:
            print(f"Yahoo scrape {i+1} was successful. Organizing...")
            yahoo_soup = BeautifulSoup(yahoo_request.text, features="html.parser")
        else:
            raise ValueError(f"Yahoo request failed with status code {yahoo_request.status_code} for page {i}")
        
        data = yahoo_soup.find('div', class_="screener-table yf-18spdsn")
        ticker = []
        name = []
        price = []
        change = []
        percent_change = []
        volume = []
        average_volumne = []
        market_cap = []
        P_E = []
        region = []
        section = []
        industry = []
        index_fund = []

        for i in range(100):
            row = data.find('tr', attrs = {"data-testid-row": str(i)})
            ticker.append(row.find('span', class_='symbol yf-90gdtp').text.strip())
            name.append(row.find('div', class_="tw-text-left tw-max-w-32 yf-362rys").text.strip())
            price.append(row.find('fin-streamer').text.strip())
            change.append(row.find('fin-streamer', attrs={"data-field": "regularMarketChange"}).text.strip())
            percent_change.append(row.find('fin-streamer', attrs={"data-field": "regularMarketChangePercent"}).text.strip())
            volume.append(row.find('td', attrs={"data-testid-cell": "dayvolume"}).text.strip())
            average_volumne.append(row.find('td', attrs={"data-testid-cell": "avgdailyvol3m"}).text.strip())
            market_cap.append(row.find('td', attrs={"data-testid-cell": "intradaymarketcap"}).text.strip())
            P_E.append(row.find('td', attrs={"data-testid-cell": "peratio.lasttwelvemonths"}).text.strip())
            region.append(row.find('td', attrs={"data-testid-cell": "region"}).text.strip())
            section.append(row.find('td', attrs={"data-testid-cell": "sector"}).text.strip())
            industry.append(row.find('td', attrs={"data-testid-cell": "industry"}).text.strip())
            try:
                index_fund.append(row.find('span', class_="list yf-16u5z4n").text.strip())
            except AttributeError:
                index_fund.append(np.nan)

        holding = pd.DataFrame(data={"Ticker": ticker, 'Company Name': name, 'Intraday Price': price, 'Change': change,
                            'Percent Change': percent_change, 'Volume': volume, 'Avg Volume': average_volumne,
                            "Market Cap": market_cap, "P/E Ratio": P_E, "Region": region, 'Section': section,
                            "Industry": industry, "Index Funds": index_fund})
        
        stock_df = pd.concat([stock_df, holding], ignore_index=True)
        if i != max_page_count - 1:
            print("Organization successful. Waiting...")
            time.sleep(5)
        else:
            print("Stock scrape completed.")
    return stock_df

def crypto_scrape() -> pd.DataFrame:
    google_url = 'https://www.google.com/finance/markets/cryptocurrencies'
    google_request = requests.get(google_url, headers=header)
    if google_request.ok:
        print("Google scrape was successful. Organizing...")
        google_soup = BeautifulSoup(google_request.text, features="html.parser")
    else:
        raise ValueError(f"Google request failed with status code: {google_request.status_code}")

    data = google_soup.find('ul', class_='sbnBtf')
    rows = data.find_all('a')

    Token = []
    Name = []
    Cost = []
    Change = []
    Percent_Change = []

    for i in range(len(rows)):
        current_row = rows[i]
        Token.append(current_row.find('div', class_="COaKTb").text.strip())
        Name.append(current_row.find('div', class_="ZvmM7").text.strip())
        Cost.append(current_row.find('div', class_="YMlKec").text.strip())

        try:
            change = float(current_row.find('span', class_='P2Luy Ebnabc').text.strip().replace(",", ""))
            if change < 0:
                percent_change = float(current_row.find('div', class_="JwB6zf").text.strip().replace("%", "")) * -1
            else:
                percent_change = float(current_row.find('div', class_="JwB6zf").text.strip().replace("%", ""))
        except AttributeError:
            change = 0
            percent_change = 0

        Change.append(change)
        Percent_Change.append(percent_change)

    crypto_df = pd.DataFrame(data = {"Token": Token, "Name": Name, "Cost": Cost, "Change": Change, "Percent Change": Percent_Change})
    print("Crypto scrape complete.")
    return crypto_df

def currency_scrape() -> pd.DataFrame:
    currency_url = "https://api.frankfurter.dev/v1/latest?base=USD"
    currency_response = requests.get(currency_url)
    if currency_response.ok:
        print("Currency API call successful. Organizing ...")
        currency_data = currency_response.json()
    else:
        print(f"Currency API call failed with status code {currency_response.status_code}")

    keys = list(currency_data['rates'].keys())
    currency_df = pd.DataFrame()

    for key in keys:
        data = {"Currency": key, "Amount": currency_data['rates'][key], "Date": currency_data['date']}
        holding = pd.DataFrame([data])
        currency_df = pd.concat([currency_df, holding], ignore_index=True)

    print(f"Currency scrape successful.")
    return currency_df

def securities_scrape(date) -> pd.DataFrame:
    FRED_API_KEY = os.getenv("FRED_API_KEY")
    if not FRED_API_KEY:
        raise RuntimeError("Set FRED_API_KEY in enviorment or .env file")

    url = 'https://api.stlouisfed.org/fred/category/series'
    params = {"category_id": 115, "api_key": FRED_API_KEY, "file_type": "json"}

    securities_request = requests.get(url, params=params)

    if securities_request.ok:
        print("Series ID obtained. Gathering data...")
    else:
        print(f"Securities API call failed with status code {securities_request.status_code}")

    series_list = [s["id"] for s in securities_request.json()["seriess"] if s['id'][0] == 'D']

    data_list = []
    name_list = []
    bond_url = 'https://api.stlouisfed.org/fred/series/observations'
    name_url = 'https://api.stlouisfed.org/fred/series'

    for i in range(len(series_list)):
        if date == None:
            params_data = {"series_id": series_list[i], "api_key": FRED_API_KEY, "file_type": "json"}
        else:
            params_data = {"series_id": series_list[i], "api_key": FRED_API_KEY, "file_type": "json", "realtime_start": date}
            
        params_name = {"series_id": series_list[i], "api_key": FRED_API_KEY, "file_type": "json"}
        data_request = requests.get(bond_url, params=params_data)
        name_request = requests.get(name_url, params=params_name)
        if data_request.ok and name_request.ok:
            print(f"Request {i+1} successful. Organizing....")
            data_list.append(data_request.json())
            name_list.append(name_request.json())
            if i != len(series_list) - 1:
                print("Moving to next request...")
            else:
                print("All request successfully processed. Organizing...")
        elif not data_request.ok:
            raise ValueError(f"Request {i+1} for data from series id {series_list[i]} failed with status code {data_request.status_code}")
        elif not name_request.ok:
            raise ValueError(f"Request {i+1} for name information from series id {series_list[i]} failed with status code {name_request.status_code}")
        else:
            raise ValueError(f"Unknown error occured at series with id {series_list[i]}.\nName request status code:{name_request.status_code}\nData request status code: {data_request.status_code}")
        
    bond_df = pd.DataFrame()

    for i in range(len(series_list)):
        Date = []
        Name = []
        Value = []
        for j in range(len(data_list[i]["observations"])):
            Date.append(data_list[i]["observations"][j]["date"])
            Value.append(data_list[i]["observations"][j]["value"])
            Name.append(series_list[i])
        holding = pd.DataFrame(data={"Name": Name, "Date": Date, "Value": Value})
        bond_df = pd.concat([bond_df, holding])

    print("Securities scrape successful.")
    return bond_df