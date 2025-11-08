from bs4 import BeautifulSoup
from dotenv import load_dotenv
import numpy as np
import os
import pandas as pd
import requests 
import time

def parse_numeric(value):
    if not isinstance(value, str):
        return np.nan

    value = value.replace(",", "").strip()
    if value in ("N/A", "-", ""):
        return np.nan

    multiplier = 1
    if value[-1] in ["K", "M", "B", "T"]:
        suffix = value[-1]
        value = value[:-1]
        multiplier = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}[suffix]

    try:
        return float(value) * multiplier
    except ValueError:
        return np.nan
    
def safe_find_text(row, tag, **kwargs):
    cell = row.find(tag, **kwargs)
    return cell.text.strip() if cell else np.nan


def stock_scrape(header, max_page_count) -> pd.DataFrame:
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

        for j in range(100):
            row = data.find('tr', attrs={"data-testid-row": str(j)})
            if not row:
                continue

            ticker.append(safe_find_text(row, 'span', class_='symbol yf-90gdtp'))
            name.append(safe_find_text(row, 'div', class_='tw-text-left tw-max-w-32 yf-362rys'))
            price.append(safe_find_text(row, 'fin-streamer'))
            change.append(safe_find_text(row, 'fin-streamer', attrs={"data-field": "regularMarketChange"}))
            percent_change.append(safe_find_text(row, 'fin-streamer', attrs={"data-field": "regularMarketChangePercent"}))
            volume.append(safe_find_text(row, 'td', attrs={"data-testid-cell": "dayvolume"}))
            average_volumne.append(safe_find_text(row, 'td', attrs={"data-testid-cell": "avgdailyvol3m"}))
            market_cap.append(safe_find_text(row, 'td', attrs={"data-testid-cell": "intradaymarketcap"}))
            P_E.append(safe_find_text(row, 'td', attrs={"data-testid-cell": "peratio.lasttwelvemonths"}))
            region.append(safe_find_text(row, 'td', attrs={"data-testid-cell": "region"}))
            section.append(safe_find_text(row, 'td', attrs={"data-testid-cell": "sector"}))
            industry.append(safe_find_text(row, 'td', attrs={"data-testid-cell": "industry"}))
            index_fund.append(safe_find_text(row, 'span', class_="list yf-16u5z4n"))


        holding = pd.DataFrame(data={"Ticker": ticker, 'Company Name': name, 'Intraday Price': price, 'Change': change,
                            'Percent Change': percent_change, 'Volume': volume, 'Avg Volume': average_volumne,
                            "Market Cap": market_cap, "P/E Ratio": P_E, "Region": region, 'Section': section,
                            "Industry": industry, "Index Funds": index_fund})
        
        stock_df = pd.concat([stock_df, holding], ignore_index=True)
        if i != max_page_count - 1:
            print("Organization successful. Waiting...")
            time.sleep(5)
        else:
            print("Scrape sucessfull. Perfoming final organization...")

    stock_df['Intraday Price'] = pd.to_numeric(stock_df['Intraday Price'].str.replace(',', ""), downcast='float')

    change_mask = stock_df['Change'].str[0] == '-'
    stock_df['Change'] = pd.to_numeric(stock_df['Change'].str.replace("+","").str.replace(",", "").str.replace('-',""), 
                                    downcast='float')
    stock_df.loc[change_mask, 'Change'] *= -1

    change_mask = stock_df['Percent Change'].str[0] == '-'
    stock_df['Percent Change'] = pd.to_numeric(stock_df['Percent Change'].str.replace("+","").str.replace(",", "").str.replace('-',"").str.replace("%", ""), 
                                    downcast='float')
    stock_df.loc[change_mask, 'Percent Change'] *= -1

    stock_df['Volume'] = stock_df['Volume'].apply(parse_numeric)
    stock_df['Avg Volume'] = stock_df['Avg Volume'].apply(parse_numeric)
    stock_df['Market Cap'] = stock_df['Market Cap'].apply(parse_numeric)
    stock_df['P/E Ratio'] = pd.to_numeric(
    stock_df['P/E Ratio'].astype(str).str.replace('%', '', regex=False), errors='coerce', downcast='float')
    print("Stock information ready for analysis!")
    return stock_df

def crypto_scrape(header) -> pd.DataFrame:
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
    print("Crypto scrape complete. Performing final organization...")

    crypto_df['Name'] = crypto_df['Name'].str.split('(').str[0].str.split('/').str[0]
    crypto_df['Cost'] = crypto_df['Cost'].str.replace(',', '').astype(float)
    print("Crypto information ready for Analysis!")
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

    print("Currency scrape successful. Performing final organization...")

    currency_df['Date'] = pd.to_datetime(currency_df['Date'], format='%Y-%m-%d')
    print("Currency Information ready for review!")
    return currency_df

def securities_scrape(api_key, date) -> pd.DataFrame:
    url = 'https://api.stlouisfed.org/fred/category/series'
    params = {"category_id": 115, "api_key": api_key, "file_type": "json"}

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
            params_data = {"series_id": series_list[i], "api_key": api_key, "file_type": "json"}
        else:
            params_data = {"series_id": series_list[i], "api_key": api_key, "file_type": "json", "realtime_start": date}
            
        params_name = {"series_id": series_list[i], "api_key": api_key, "file_type": "json"}
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
        
    securities_df = pd.DataFrame()

    for i in range(len(series_list)):
        Date = []
        Name = []
        Value = []
        series_title = []
        for j in range(len(data_list[i]["observations"])):
            Date.append(data_list[i]["observations"][j]["date"])
            Value.append(data_list[i]["observations"][j]["value"])
            Name.append(series_list[i])
            series_title.append(name_list[i]['seriess'][0]['title'])
        holding = pd.DataFrame(data={"Series ID": Name, "Series Title": series_title, "Date": Date, "Value": Value})
        securities_df = pd.concat([securities_df, holding])

    print("Securities scrape successful. Performing Final organization...")
    securities_df['Value'] = pd.to_numeric(securities_df['Value'], errors='coerce')
    print("Security information ready for analysis!")
    return securities_df