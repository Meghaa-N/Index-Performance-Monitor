"""
Module to interact with Yahoo Finance API to fetch market data.
Most of the functions here are used to populate the database tables (one-time or periodic updates).
"""

import pandas as pd
import requests
from datetime import datetime, timedelta
from io import StringIO
import yfinance as yf

WIKI_SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


def get_sp500_tickers():
    """
    We scrape the list of S&P 500 companies from Wikipedia to get the tickers.
    This is intended to be used once to get the list of tickers. Subsequent runs
    will use a stored list/database.
    """
    url = WIKI_SP500_URL

    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    tables = pd.read_html(StringIO(response.text))
    sp500_df = tables[0]
    tickers = sp500_df["Symbol"].tolist()
    return tickers


def get_ticker_metadata(tickers: list):
    """
    The API that is used to fetch ticker metadata from Yahoo Finance.
    We get ticketer metadata including ticker, name, active status, primary exchange, and shares outstanding.

    :param tickers: List of ticker symbols
    """
    rows = []
    count = 0
    for ticker in tickers:
        count += 1
        try:
            yf_ticker = yf.Ticker(ticker)
            info = yf_ticker.info

            name = info.get("longName") or info.get("shortName", "")
            exchange = info.get("exchange", "")
            shares_outstanding = info.get("sharesOutstanding", 0)
            print(
                f"Fetched metadata for {count} {ticker}: {name}, {exchange}, {shares_outstanding}"
            )
            rows.append(
                (
                    ticker,
                    name,
                    exchange,
                    True,
                    shares_outstanding,
                    0.1,
                )  # Equal weighted initial allocation and we expect all the tickers to be active as it is from S&P500
            )

        except Exception as e:
            print(f"Error fetching metadata for {ticker}: {e}")

    return rows

def format_prices_data(tickers: list, df: pd.DataFrame, date: datetime):
    records = []
    for ticker in tickers:
        if ticker not in df.columns.levels[0]:
            continue

        tdf = df[ticker].dropna()
        for dt, row in tdf.iterrows():
            records.append(
                [
                    dt.date().strftime("%Y-%m-%d"),
                    ticker,
                    float(row["Close"]),
                    0.0,  # Placeholder for market cap
                ]
            )

    return records

def get_daily_prices(tickers: list, date: datetime):
    """
    Fetch daily prices for the given tickers on the specified date.

    :param tickers: List of ticker symbols
    :param date   : Date for which to fetch the prices (datetime.date)
    :return       : List of [date, ticker, close_price, market_cap]
    """
    start_date = date.strftime("%Y-%m-%d")
    end_date = (date + timedelta(days=1)).strftime("%Y-%m-%d")

    df = yf.download(
        tickers=tickers,
        start=start_date,
        end=end_date,
        interval="1d",
        group_by="ticker",
    )

    records = format_prices_data(tickers, df, date)

    return records



def get_historical_prices(tickers: list, date: datetime, days: int = 30):
    end_date = date
    start_date = end_date - timedelta(days=days)

    df = yf.download(
        tickers=tickers,
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        interval="1d",
        group_by="ticker",
    )

    records = format_prices_data(tickers, df, end_date)

    return records
