"""
This script is designed as a one-time bootstrap utility to populate the database with historical price and metadata required for initial index construction. It is not part of the API flow and is intended to be executed manually only during the initial system setup.
"""

from app.database.db_manipulation import (
    get_ticker_table_data,
    insert_ticker_metadata,
    insert_price_data,
)
from app.market_data.yfinance_api import (
    get_sp500_tickers,
    get_ticker_metadata,
    get_historical_prices,
)
from datetime import datetime


def __main__():
    """
    Main function to populate the databases.
    This function fetches ticker metadata and daily prices, then inserts them into the database.
    """
    end_date = (
        datetime.today().date()
    )  # Define the end date for fetching historical prices. Here we use today's date at the time of execution.
    tickers = get_sp500_tickers()  # Get the list of S&P 500 tickers from Wikipedia
    ticker_metadata = get_ticker_metadata(
        tickers
    )  # Fetch ticker metadata from Yahoo Finance
    historical_data = get_historical_prices(
        tickers, end_date
    )  # Fetch daily prices from Yahoo Finance for the last 30 days
    insert_ticker_metadata(ticker_metadata)  # Insert ticker metadata into the database
    ticker_metadata = (
        get_ticker_table_data()
    )  # Refresh ticker metadata from the database to get shares outstanding
    for (
        data
    ) in (
        historical_data
    ):  # Update market cap in historical data based on shares outstanding * close price on that day
        data[3] = ticker_metadata[data[1]]["shares_outstanding"] * data[2]

    insert_price_data(
        historical_data
    )  # Insert daily prices into the database after updating market cap


if __name__ == "__main__":
    __main__()
