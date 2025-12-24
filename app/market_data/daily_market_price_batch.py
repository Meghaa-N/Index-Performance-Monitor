from datetime import datetime
from pytz import timezone
from app.market_data.yfinance_api import get_daily_prices
from app.database.db_manipulation import insert_price_data, get_ticker_table_data

def main():
    IST = timezone("Asia/Kolkata")
    run_date = datetime.now(IST).date()

    ticker_metadata = get_ticker_table_data() 
    tickers = list(ticker_metadata.keys())

    print(f"[BATCH] Fetching prices for {run_date}")

    data = get_daily_prices(tickers, run_date)

    if not data:
        print("[BATCH] No data fetched. Market closed.")
        return

    for row in data:
        ticker = row[1]
        close_price = row[2]
        shares = ticker_metadata.get(ticker, {}).get("shares_outstanding")
        if shares:
            row[3] = shares * close_price

    insert_price_data(data)
    print(f"[BATCH] Inserted {len(data)} price rows for {run_date}")

if __name__ == "__main__":
    main()
