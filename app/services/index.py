from duckdb import df
from fastapi import FastAPI
from datetime import datetime, timedelta, date
import pandas as pd

from app.database.db_manipulation import (
    get_index_performance_table_data,
    get_top_hundred_tickers_by_market_cap,
    insert_index_composition,
    get_prev_trading_date,
    build_index_performance,
    delete_index_composition,
    delete_index_performance_table,
    get_index_composition,
)

from app.database.redis_client import flush_redis
from app.services.utils import download_data_as_excel

app = FastAPI(
    title="Index Performance Monitor",
    description="Equal-weighted index backend service",
    version="1.0.0",
)


@app.post("/build-index")
def build_index(start_date: date, end_date: date):
    """
    Endpoint to build index composition between start_date and end_date.

    We build the index tracking the top 100 tickers by market cap for each day in the given range.
    We also compute the index performance based on this composition.

    :param start_date   : Start date for index composition (YYYY-MM-DD)
    :param end_date     : End date for index composition (YYYY-MM-DD)
    """
    index_composition = get_top_hundred_tickers_by_market_cap(
        start_date.isoformat(), end_date.isoformat()
    )  # Fetch top 100 tickers by market cap
    delete_index_composition()  # Clear existing index composition
    index_composition_rows = []
    for date in index_composition:
        total_tickers = len(index_composition[date])
        for i in range(total_tickers):
            ticker, _ = index_composition[date][i]
            weight = 1 / total_tickers  # Equal weight for each ticker
            index_composition_rows.append((date, ticker, weight))
    insert_index_composition(
        index_composition_rows
    )  # Insert index composition into the database
    delete_index_performance_table()  # Clear existing index performance data
    build_index_performance(
        start_date.isoformat(), end_date.isoformat()
    )  # Compute and store index performance
    flush_redis()  # Clear relevant Redis cache
    return {"message": "Index built successfully"}


@app.get("/index-performance")
def get_index_performance(
    start_date: date, end_date: date
):
    """
    Endpoint to get index performance between start_date and end_date.

    :param start_date       : Start date for index performance (YYYY-MM-DD)
    :param end_date         : End date for index performance (YYYY-MM-DD)

    :return: Index performance data
    """
    performance_data = get_index_performance_table_data(start_date, end_date)
    return {"performance_data": performance_data}


@app.get("/index-composition")
def get_composition(date: date):
    """
    Endpoint to get index composition for a specific date.

    :param date: Date for which to fetch the index composition (YYYY-MM-DD)

    :return: List of tickers or Excel file of ticker and weights, depending on download_as_excel flag
    """
    composition = get_index_composition(date)
    return {"composition": [ticker for ticker, _ in composition]}


@app.get("/composition-changes")
def get_composition_changes(start_date: date, end_date: date):

    changes = {}

    current_date = start_date
    while current_date <= end_date:

        current_composition = get_index_composition(current_date)
        if not current_composition:
            current_date += timedelta(days=1)
            continue

        previous_date = get_prev_trading_date(current_date)
        if not previous_date:
            current_date += timedelta(days=1)
            continue

        previous_composition = get_index_composition(previous_date)
        if not previous_composition:
            current_date += timedelta(days=1)
            continue

        current_tickers = {t for t, _ in current_composition}
        previous_tickers = {t for t, _ in previous_composition}

        dt = current_date.isoformat()
        

        tickers_added = current_tickers - previous_tickers
        tickers_removed = previous_tickers - current_tickers
        if len(tickers_added) + len(tickers_removed) > 0:
            changes.setdefault(dt, {"added": [], "removed": []})
            for ticker in tickers_added:
                changes[dt]["added"].append(ticker)
            for ticker in tickers_removed:
                changes[dt]["removed"].append(ticker)

        current_date += timedelta(days=1)

    return changes

@app.post("/export-data")
def export_data(start_date: date, end_date: date):

    performance = get_index_performance_table_data(start_date, end_date)
    changes_dict = get_composition_changes(start_date, end_date)

    # Build Daily Compositions sheet
    comp_rows = []
    current_date = start_date
    while current_date <= end_date:
        comp = get_index_composition(current_date)
        for ticker, weight in comp:
            comp_rows.append({
                "Date": current_date.isoformat(),
                "Ticker": ticker,
                "Weight": round(weight * 100, 4)
            })
        current_date += timedelta(days=1)
    # Build Composition Changes sheet
    change_rows = []
    for dt, changes in changes_dict.items():
        for t in changes["added"]:
            change_rows.append({"Date": dt, "Ticker": t, "Change": "ADDED"})
        for t in changes["removed"]:
            change_rows.append({"Date": dt, "Ticker": t, "Change": "REMOVED"})

    sheets = {
        "Index_Performance": pd.DataFrame(
            performance,
            columns=["Date", "Daily_Return", "Cumulative_Return"]
        ),
        "Composition_Changes": pd.DataFrame(
            change_rows,
            columns=["Date", "Ticker", "Change"]
        ),
        "Daily_Compositions": pd.DataFrame(
            comp_rows,
            columns=["Date", "Ticker", "Weight"]
        )
    }

    filename = f"index_export_{start_date}_{end_date}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return download_data_as_excel(sheets, filename)
