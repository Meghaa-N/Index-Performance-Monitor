from datetime import date, timedelta

from .decorators import memoize, with_db_connection

"""
Module for database interactions.
"""


@with_db_connection
def insert_ticker_metadata(conn, ticker_metadata):
    """
    Insert ticker metadata into the ticker table.
    If a ticker already exists, update its information.

    :param conn             : DuckDB connection object
    :param ticker_metadata  : List of tuples containing ticker metadata
    """
    insert_query = """
        INSERT INTO ticker (
            ticker,
            company_name,
            exchange,
            active,
            shares_outstanding,
            weight
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (ticker) DO UPDATE SET
            company_name = excluded.company_name,
            exchange = excluded.exchange,
            active = excluded.active,
            shares_outstanding = excluded.shares_outstanding,
            weight = excluded.weight;
    """

    conn.executemany(insert_query, ticker_metadata)


@with_db_connection
def get_ticker_table_data(conn):
    """
    Fetch all ticker metadata from the ticker table.

    :param conn: DuckDB connection object
    :return: Dictionary mapping ticker symbols to their metadata
    """
    rows = conn.execute("SELECT * FROM ticker").fetchall()
    return {
        row[0]: {
            "ticker": row[0],
            "company_name": row[1],
            "exchange": row[2],
            "active": row[3],
            "shares_outstanding": row[4],
        }
        for row in rows
    }


@with_db_connection
def get_tickers(conn):
    """
    Fetch all ticker symbols from the ticker table.

    :param conn: DuckDB connection object
    """
    rows = conn.execute("SELECT ticker FROM ticker").fetchall()
    return [row[0] for row in rows]


@with_db_connection
def insert_price_data(conn, data):

    insert_query = """
        INSERT INTO price (
            date,
            ticker,
            close_price,
            market_cap
        )
        VALUES (?, ?, ?, ?)
        ON CONFLICT (date, ticker) DO UPDATE SET
            close_price = excluded.close_price,
            market_cap = excluded.market_cap;
    """
    conn.executemany(insert_query, data)


@with_db_connection
def get_top_hundred_tickers_by_market_cap(conn, start_date, end_date=None):
    """
    Fetch top 100 tickers by market cap for each day in the given range from the price table.

    :param conn      : DuckDB connection object
    :param start_date: Start date for the range (inclusive)
    :param end_date  : End date for the range (inclusive). If None, we provide data only on start_date.

    :return: List of tuples containing ticker symbols, market cap
    """
    index_composition = {}
    end_date = end_date or start_date

    query = """
    WITH valid_trading_days AS (
        SELECT DISTINCT date
        FROM price
        WHERE date BETWEEN ? AND ?
    ),
    ranked AS (
        SELECT
            p.date,
            p.ticker,
            p.market_cap,
            ROW_NUMBER() OVER (
                PARTITION BY p.date
                ORDER BY p.market_cap DESC
            ) AS rank
        FROM price p
        JOIN valid_trading_days v
        ON p.date = v.date
    )
    SELECT date, ticker, market_cap
    FROM ranked
    WHERE rank <= 100
    ORDER BY date, rank;
    """
    rows = conn.execute(query, [start_date, end_date]).fetchall()
    index_composition = {}
    for dt, ticker, market_cap in rows:
        dt_str = dt.isoformat()
        index_composition.setdefault(dt_str, []).append((ticker, market_cap))
    return index_composition


@with_db_connection
def insert_index_composition(conn, composition_data):
    """
    Insert index composition data into the index_composition table.

    :param conn             : DuckDB connection object
    :param composition_data : List of
    """
    insert_query = """
        INSERT INTO index_composition (
            date,
            ticker,
            weight
        )
        VALUES (?, ?, ?)
    """
    conn.executemany(insert_query, composition_data)


@with_db_connection
def delete_index_composition(conn):
    """
    Delete all records from the index_composition table.
    This is useful for resetting the index composition before inserting new data.

    :param conn: DuckDB connection object
    """
    conn.execute("DELETE FROM index_composition;")


@memoize
@with_db_connection
def get_index_composition(conn, date):
    """
    Fetch index composition for a given date from the index_composition table.

    :param conn: DuckDB connection object
    :param date: Date for which to fetch the index composition
    :return: List of tuples containing ticker symbols and their weights
    """
    query = """
    SELECT
        ticker,
        weight
    FROM index_composition
    WHERE date = ?
    ORDER BY weight DESC;
    """
    rows = conn.execute(query, [date]).fetchall()
    return rows


@with_db_connection
def set_index_performance(conn, current_date: str):

    # Fetch composition
    composition = conn.execute(
        """
        SELECT ticker, weight
        FROM index_composition
        WHERE date = ?
    """,
        (current_date,),
    ).fetchall()

    # Skip if market closed / no composition
    if len(composition) != 100:
        return

    # Get previous trading date
    prev_row = conn.execute(
        """
        SELECT MAX(date)
        FROM index_composition
        WHERE date < ?
    """,
        (current_date,),
    ).fetchone()

    prev_date = prev_row[0]
    if not prev_date:
        # First valid trading day
        conn.execute(
            """
            INSERT INTO index_performance(date, daily_return, cumulative_return)
            VALUES (?, 0.0, 0.0)
            ON CONFLICT(date) DO NOTHING
        """,
            (current_date,),
        )
        return

    daily_return = 0.0
    valid_count = 0

    for ticker, weight in composition:
        row = conn.execute(
            """
            SELECT t.close_price, p.close_price
            FROM price t
            JOIN price p
              ON t.ticker = p.ticker
            WHERE t.ticker = ?
              AND t.date = ?
              AND p.date = ?
        """,
            (ticker, current_date, prev_date),
        ).fetchone()

        if not row:
            return  # Abort entire day – partial data is unacceptable

        today_close, prev_close = row
        daily_return += weight * ((today_close / prev_close) - 1)
        valid_count += 1

    if valid_count != 100:
        return

    prev_cum = conn.execute(
        """
        SELECT cumulative_return
        FROM index_performance
        WHERE date = ?
    """,
        (prev_date,),
    ).fetchone()

    if not prev_cum:
        return

    cumulative_return = (1 + prev_cum[0]) * (1 + daily_return) - 1

    conn.execute(
        """
        INSERT INTO index_performance(date, daily_return, cumulative_return)
        VALUES (?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            daily_return = excluded.daily_return,
            cumulative_return = excluded.cumulative_return
    """,
        (current_date, daily_return, cumulative_return),
    )


def build_index_performance(start_date: str, end_date: str):
    """
    Build index performance data for a range of dates.

    :param start_date: Start date for the range (inclusive)
    :param end_date  : End date for the range (inclusive)
    """
    current_date = date.fromisoformat(start_date)
    end_date_obj = date.fromisoformat(end_date)

    while current_date <= end_date_obj:
        set_index_performance(current_date.isoformat())
        current_date += timedelta(days=1)


@with_db_connection
def create_index_performance_table(conn):
    """
    Create the index_performance table if it does not exist.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS index_performance (
            date DATE PRIMARY KEY,
            daily_return DOUBLE,
            cumulative_return DOUBLE
        );
        """
    )


@with_db_connection
def delete_index_performance_table(conn):
    """
    Delete the index_performance table if it exists.
    This is useful for resetting the performance data, while creating the index composition afresh.

    @param conn: DuckDB connection object
    """
    conn.execute(
        """
        DELETE FROM index_performance;
        """
    )


@memoize
@with_db_connection
def get_index_performance_per_day(conn, date: date):
    """
    Fetch index performance data for a specific date.

    :param conn      : DuckDB connection object
    :param date      : Date for which we must get the performance data
    :return          : Tuple containing date, daily return, cumulative return
    """
    query = """
    SELECT
        date,
        daily_return,
        cumulative_return
    FROM index_performance
    WHERE date = ?
    ORDER BY date;
    """
    row = conn.execute(query, [date]).fetchone()
    return row


@with_db_connection
def get_index_performance_table_data(conn, start_date: date, end_date: date):
    """
    Fetch index performance data between start_date and end_date.

    :param conn      : DuckDB connection object
    :param start_date: Start date for the range (inclusive)
    :param end_date  : End date for the range (inclusive)
    :return          : List of tuples containing date, daily return, cumulative return
    """
    curr_date = start_date
    performance_data = []
    while curr_date <= end_date:
        row = get_index_performance_per_day(curr_date.isoformat())
        if row:
            performance_data.append(row)
        curr_date += timedelta(days=1)
    return performance_data


@with_db_connection
def get_prev_trading_date(conn, date):
    """
    Fetch the previous trading date before the given date.

    :param conn: DuckDB connection object
    :param date: Date for which to find the previous trading date
    :return: Previous trading date or None if not found
    """
    query = """
    SELECT MAX(date)
    FROM index_composition
    WHERE date < ?;
    """
    row = conn.execute(query, [date]).fetchone()
    return row[0] if row and row[0] else None
