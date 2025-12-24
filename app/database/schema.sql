/*
This file contains reference SQL used to define the database schema.
It is intended for documentation and bootstrap purposes only and is not executed
as part of the API runtime workflow.
*/

CREATE TABLE IF NOT EXISTS ticker (
    ticker VARCHAR PRIMARY KEY,
    company_name VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    active BOOLEAN DEFAULT TRUE,
    shares_outstanding BIGINT
);

CREATE TABLE IF NOT EXISTS price (
    ticker VARCHAR,
    date DATE,
    close FLOAT,
    market_cap DOUBLE,
    PRIMARY KEY (ticker, date),
    FOREIGN KEY (ticker) REFERENCES ticker(ticker)
);

CREATE TABLE IF NOT EXISTS index_composition (
        date DATE NOT NULL,
        ticker VARCHAR NOT NULL,
        weight DOUBLE NOT NULL,
        PRIMARY KEY (date, ticker),
        FOREIGN KEY (ticker) REFERENCES ticker(ticker)
    );


CREATE TABLE IF NOT EXISTS index_performance (
    date DATE NOT NULL,
    daily_return FLOAT,
    cumulative_return FLOAT,
    PRIMARY KEY (date),
);
