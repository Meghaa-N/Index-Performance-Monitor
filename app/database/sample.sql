-- Active: 1766261975522@@127.0.0.1@3306
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