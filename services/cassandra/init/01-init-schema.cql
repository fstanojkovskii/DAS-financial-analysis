CREATE KEYSPACE IF NOT EXISTS stock_data
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE stock_data;

CREATE TABLE IF NOT EXISTS historical_data (
    symbol text,
    timestamp timestamp,
    open double,
    high double,
    low double,
    close double,
    volume bigint,
    PRIMARY KEY ((symbol), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

CREATE TABLE IF NOT EXISTS forecast_data (
    symbol text,
    timestamp timestamp,
    predicted_price double,
    confidence_interval double,
    PRIMARY KEY ((symbol), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

CREATE TABLE IF NOT EXISTS technical_indicators (
    symbol text,
    timestamp timestamp,
    rsi double,
    macd double,
    macd_signal double,
    macd_hist double,
    sma_20 double,
    sma_50 double,
    sma_200 double,
    PRIMARY KEY ((symbol), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

CREATE TABLE IF NOT EXISTS sentiment_data (
    symbol text,
    timestamp timestamp,
    sentiment_score double,
    sentiment_label text,
    source text,
    PRIMARY KEY ((symbol), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

CREATE TABLE IF NOT EXISTS market_events (
    symbol text,
    timestamp timestamp,
    event_type text,
    description text,
    impact_score double,
    PRIMARY KEY ((symbol), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

CREATE TABLE IF NOT EXISTS symbol_metadata (
    symbol text PRIMARY KEY,
    company_name text,
    sector text,
    industry text,
    last_updated timestamp
); 