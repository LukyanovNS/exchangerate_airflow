clickhouse-client --query="CREATE DATABASE IF NOT EXISTS exchangerate_db"
clickhouse-client --database=exchangerate_db --query="CREATE TABLE IF NOT EXISTS exchangerate_landing (CreateDTime DateTime, RateDate date, response String) ENGINE=MergeTree() ORDER BY CreateDTime"
clickhouse-client --database=exchangerate_db --query="CREATE TABLE IF NOT EXISTS exchangerate_parsed (CreateDTime DateTime, RateDate date, CurrencyPair String, Rate float, INDEX IX_RateDate RateDate TYPE minmax GRANULARITY 8192) ENGINE=MergeTree() ORDER BY CreateDTime"
