# Equal-Weighted Top-100 US Stock Index Backend

This project builds and maintains an equal-weighted Top-100 US stock index using FastAPI, DuckDB, Redis, Docker, and a cron-based nightly batch job.

It supports historical index construction, performance analytics, composition change tracking, and multi-sheet Excel export.

### Tech Stack

| Layer            | Technology                   |
| ---------------- | ---------------------------- |
| API              | FastAPI                      |
| Storage          | DuckDB                       |
| Cache            | Redis                        |
| Data Source      | Yahoo Finance (`yfinance`)   |
| Batch Scheduling | Linux `cron` (inside Docker) |
| Containerization | Docker + Docker Compose      |

### Data Source Evaluation

Two market data providers were evaluated before selecting the final ingestion source.

| Feature                     | Yahoo Finance (`yfinance`)        | Polygon.io                           |
| --------------------------- | ---------------------------       | -------------------------------      |
| API Cost                    | Free                              | Free tier severely rate-limited      |
| Rate Limits                 | No enforced public limits         | 5 requests / minute                  |
| Batch Download Support      | Yes (multi-ticker download)       | No (per-ticker endpoints)            |
| Historical Coverage         | Yes                               | Yes                                  |
| Market Cap Availability     | Available (no historical support) | Available (supports historical data) |
| Authentication              | Not required                      | API key required                |
| Ease of Integration         | Very easy                         | Moderate                        |
| Reliability for Daily Batch | High                              | Limited by throttling           |


Polygon provides better long-term maintenance, structured endpoints and enhanced features. However, the free tier rate limits (5 requests per minute) make it impractical to fetch data for 100 stocks daily without upgrading to a paid plan.
Yahoo Finance supports batch downloads for multiple tickers in a single request and does not enforce hard public rate limits, making it more suitable for daily index rebalancing workloads at zero cost.
Therefore, Yahoo Finance was selected as the ingestion source for this project.

### Project Structure
```text
app/
 ├── database/
 │   ├── db_manipulation.py
 │   ├── redis_client.py
 │   └── index_data.duckdb
 ├── market_data/
 │   ├── daily_market_price_batch.py
 │   └── yfinance_api.py
 └── services/
     └── index.py        # FastAPI application
docker/
 ├── Dockerfile
 └── docker-compose.yml
requirements.txt
start.sh
README.md
```

___
## Setup Instructions

#### Prerequisites

- Docker

- Git

#### Run Using Docker (Recommended)

```
cd docker
docker compose up --build
```

#### FastAPI will be available at: http://localhost:8000/docs
#### Redis and DuckDB are started automatically.
### Nightly Data Acquisition Job
```
A cron job runs daily at 2:30 AM IST inside Docker to fetch US market close prices.
```
### Manually trigger the batch job
```
docker exec -it docker-app-1 python -m app.market_data.daily_market_price_batch
```

### Verify cron is active
```
docker exec -it docker-app-1 crontab -l
```

---
## API Endpoints
### Build Index
```
POST /build_index?start_date=2025-12-01&end_date=2025-12-20
```

### Index Performance
```
GET /index_performance?start_date=2025-12-01&end_date=2025-12-20
```

### Index Composition for Date
```
GET /index_composition?date=2025-12-20
```

### Composition Changes
```
GET /composition_changes?start_date=2025-12-01&end_date=2025-12-20
```

### Export Data (Excel)
```POST /export-data?start_date=2025-12-01&end_date=2025-12-20```

This downloads a single Excel file with multiple sheets:

##### Sheet Name
```
Index_Performance
Composition_Changes
Daily_Compositions
```
## Database Schema Overview
```
price
-------------------
Column	Type
date	DATE
ticker	STRING
close_price	DOUBLE
market_cap	DOUBLE

index_composition
-------------------
Column	Type
date	DATE
ticker	STRING
weight	DOUBLE

index_performance
------------------
Column	Type
date	DATE
daily_return	DOUBLE
cumulative_return	DOUBLE
```

### Production / Scaling Improvements

- This project currently uses Yahoo Finance for market data (see *Data Source Evaluation* section). Yahoo does not provide historical outstanding share counts, so the latest available share count is stored as metadata and reused for historical market cap computation. Since outstanding shares change infrequently, this approximation is acceptable for this assignment. In a production system, this can be enhanced by periodically refreshing share counts or using a paid data provider for accurate historical values.

- Introduce circuit breakers and automatic retries around external API calls to handle transient failures.

- The current cron-based batch job is sufficient for daily ingestion. If ingestion frequency or data volume increases significantly, this can be extended into a distributed scheduling system while keeping the same ingestion logic.

- Add structured logging and system metrics to improve observability and failure diagnosis (like Grafana or Prometheus).

- Add visualization support (charts and dashboards) for tracking index performance trends.

