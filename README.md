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

* Add circuit-breaker and retries for Yahoo Finance API failures

* Add performance charts and dashboards

* Add structured logging and monitoring (Grafana)
