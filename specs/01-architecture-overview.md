# 01 - Architecture Overview

> System architecture for the Multi-Asset Market Data API

## System Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA PIPELINE (ETL)                                │
│  ┌────────────────────┐  ┌──────────────┐  ┌──────────────┐  ┌───────────┐ │
│  │   Data Sources      │─▶│  Ingestion   │─▶│  Transform   │─▶│  Parquet  │ │
│  │  Equities/FX/CEX/   │  │  Workers     │  │  & Validate  │  │  Storage  │ │
│  │  DEX/Bonds          │  │              │  │              │  │           │ │
│  └────────────────────┘  └──────────────┘  └──────────────┘  └─────┬─────┘ │
└────────────────────────────────────────────────────────────────────┼───────┘
                                                                     │
                                                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         HUGGINGFACE DATASETS                                 │
│  datasets/org/market-data/                                                   │
│  ├── data/                                                                   │
│  │   ├── equities_prices.parquet                                             │
│  │   ├── fx_rates_daily.parquet                                              │
│  │   ├── cex_ohlcv_daily.parquet                                             │
│  │   ├── cex_funding_rates.parquet                                           │
│  │   ├── dex_swaps.parquet                                                   │
│  │   ├── dex_pools.parquet                                                   │
│  │   ├── bond_yields_daily.parquet                                           │
│  │   └── bond_reference.parquet                                              │
│  └── spec.json  (update_time, version)                                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                                                     │
                                                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CLIENT LIBRARY                                       │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                   │
│  │  DuckDB +    │◀───│  SQL         │◀───│  Asset       │◀── User Code     │
│  │  cache_httpfs│    │  Templates   │    │  Classes     │                   │
│  └──────────────┘    └──────────────┘    └──────────────┘                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Component Responsibilities

### 1. Data Pipeline (ETL)

**Repository**: `defeatbeta-market-pipeline/`

| Component | Responsibility |
|-----------|---------------|
| **Ingestion Workers** | Fetch data from equity/FX/bond providers, CEX APIs, and DEX indexers |
| **Transform Layer** | Normalize across venues, validate quality, enforce schemas |
| **Storage Layer** | Write to parquet format with proper schemas and partitions |
| **Orchestration** | Schedule jobs, handle failures, backfill history |

### 2. HuggingFace Datasets

**Repository**: `datasets/org/market-data`

| Component | Responsibility |
|-----------|---------------|
| **Parquet Files** | Store normalized multi-asset market data |
| **spec.json** | Track update time, version, available tables, asset classes |
| **README** | Dataset documentation and usage |

### 3. Client Library

**Repository**: `defeatbeta-api/`

| Component | Responsibility |
|-----------|---------------|
| **DuckDB Client** | Execute SQL queries with caching |
| **HuggingFace Client** | Resolve dataset URLs, check updates |
| **Asset Classes** | Ticker (equities), FXPair, CryptoToken, DexPool, Bond |
| **SQL Templates** | Parameterized queries for data access |
| **Reports** | Generate tearsheets and visualizations |

## Data Flow

### Write Path (Pipeline → HuggingFace)

```
1. Scheduler triggers job (cron)
2. Fetcher pulls data from providers (equity/FX/bonds/CEX/DEX)
3. Normalizer standardizes schema and symbols
4. Validator checks data quality
5. Writer appends to parquet
6. Publisher uploads to HuggingFace
7. spec.json updated with new timestamp
```

### Read Path (Client → User)

```
1. User instantiates asset class (e.g., `Ticker("AAPL")`, `FXPair("EURUSD")`)
2. Asset class calls method (e.g., `.price()`, `.ohlcv()`)
3. HuggingFace client resolves parquet URL
4. SQL template loaded and parameterized
5. DuckDB executes query via cache_httpfs
6. Results returned as pandas DataFrame
```

## Design Principles

### From defeatbeta-api

1. **Singleton Pattern**: Single DuckDB connection shared across queries
2. **SQL Templates**: Parameterized queries in separate `.sql` files
3. **JSON Templates**: Configuration and schemas in `.json` files
4. **Visitor Pattern**: For statement/report generation
5. **Lazy Loading**: Data fetched on-demand, cached locally

### Multi-Asset Adaptations

1. **Multi-Venue**: Normalize across exchanges, pools, and venues
2. **Time Granularity**: Asset-specific intervals (FX daily, CEX intraday, DEX snapshots)
3. **Derivatives Data**: Funding rates, open interest, liquidations
4. **Market Calendars**: FX/bond holidays and trading calendars

## Technology Stack

| Layer | Technology | Rationale |
|-------|------------|-----------|
| Data Fetching | `ccxt`, `requests`, indexers | Multi-venue support |
| Data Processing | `pandas`, `pyarrow` | DataFrame operations, parquet I/O |
| Storage | Parquet on HuggingFace | Columnar, compressed, accessible |
| Query Engine | DuckDB + cache_httpfs | OLAP performance, HTTP caching |
| Scheduling | APScheduler / GitHub Actions | Flexible job orchestration |
| Client API | Python package | Easy installation via pip |

## Comparison with defeatbeta-api

| Aspect | defeatbeta-api (today) | multi-asset extension |
|--------|------------------------|-----------------------|
| Data Source | Yahoo Finance (equities) | Equities + FX + CEX/DEX + bonds |
| Asset Type | Stocks | Stocks, FX, crypto, bonds |
| Update Frequency | Daily | Asset-specific (daily/hourly) |
| Unique Data | Earnings calls, financials | Funding, pools, curves |
| Entry Point | `Ticker` class | Ticker, FXPair, CryptoToken, DexPool, Bond |
| Storage | HuggingFace parquet | HuggingFace parquet |
| Query Engine | DuckDB | DuckDB |

## Security Considerations

1. **API Keys**: Never stored in code, use environment variables
2. **Rate Limits**: Respect exchange limits, implement backoff
3. **Data Validation**: Verify data integrity before publishing
4. **No PII**: Only market data, no user information
