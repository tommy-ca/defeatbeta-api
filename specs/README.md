# Multi-Asset Market Data API - Specifications

> Specs-Driven Development documentation for extending [defeatbeta-api](../README.md) into a multi-asset market data library covering equities, forex, crypto (CEX/DEX), and bonds.

## Overview

This project adapts the architecture and patterns of `defeatbeta-api` to support multiple asset classes with consistent data access patterns and schemas.

## Specifications Index

| Document | Description | Status |
|----------|-------------|--------|
| [00 - Requirements](./00-requirements.md) | Product and data requirements for multi-asset coverage | **New** |
| [01 - Architecture Overview](./01-architecture-overview.md) | System architecture, components, data flow | Updated |
| [02 - Data Pipeline](./02-data-pipeline.md) | **Dagster-based** batch pipelines (Bronze/Silver/Gold) | **Updated** |
| [03 - HuggingFace Publishing](./03-huggingface-publishing.md) | Dataset publishing workflow | Updated |
| [04 - Client Library](./04-client-library.md) | Consumer library design and API | Updated |
| [05 - Data Schemas](./05-data-schemas.md) | Parquet schemas, SQL templates | Updated |
| [06 - Implementation Roadmap](./06-implementation-roadmap.md) | **Dagster-based** phased delivery plan | Updated |
| [07 - Testing Strategy](./07-testing-strategy.md) | Unit/integration tests, mocking, CI/CD | Updated |
| [08 - Configuration](./08-configuration.md) | Environment variables, secrets, multi-env setup | Updated |
| [09 - Error Handling](./09-error-handling.md) | API failures, recovery, alerting | Updated |
| [10 - Security](./10-security.md) | API keys, secrets rotation, access controls | Updated |
| [11 - Monitoring](./11-monitoring.md) | Metrics, dashboards, alerting, observability | Updated |

## Quick Links

- **Reference Implementation**: [defeatbeta-api](https://github.com/defeat-beta/defeatbeta-api)
- **Target Datasets**: HuggingFace Datasets (multi-table, multi-asset)
- **Primary Sources**: Yahoo Finance (equities), FX providers, CEXs, DEX indexers, Treasury/bond sources

## Project Goals

1. **High-Performance Data Access**: DuckDB + cache_httpfs for sub-second queries
2. **Reliable Data Source**: Pre-processed parquet files on HuggingFace (no rate limits)
3. **Multi-Asset Coverage**: Equities, FX, crypto (CEX/DEX), bonds
4. **Normalized Schemas**: Consistent tables and symbols across venues
5. **Familiar API**: Similar patterns to defeatbeta-api for easy adoption

## Technology Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Orchestration** | Dagster | Asset-centric batch pipelines |
| **Data Models** | Pydantic | Type-safe validation |
| **HTTP Client** | httpx + tenacity | Async requests with retries |
| **Storage** | PyArrow/Parquet | Columnar data format |
| **Publishing** | HuggingFace Hub | Dataset hosting |
| **Query Engine** | DuckDB + cache_httpfs | OLAP queries over HTTP |
| **Client API** | Python package | User-facing library |

## Architecture Summary

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Data Sources   │────▶│  Data Pipeline  │────▶│   HuggingFace   │
│  (Equity/FX/    │     │  (ETL Jobs)     │     │   Datasets      │
│   CEX/DEX/Bond) │     └─────────────────┘     └────────┬────────┘
                                                         │
                                                         ▼
                        ┌─────────────────┐     ┌─────────────────┐
                        │   User Code     │◀────│  Client Library │
                        │                 │     │  (DuckDB+httpfs)│
                        └─────────────────┘     └─────────────────┘
```

## Getting Started

1. Read [Architecture Overview](./01-architecture-overview.md) for system understanding
2. Review [Data Schemas](./05-data-schemas.md) for data structures
3. Check [Implementation Roadmap](./06-implementation-roadmap.md) for development phases

## Contributing

When adding or modifying specs:
1. Update this index if adding new documents
2. Mark document status (Draft/Review/Approved)
3. Include code examples where applicable
4. Reference the original defeatbeta-api patterns being adapted
