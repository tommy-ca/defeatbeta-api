# 00 - Requirements

> Product, data, and quality requirements for a multi-asset market data API

## Objectives

1. Extend the library beyond equities to include:
   - Forex spot rates (major/minor/exotic pairs)
   - Crypto markets (CEX spot/derivatives and DEX on-chain activity)
   - Bonds (sovereign curves and selected corporate issues)
2. Preserve the existing defeatbeta-api design patterns (DuckDB + cache_httpfs, SQL templates, HuggingFace datasets).
3. Offer a consistent, Pythonic API across asset classes with predictable schemas and naming.

## In Scope

- **Equities**: Continue Yahoo Finance-derived datasets and existing statements/ratios.
- **Forex**: Spot rates with standardized base/quote pairs and consistent timestamping.
- **Crypto CEX**: OHLCV, funding, open interest, liquidations, exchange metadata.
- **Crypto DEX**: Pool metadata, swap events, volume and liquidity snapshots.
- **Bonds**: Yield curves (tenor grids), bond metadata, and pricing/yield history where available.
- **Data publishing**: Multi-table datasets on HuggingFace with a shared spec.json.
- **Client library**: New asset entry points that map to the data tables.

## Non-Goals

- Tick-by-tick real-time feeds or sub-second streaming.
- Execution, trading, or order management features.
- Guaranteed completeness for all venues and instruments (coverage is curated).

## Data Quality Requirements

- **Schema stability**: Backward-compatible changes only; breaking changes versioned.
- **Normalization**:
  - Timestamps in UTC.
  - Consistent symbol formatting per asset class.
  - Explicit venue/source columns for provenance.
- **Validation**:
  - OHLC integrity (high >= open/close >= low).
  - No negative volumes or yields where invalid.
  - DEX swap direction and token decimals normalized.
- **Freshness**:
  - Equities: daily/weekly updates (as today).
  - FX: daily, with clear holiday handling.
  - CEX: hourly/daily, depending on table.
  - DEX: hourly/daily snapshots.
  - Bonds: daily (trading day) updates for curves/prices.

## API Requirements

- Consistent method naming across assets (e.g., `.price()`, `.ohlcv()`, `.yield_curve()`).
- Asset-specific classes (Ticker, FXPair, CryptoToken, DexPool, Bond).
- SQL template-backed queries to keep logic transparent and customizable.

## Compliance and Risk

- Respect provider terms of service and rate limits.
- Avoid PII and user data collection.
- Clearly document coverage gaps and known limitations.
