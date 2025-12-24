# 03 - HuggingFace Publishing

> Dataset publishing workflow and HuggingFace integration for multi-asset tables

## Dataset Structure

```
datasets/yourorg/market-data/
├── README.md                    # Dataset card (auto-generated)
├── spec.json                    # Metadata and version info
└── data/
    ├── equities_prices.parquet      # Daily equities OHLCV
    ├── fx_rates_daily.parquet       # Daily FX spot rates
    ├── cex_ohlcv_daily.parquet      # Daily crypto OHLCV
    ├── cex_funding_rates.parquet    # Funding rate history
    ├── cex_open_interest.parquet    # Open interest snapshots
    ├── dex_swaps.parquet            # DEX swap events
    ├── dex_pools.parquet            # DEX pool metadata
    ├── bond_yields_daily.parquet    # Yield curve snapshots
    └── bond_reference.parquet       # Bond reference data
```

## spec.json Schema

```json
{
  "update_time": "2025-12-07",
  "version": "1.0.0",
  "pipeline_version": "0.1.0",
  "asset_classes": ["equities", "fx", "crypto_cex", "crypto_dex", "bonds"],
  "tables": [
    {
      "name": "equities_prices",
      "description": "Daily equities OHLCV",
      "asset_class": "equities",
      "row_count": 5000000,
      "size_bytes": 524288000,
      "date_range": {
        "start": "2020-01-01",
        "end": "2025-12-06"
      }
    },
    {
      "name": "fx_rates_daily",
      "description": "Daily FX spot rates",
      "asset_class": "fx",
      "row_count": 1000000,
      "size_bytes": 104857600,
      "date_range": {
        "start": "2020-01-01",
        "end": "2025-12-06"
      }
    },
    {
      "name": "cex_funding_rates",
      "description": "Perpetual futures funding rates",
      "asset_class": "crypto_cex",
      "row_count": 1000000,
      "size_bytes": 104857600,
      "date_range": {
        "start": "2021-01-01",
        "end": "2025-12-06"
      }
    }
  ],
  "venues": ["binance", "coinbase", "okx", "bybit"],
  "chains": ["ethereum", "arbitrum", "base"],
  "symbols_count": 500,
  "symbols_sample": ["AAPL", "EURUSD", "BTCUSDT"],
  "intervals": {
    "equities_prices": "1d",
    "fx_rates_daily": "1d",
    "cex_ohlcv_daily": "1d"
  },
  "data_quality": {
    "validation_passed": true,
    "missing_data_pct": 0.02,
    "last_validation": "2025-12-07T01:30:00Z"
  }
}
```

## Publisher Implementation

```python
from huggingface_hub import HfApi, upload_file, create_repo
from pathlib import Path
from datetime import datetime
import json
import os

class HuggingFacePublisher:
    """Publish parquet datasets to HuggingFace"""
    
    def __init__(self, repo_id: str, token: str = None):
        """
        Args:
            repo_id: HuggingFace repo (e.g., 'yourorg/market-data')
            token: HuggingFace API token (or from HF_TOKEN env var)
        """
        self.repo_id = repo_id
        self.token = token or os.environ.get("HF_TOKEN")
        self.api = HfApi(token=self.token)
        
    def ensure_repo_exists(self):
        """Create dataset repo if it doesn't exist"""
        try:
            self.api.repo_info(repo_id=self.repo_id, repo_type="dataset")
        except Exception:
            create_repo(
                repo_id=self.repo_id,
                repo_type="dataset",
                private=False,
                token=self.token
            )
    
    def publish_dataset(self, parquet_files: dict, 
                       pipeline_version: str = "0.1.0"):
        """Upload all parquet files to HuggingFace
        
        Args:
            parquet_files: Dict of {table_name: Path} to upload
            pipeline_version: Version of the pipeline that generated data
        """
        self.ensure_repo_exists()
        
        table_metadata = []
        
        for table_name, file_path in parquet_files.items():
            file_path = Path(file_path)
            
            if not file_path.exists():
                raise FileNotFoundError(f"Parquet file not found: {file_path}")
            
            # Upload parquet file
            upload_file(
                path_or_fileobj=str(file_path),
                path_in_repo=f"data/{table_name}.parquet",
                repo_id=self.repo_id,
                repo_type="dataset",
                token=self.token
            )
            
            # Collect metadata
            stats = self._get_parquet_stats(file_path)
            table_metadata.append({
                "name": table_name,
                **stats
            })
        
        # Generate and upload spec.json
        spec = self._generate_spec(table_metadata, pipeline_version)
        self._upload_spec(spec)
        
        # Generate and upload README
        readme = self._generate_readme(spec)
        self._upload_readme(readme)
    
    def publish_single_table(self, table_name: str, file_path: Path):
        """Upload a single table (for incremental updates)"""
        upload_file(
            path_or_fileobj=str(file_path),
            path_in_repo=f"data/{table_name}.parquet",
            repo_id=self.repo_id,
            repo_type="dataset",
            token=self.token
        )
    
    def _get_parquet_stats(self, file_path: Path) -> dict:
        """Extract statistics from parquet file"""
        import pyarrow.parquet as pq
        
        metadata = pq.read_metadata(file_path)
        table = pq.read_table(file_path, columns=['timestamp'])
        
        timestamps = table['timestamp'].to_pandas()
        
        return {
            "row_count": metadata.num_rows,
            "size_bytes": file_path.stat().st_size,
            "date_range": {
                "start": timestamps.min().strftime("%Y-%m-%d"),
                "end": timestamps.max().strftime("%Y-%m-%d")
            }
        }
    
    def _generate_spec(self, table_metadata: list, 
                       pipeline_version: str) -> dict:
        """Generate spec.json content"""
        
        # Extract unique venues and symbols from data
        venues = self._extract_venues()
        symbols = self._extract_symbols()
        
        return {
            "update_time": datetime.utcnow().strftime("%Y-%m-%d"),
            "version": "1.0.0",
            "pipeline_version": pipeline_version,
            "tables": table_metadata,
            "venues": venues,
            "symbols_count": len(symbols),
            "symbols_sample": symbols[:10],
            "intervals": {
                "equities_prices": "1d",
                "fx_rates_daily": "1d",
                "cex_ohlcv_daily": "1d"
            },
            "data_quality": {
                "validation_passed": True,
                "last_validation": datetime.utcnow().isoformat() + "Z"
            }
        }
    
    def _upload_spec(self, spec: dict):
        """Upload spec.json"""
        spec_content = json.dumps(spec, indent=2).encode('utf-8')
        
        upload_file(
            path_or_fileobj=spec_content,
            path_in_repo="spec.json",
            repo_id=self.repo_id,
            repo_type="dataset",
            token=self.token
        )
    
    def _generate_readme(self, spec: dict) -> str:
        """Generate dataset README/card"""
        return f"""---
license: apache-2.0
task_categories:
  - time-series-forecasting
language:
  - en
tags:
  - market-data
  - equities
  - forex
  - cryptocurrency
  - bonds
size_categories:
  - 1M<n<10M
---

# Multi-Asset Market Data

High-quality market data across equities, FX, crypto (CEX/DEX), and bonds.

## Overview

- **Update Frequency**: Daily
- **Last Updated**: {spec['update_time']}
- **Venues**: {', '.join(spec.get('venues', []))}
- **Symbols**: {spec['symbols_count']} instruments/pairs

## Available Tables

| Table | Description | Rows | Size |
|-------|-------------|------|------|
{self._format_table_rows(spec['tables'])}

## Usage

```python
import duckdb

# Query directly from HuggingFace
conn = duckdb.connect()
conn.execute("INSTALL httpfs; LOAD httpfs;")

df = conn.execute(\"\"\"
    SELECT * FROM 'https://huggingface.co/datasets/{self.repo_id}/resolve/main/data/fx_rates_daily.parquet'
    WHERE base = 'EUR' AND quote = 'USD'
    ORDER BY timestamp DESC
    LIMIT 100
\"\"\").df()
```

## With defeatbeta-api

```python
from defeatbeta_api import Ticker
from defeatbeta_api.data.fx import FXPair
from defeatbeta_api.data.crypto import CryptoToken
from defeatbeta_api.data.bond import Bond

aapl = Ticker("AAPL")
aapl.price()

eurusd = FXPair("EURUSD")
eurusd.rate()

btc = CryptoToken("BTC")
btc.ohlcv(interval="1d", limit=365)

ust = Bond("US10Y")
ust.yield_curve()
```

## Data Sources

Data is collected from official and reputable sources:
- Equities data providers (Yahoo Finance or equivalents)
- FX rate providers (central banks or aggregators)
- CEX APIs (Binance, Coinbase, OKX, Bybit)
- DEX indexers (The Graph or chain indexers)
- Bonds sources (Treasury yield data, bond reference data)

## License

Apache 2.0
"""
    
    def _format_table_rows(self, tables: list) -> str:
        """Format table metadata for README"""
        rows = []
        for t in tables:
            size_mb = t['size_bytes'] / (1024 * 1024)
            rows.append(
                f"| {t['name']} | {t.get('description', '')} | "
                f"{t['row_count']:,} | {size_mb:.1f} MB |"
            )
        return '\n'.join(rows)
    
    def _upload_readme(self, readme: str):
        """Upload README.md"""
        upload_file(
            path_or_fileobj=readme.encode('utf-8'),
            path_in_repo="README.md",
            repo_id=self.repo_id,
            repo_type="dataset",
            token=self.token
        )
    
    def _extract_venues(self) -> list:
        """Extract unique venues from uploaded data"""
        # Would query the parquet files
        return ["binance", "coinbase", "okx", "bybit"]
    
    def _extract_symbols(self) -> list:
        """Extract unique symbols from uploaded data"""
        # Would query the parquet files
        return ["AAPL", "EURUSD", "BTCUSDT", "US10Y", "ETHUSDC"]
```

## CI/CD Integration

### GitHub Actions Workflow

```yaml
# .github/workflows/daily-pipeline.yml
name: Daily Data Pipeline

on:
  schedule:
    - cron: '0 1 * * *'  # 1 AM UTC daily
  workflow_dispatch:      # Manual trigger

env:
  HF_TOKEN: ${{ secrets.HF_TOKEN }}
  HF_REPO_ID: yourorg/market-data

jobs:
  run-pipeline:
    runs-on: ubuntu-latest
    timeout-minutes: 120
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          
      - name: Install dependencies
        run: |
          pip install -e ".[dev]"
          
      - name: Run daily multi-asset pipeline
        run: python scripts/run_daily_pipeline.py
        
      - name: Validate data quality
        run: python scripts/validate_data.py
        
      - name: Publish to HuggingFace
        run: python scripts/publish_to_huggingface.py
        
      - name: Notify on failure
        if: failure()
        uses: actions/github-script@v7
        with:
          script: |
            github.rest.issues.create({
              owner: context.repo.owner,
              repo: context.repo.repo,
              title: 'Daily pipeline failed',
              body: 'Check workflow run: ${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}'
            })
```

### Publish Script

```python
#!/usr/bin/env python3
"""scripts/publish_to_huggingface.py"""

import os
from pathlib import Path
from publish import HuggingFacePublisher

def main():
    publisher = HuggingFacePublisher(
        repo_id=os.environ["HF_REPO_ID"],
        token=os.environ["HF_TOKEN"]
    )
    
    data_dir = Path("./data")
    
    parquet_files = {
        'equities_prices': data_dir / 'equities_prices.parquet',
        'fx_rates_daily': data_dir / 'fx_rates_daily.parquet',
        'cex_ohlcv_daily': data_dir / 'cex_ohlcv_daily.parquet',
        'cex_funding_rates': data_dir / 'cex_funding_rates.parquet',
        'dex_swaps': data_dir / 'dex_swaps.parquet',
        'bond_yields_daily': data_dir / 'bond_yields_daily.parquet',
        'bond_reference': data_dir / 'bond_reference.parquet',
    }
    
    # Filter to only existing files
    existing_files = {k: v for k, v in parquet_files.items() if v.exists()}
    
    if not existing_files:
        raise RuntimeError("No parquet files found to publish")
    
    publisher.publish_dataset(
        parquet_files=existing_files,
        pipeline_version="0.1.0"
    )
    
    print(f"Published {len(existing_files)} tables to HuggingFace")

if __name__ == "__main__":
    main()
```

## Cache Invalidation

The client library uses `cache_httpfs` to cache parquet files locally. When new data is published:

1. **spec.json** is updated with new `update_time`
2. Client checks `update_time` on initialization
3. If remote `update_time` > cached `update_time`, cache is cleared

```python
# In client library
def _validate_httpfs_cache(self):
    """Clear cache if remote data is newer"""
    try:
        current_spec = self.query(
            f"SELECT * FROM '{self.base_url}/resolve/main/spec.json'"
        )
        current_update_time = current_spec['update_time'].iloc[0]
        
        if current_update_time != self.cached_update_time:
            self.query("SELECT cache_httpfs_clear_cache()")
            self.cached_update_time = current_update_time
    except Exception as e:
        self.logger.warning(f"Cache validation failed: {e}")
```

## Versioning Strategy

### Data Versioning

- **Daily updates**: Append new data, no version bump
- **Schema changes**: Bump minor version (1.0 → 1.1)
- **Breaking changes**: Bump major version (1.x → 2.0)

### Backward Compatibility

For schema changes, maintain both versions temporarily:

```
data/
├── equities_prices.parquet      # Current version
└── v1/
    └── equities_prices.parquet  # Previous version (deprecated)
```

## Monitoring

### Data Freshness Check

```python
def check_data_freshness():
    """Alert if data is stale"""
    import requests
    from datetime import datetime, timedelta
    
    spec_url = "https://huggingface.co/datasets/yourorg/market-data/resolve/main/spec.json"
    spec = requests.get(spec_url).json()
    
    update_time = datetime.strptime(spec['update_time'], "%Y-%m-%d")
    max_staleness = timedelta(days=2)
    
    if datetime.utcnow() - update_time > max_staleness:
        raise AlertException(f"Data is stale: last update {spec['update_time']}")
```
