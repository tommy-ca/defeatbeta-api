# 11 - Monitoring & Observability

> Metrics, dashboards, alerting, and operational visibility

## Observability Philosophy

1. **Measure Everything**: Collect metrics at every layer
2. **Alert on Symptoms**: Alert on user-facing issues, not internal details
3. **Debug with Logs**: Structured logging for troubleshooting
4. **Trace Requests**: Distributed tracing for pipeline flows
5. **Dashboards for Humans**: Actionable visualizations

---

## 1. Key Metrics

### Pipeline Health Metrics

| Metric | Type | Description | Alert Threshold |
|--------|------|-------------|-----------------|
| `pipeline.runs.total` | Counter | Total pipeline runs | - |
| `pipeline.runs.success` | Counter | Successful runs | - |
| `pipeline.runs.failed` | Counter | Failed runs | > 3/hour |
| `pipeline.runs.duration_seconds` | Histogram | Run duration | p95 > 300s |
| `pipeline.assets.materialized` | Counter | Assets materialized | - |
| `pipeline.assets.failed` | Counter | Asset failures | > 0 |

### Data Quality Metrics

| Metric | Type | Description | Alert Threshold |
|--------|------|-------------|-----------------|
| `data.rows.ingested` | Counter | Rows ingested per run | - |
| `data.rows.invalid` | Counter | Invalid rows removed | > 5% of total |
| `data.symbols.success` | Gauge | Symbols fetched successfully | < 90% |
| `data.symbols.failed` | Gauge | Symbols with errors | > 10% |
| `data.freshness_seconds` | Gauge | Time since last update | > 7200s (2h) |
| `data.completeness` | Gauge | % of expected data present | < 95% |

### API Client Metrics

| Metric | Type | Description | Alert Threshold |
|--------|------|-------------|-----------------|
| `api.requests.total` | Counter | Total API requests | - |
| `api.requests.success` | Counter | Successful requests | - |
| `api.requests.failed` | Counter | Failed requests | > 10% |
| `api.requests.rate_limited` | Counter | Rate limit hits | > 5/min |
| `api.latency_seconds` | Histogram | Request latency | p99 > 10s |
| `api.retries` | Counter | Retry attempts | - |

### HuggingFace Publishing Metrics

| Metric | Type | Description | Alert Threshold |
|--------|------|-------------|-----------------|
| `publish.uploads.total` | Counter | Total uploads | - |
| `publish.uploads.success` | Counter | Successful uploads | - |
| `publish.uploads.failed` | Counter | Failed uploads | > 0 |
| `publish.bytes_uploaded` | Counter | Bytes uploaded | - |
| `publish.duration_seconds` | Histogram | Upload duration | p95 > 600s |

---

## 2. Metrics Implementation

### Prometheus Metrics with Dagster

```python
# crypto_pipeline/utils/metrics.py
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, push_to_gateway
import time
from functools import wraps
from typing import Callable

REGISTRY = CollectorRegistry()

# Pipeline metrics
PIPELINE_RUNS = Counter(
    'pipeline_runs_total',
    'Total pipeline runs',
    ['status', 'job_name'],
    registry=REGISTRY
)

PIPELINE_DURATION = Histogram(
    'pipeline_run_duration_seconds',
    'Pipeline run duration',
    ['job_name'],
    buckets=[30, 60, 120, 300, 600, 1200, 3600],
    registry=REGISTRY
)

# Data metrics
DATA_ROWS_INGESTED = Counter(
    'data_rows_ingested_total',
    'Rows ingested',
    ['asset', 'exchange'],
    registry=REGISTRY
)

DATA_ROWS_INVALID = Counter(
    'data_rows_invalid_total',
    'Invalid rows removed',
    ['asset', 'reason'],
    registry=REGISTRY
)

SYMBOLS_STATUS = Gauge(
    'data_symbols_status',
    'Symbol fetch status',
    ['status'],
    registry=REGISTRY
)

DATA_FRESHNESS = Gauge(
    'data_freshness_seconds',
    'Seconds since last data update',
    ['table'],
    registry=REGISTRY
)

# API metrics
API_REQUESTS = Counter(
    'api_requests_total',
    'API requests',
    ['exchange', 'endpoint', 'status'],
    registry=REGISTRY
)

API_LATENCY = Histogram(
    'api_request_latency_seconds',
    'API request latency',
    ['exchange', 'endpoint'],
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30],
    registry=REGISTRY
)


def track_duration(metric: Histogram, labels: dict = None):
    """Decorator to track function duration."""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                duration = time.perf_counter() - start
                if labels:
                    metric.labels(**labels).observe(duration)
                else:
                    metric.observe(duration)
        return wrapper
    return decorator


def push_metrics(gateway: str = "localhost:9091", job: str = "crypto_pipeline"):
    """Push metrics to Prometheus Pushgateway."""
    push_to_gateway(gateway, job=job, registry=REGISTRY)
```

### Dagster Asset Metrics

```python
# crypto_pipeline/assets/bronze/ohlcv.py
from dagster import asset, AssetExecutionContext, Output
from crypto_pipeline.utils.metrics import (
    DATA_ROWS_INGESTED,
    DATA_ROWS_INVALID,
    SYMBOLS_STATUS,
    API_REQUESTS,
)


@asset(partitions_def=daily_partitions, group_name="bronze")
def bronze_binance_ohlcv(
    context: AssetExecutionContext,
    binance_client: BinanceClient,
) -> Output[pd.DataFrame]:
    """Fetch OHLCV with metrics tracking."""
    
    results = []
    success_count = 0
    failed_count = 0
    
    for symbol in SYMBOLS:
        try:
            df = binance_client.fetch_ohlcv_sync(symbol=symbol, ...)
            results.append(df)
            success_count += 1
            
            # Track rows ingested
            DATA_ROWS_INGESTED.labels(
                asset="bronze_ohlcv",
                exchange="binance"
            ).inc(len(df))
            
            # Track API success
            API_REQUESTS.labels(
                exchange="binance",
                endpoint="klines",
                status="success"
            ).inc()
            
        except Exception as e:
            failed_count += 1
            API_REQUESTS.labels(
                exchange="binance",
                endpoint="klines",
                status="error"
            ).inc()
    
    # Update symbol status gauge
    SYMBOLS_STATUS.labels(status="success").set(success_count)
    SYMBOLS_STATUS.labels(status="failed").set(failed_count)
    
    combined = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
    
    return Output(
        combined,
        metadata={
            "row_count": len(combined),
            "symbols_success": success_count,
            "symbols_failed": failed_count,
        },
    )
```

---

## 3. Structured Logging

### Log Format

```python
# crypto_pipeline/utils/logging.py
import logging
import json
import sys
from datetime import datetime
from typing import Any, Dict


class StructuredFormatter(logging.Formatter):
    """JSON structured log formatter."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add extra fields
        if hasattr(record, "extra"):
            log_data.update(record.extra)
        
        # Add exception info
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)


def setup_logging(level: str = "INFO", json_format: bool = True):
    """Configure structured logging."""
    root = logging.getLogger()
    root.setLevel(level)
    
    handler = logging.StreamHandler(sys.stdout)
    
    if json_format:
        handler.setFormatter(StructuredFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s %(name)s - %(message)s'
        ))
    
    root.addHandler(handler)


class LogContext:
    """Context manager for adding fields to logs."""
    
    def __init__(self, **kwargs):
        self.extra = kwargs
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        pass
    
    def info(self, msg: str, **kwargs):
        logging.info(msg, extra={"extra": {**self.extra, **kwargs}})
    
    def error(self, msg: str, **kwargs):
        logging.error(msg, extra={"extra": {**self.extra, **kwargs}})
```

### Usage in Pipeline

```python
from crypto_pipeline.utils.logging import LogContext

def fetch_symbol_data(symbol: str, exchange: str):
    with LogContext(symbol=symbol, exchange=exchange) as log:
        log.info("Starting data fetch")
        
        try:
            data = client.fetch(symbol)
            log.info("Fetch complete", rows=len(data))
            return data
        except Exception as e:
            log.error("Fetch failed", error=str(e))
            raise
```

### Log Output Example

```json
{
  "timestamp": "2024-01-15T10:30:00.000Z",
  "level": "INFO",
  "logger": "crypto_pipeline.assets.bronze",
  "message": "Fetch complete",
  "module": "ohlcv",
  "function": "fetch_symbol_data",
  "line": 45,
  "symbol": "BTCUSDT",
  "exchange": "binance",
  "rows": 365
}
```

---

## 4. Dashboards

### Grafana Dashboard Panels

#### Pipeline Overview

```yaml
# grafana/dashboards/pipeline-overview.json
panels:
  - title: "Pipeline Success Rate (24h)"
    type: stat
    query: |
      sum(rate(pipeline_runs_total{status="success"}[24h])) /
      sum(rate(pipeline_runs_total[24h])) * 100
    thresholds:
      - value: 95
        color: green
      - value: 80
        color: yellow
      - value: 0
        color: red

  - title: "Pipeline Run Duration"
    type: graph
    query: |
      histogram_quantile(0.95, 
        rate(pipeline_run_duration_seconds_bucket[1h])
      )

  - title: "Assets Materialized"
    type: timeseries
    query: |
      sum(rate(pipeline_assets_materialized_total[1h])) by (asset)
```

#### Data Quality

```yaml
panels:
  - title: "Data Freshness"
    type: gauge
    query: data_freshness_seconds
    thresholds:
      - value: 3600   # 1h - green
      - value: 7200   # 2h - yellow
      - value: 14400  # 4h - red

  - title: "Invalid Rows Rate"
    type: timeseries
    query: |
      sum(rate(data_rows_invalid_total[1h])) /
      sum(rate(data_rows_ingested_total[1h])) * 100

  - title: "Symbol Success Rate"
    type: stat
    query: |
      data_symbols_status{status="success"} /
      (data_symbols_status{status="success"} + 
       data_symbols_status{status="failed"}) * 100
```

#### API Performance

```yaml
panels:
  - title: "API Latency (p95)"
    type: timeseries
    query: |
      histogram_quantile(0.95,
        rate(api_request_latency_seconds_bucket[5m])
      ) by (exchange)

  - title: "Rate Limit Events"
    type: timeseries
    query: |
      sum(rate(api_requests_total{status="rate_limited"}[5m])) by (exchange)

  - title: "API Error Rate"
    type: stat
    query: |
      sum(rate(api_requests_total{status="error"}[1h])) /
      sum(rate(api_requests_total[1h])) * 100
```

---

## 5. Alerting Rules

### Prometheus Alert Rules

```yaml
# prometheus/alerts/pipeline.yml
groups:
  - name: pipeline_alerts
    rules:
      - alert: PipelineHighFailureRate
        expr: |
          sum(rate(pipeline_runs_total{status="failed"}[1h])) /
          sum(rate(pipeline_runs_total[1h])) > 0.1
        for: 15m
        labels:
          severity: critical
        annotations:
          summary: "Pipeline failure rate > 10%"
          description: "{{ $value | humanizePercentage }} of pipeline runs failing"

      - alert: PipelineSlowRuns
        expr: |
          histogram_quantile(0.95, rate(pipeline_run_duration_seconds_bucket[1h])) > 600
        for: 30m
        labels:
          severity: warning
        annotations:
          summary: "Pipeline runs taking > 10 minutes (p95)"

      - alert: DataStale
        expr: data_freshness_seconds > 7200
        for: 30m
        labels:
          severity: critical
        annotations:
          summary: "Data not updated for > 2 hours"
          description: "Table {{ $labels.table }} last updated {{ $value | humanizeDuration }} ago"

      - alert: HighInvalidRowRate
        expr: |
          sum(rate(data_rows_invalid_total[1h])) /
          sum(rate(data_rows_ingested_total[1h])) > 0.05
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "Invalid row rate > 5%"

      - alert: APIHighErrorRate
        expr: |
          sum(rate(api_requests_total{status="error"}[5m])) by (exchange) /
          sum(rate(api_requests_total[5m])) by (exchange) > 0.1
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "API error rate > 10% for {{ $labels.exchange }}"

      - alert: APIRateLimited
        expr: |
          sum(rate(api_requests_total{status="rate_limited"}[5m])) by (exchange) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Rate limiting detected for {{ $labels.exchange }}"
```

### Alert Routing

```yaml
# alertmanager/config.yml
route:
  receiver: default
  group_by: [alertname, severity]
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  
  routes:
    - match:
        severity: critical
      receiver: pagerduty
      continue: true
    
    - match:
        severity: critical
      receiver: slack-critical
    
    - match:
        severity: warning
      receiver: slack-warnings

receivers:
  - name: default
    slack_configs:
      - channel: '#crypto-pipeline-alerts'

  - name: slack-critical
    slack_configs:
      - channel: '#crypto-pipeline-critical'
        send_resolved: true

  - name: slack-warnings
    slack_configs:
      - channel: '#crypto-pipeline-warnings'

  - name: pagerduty
    pagerduty_configs:
      - routing_key: ${PAGERDUTY_KEY}
        severity: critical
```

---

## 6. Health Checks

### Endpoint Implementation

```python
# crypto_pipeline/health.py
from fastapi import FastAPI, Response
from datetime import datetime, timedelta
import httpx

app = FastAPI()


@app.get("/health")
async def health_check():
    """Basic health check."""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


@app.get("/health/ready")
async def readiness_check():
    """Readiness check - verify dependencies."""
    checks = {}
    
    # Check HuggingFace connectivity
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://huggingface.co/api/datasets/yourorg/crypto-cex-data",
                timeout=5
            )
            checks["huggingface"] = resp.status_code == 200
    except Exception:
        checks["huggingface"] = False
    
    # Check data freshness
    try:
        freshness = get_data_freshness()
        checks["data_fresh"] = freshness < timedelta(hours=2)
    except Exception:
        checks["data_fresh"] = False
    
    all_healthy = all(checks.values())
    
    return Response(
        content={"status": "ready" if all_healthy else "not_ready", "checks": checks},
        status_code=200 if all_healthy else 503
    )


@app.get("/health/live")
async def liveness_check():
    """Liveness check - is the process alive."""
    return {"status": "alive"}
```

---

## 7. Runbook Integration

### Alert Runbooks

| Alert | Runbook |
|-------|---------|
| `PipelineHighFailureRate` | Check Dagster UI for failed runs, review logs for error patterns |
| `DataStale` | Verify exchange APIs are accessible, check for rate limiting |
| `HighInvalidRowRate` | Review data validation logs, check for schema changes |
| `APIRateLimited` | Reduce request rate, verify API key limits |

### Quick Commands

```bash
# Check pipeline status
dagster job list --status FAILURE

# View recent logs
kubectl logs -l app=crypto-pipeline --tail=100

# Check data freshness
duckdb -c "SELECT MAX(timestamp) FROM 'https://...'"

# Force backfill
dagster job backfill --job daily_ingestion --from 2024-01-01
```

---

## Monitoring Checklist

### Setup

- [ ] Deploy Prometheus and Grafana
- [ ] Configure metrics collection
- [ ] Import dashboards
- [ ] Set up alert rules
- [ ] Configure alert routing (Slack, PagerDuty)
- [ ] Create runbooks for each alert

### Operations

- [ ] Review dashboards daily
- [ ] Investigate alerts within SLA
- [ ] Update runbooks with learnings
- [ ] Tune alert thresholds quarterly
- [ ] Archive resolved incidents
