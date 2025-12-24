# FX Examples

Examples for FX spot rate access. This section aligns with the multi-asset specs in `/specs`.

## 1. FX Spot Rate (Single Pair)

```python
from defeatbeta_api.data.fx import FXPair

eurusd = FXPair("EURUSD")
eurusd.rate()
```

## 2. FX History (Date Range)

```python
from defeatbeta_api.data.fx import FXPair

usdjpy = FXPair("USDJPY")
usdjpy.rate(limit=365)
```

## 3. Batch FX Rates (Multiple Pairs)

```python
from defeatbeta_api.data.fx import FXPair

pairs = ["EURUSD", "GBPUSD", "USDJPY"]
data = {pair: FXPair(pair).rate(limit=30) for pair in pairs}
```
