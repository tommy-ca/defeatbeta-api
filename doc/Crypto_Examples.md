# Crypto Examples

Examples for crypto market data (CEX and DEX) aligned with `/specs`.

## 1. CEX OHLCV

```python
from defeatbeta_api.data.crypto import CryptoToken

btc = CryptoToken("BTC")
btc.ohlcv(interval="1d", limit=365)
```

## 2. CEX Funding Rates

```python
from defeatbeta_api.data.crypto import CryptoToken

eth = CryptoToken("ETH", exchange="binance")
eth.funding_rate(limit=200)
```

## 3. DEX Pool Swaps

```python
from defeatbeta_api.data.dex import DexPool

pool = DexPool("0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8", chain="ethereum")
pool.swaps(limit=500)
```
