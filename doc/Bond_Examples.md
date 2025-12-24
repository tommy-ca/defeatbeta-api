# Bond Examples

Examples for bond yields and curves aligned with `/specs`.

## 1. Treasury Yield Curve

```python
from defeatbeta_api.data.bond import Bond

ust10y = Bond("US10Y")
ust10y.yield_curve()
```

## 2. Bond Yield History

```python
from defeatbeta_api.data.bond import Bond

ust2y = Bond("US2Y")
ust2y.yield_curve(limit=365)
```

## 3. Multiple Tenors

```python
from defeatbeta_api.data.bond import Bond

tenors = ["US2Y", "US5Y", "US10Y", "US30Y"]
curves = {t: Bond(t).yield_curve(limit=30) for t in tenors}
```
