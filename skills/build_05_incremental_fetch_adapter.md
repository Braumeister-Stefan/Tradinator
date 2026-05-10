# BUILD PROMPT 05 — Add Date-Range Fetch to ig_adapter.py

**Prepared by:** FUNCTIONALIST  
**Scope:** Add a new method `fetch_historical_prices_by_date_range()` to `IGBrokerAdapter` in `model/model_components/ig_adapter.py`. This method wraps the `trading_ig` date-range API for incremental data fetching. Also add the method signature to `model/model_components/broker_adapter.py` protocol.

**No dependencies on other Build tasks** — this can be built independently.

---

## Context

The repository is at `/home/runner/work/Tradinator/Tradinator`.

Currently `IGBrokerAdapter.fetch_historical_prices()` always uses `fetch_historical_prices_by_epic_and_num_points` (fixed count). The `trading_ig` library also exposes a date-range variant that allows fetching bars from a specific start date to the present, which is needed for incremental fetching.

**Assumption about `trading_ig` API (verify before implementing):**  
The `trading_ig` library's `IGService` has a method `fetch_historical_prices_by_epic` with parameters:
- `epic`: str
- `resolution`: str (e.g. `"DAY"`)
- `start_date`: str formatted as `"YYYY-MM-DD HH:MM:SS"` (or ISO format depending on library version)
- `end_date`: str (optional; if omitted, defaults to present)
- `numpoints`: optional int (if both date range and numpoints are passed, date range takes precedence)

Verify the exact method signature by inspecting the `trading_ig` package:
```bash
python -c "from trading_ig import IGService; help(IGService.fetch_historical_prices_by_epic)"
```
Adapt the implementation to match the actual signature. If the date-range variant does not exist or has a different name, use `fetch_historical_prices_by_epic_and_num_points` with a large `numpoints` value and filter results in Python.

---

## Task 1 — Add `fetch_historical_prices_by_date_range()` to `IGBrokerAdapter`

Add the following method to the `IGBrokerAdapter` class, immediately after the existing `fetch_historical_prices()` method (around line 141):

```python
def fetch_historical_prices_by_date_range(
    self,
    instrument_id: str,
    resolution: str,
    from_date: str,
) -> list[dict]:
    """Fetch historical OHLCV bars from IG from a given UTC date to now.

    Parameters
    ----------
    instrument_id : str
        IG epic identifier.
    resolution : str
        Bar resolution, e.g. ``"DAY"``.
    from_date : str
        ISO-8601 UTC start date, e.g. ``"2026-01-15T00:00:00"``.
        Bars at or after this timestamp are returned.

    Returns
    -------
    list[dict]
        Same schema as ``fetch_historical_prices()``:
        ``{close, high, low, open, volume, bid_close, timestamp}``.
    """
    ig = self._require_session()
    # trading_ig date-range call — verify exact parameter names against library version.
    # Expected signature: fetch_historical_prices_by_epic(epic, resolution,
    #     start_date="YYYY-MM-DD HH:MM:SS", end_date="YYYY-MM-DD HH:MM:SS", numpoints=0)
    start_str = from_date.replace("T", " ").replace("Z", "")  # normalise to "YYYY-MM-DD HH:MM:SS"
    raw = ig.fetch_historical_prices_by_epic(
        instrument_id,
        resolution,
        start_date=start_str,
        end_date="",        # empty string = up to present (trading_ig convention)
        numpoints=0,        # 0 = rely on date range, not count
    )
    bars = raw.get("prices", [])
    result: list[dict] = []
    for bar in bars:
        ts = bar.get("snapshotTimeUTC") or bar.get("snapshotTime")
        close_price = bar.get("closePrice")
        bid_close_val = None
        if close_price is not None:
            bid_close_val = close_price.get("bid")
        result.append({
            "close": self._mid(bar.get("closePrice")),
            "high": self._mid(bar.get("highPrice")),
            "low": self._mid(bar.get("lowPrice")),
            "open": self._mid(bar.get("openPrice")),
            "volume": bar.get("lastTradedVolume"),
            "bid_close": bid_close_val,
            "timestamp": ts,
        })
    return result
```

**If the date-range API is not available:** Fall back to fetching `numpoints=500` bars via the fixed-count method and filtering in Python. Document this fallback with a clear comment.

---

## Task 2 — Add method signature to `broker_adapter.py` protocol

**File:** `model/model_components/broker_adapter.py`

Add a method signature to the `BrokerAdapter` Protocol, immediately after the `fetch_historical_prices` signature:

```python
def fetch_historical_prices_by_date_range(
    self,
    instrument_id: str,
    resolution: str,
    from_date: str,
) -> list[dict]:
    """Fetch OHLCV bars from ``from_date`` (ISO-8601 UTC) to the present."""
    ...
```

---

## Acceptance Criteria

1. `IGBrokerAdapter` has a `fetch_historical_prices_by_date_range(instrument_id, resolution, from_date)` method.
2. The method uses the `trading_ig` date-range API (or documented fallback).
3. Return type matches `fetch_historical_prices()`: `list[dict]` with keys `close, high, low, open, volume, bid_close, timestamp`.
4. `broker_adapter.py` Protocol has the matching method signature.
5. `python -c "from model.model_components import IGBrokerAdapter"` imports without errors.
6. Existing `fetch_historical_prices()` method is unchanged.

---

## Files to Touch

| File | Action |
|---|---|
| `model/model_components/ig_adapter.py` | MODIFY — add `fetch_historical_prices_by_date_range()` |
| `model/model_components/broker_adapter.py` | MODIFY — add method signature to Protocol |
