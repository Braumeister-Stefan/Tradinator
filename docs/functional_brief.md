# Tradinator — Functional Brief

> Produced by FUNCTIONALIST from IDEATOR's architectural brief.
> Target: BUILDER agents implementing each module independently.

---

## 0. Conventions

| Convention | Rule |
|---|---|
| Inter-component data | Plain `dict` (JSON-serialisable). No custom classes cross component boundaries. |
| Pipeline flow | Strictly linear 1→10. No component may import or call another component. |
| Major parameters | Declared in `config` dict in `main.py`. Passed to every component via `__init__(config)`. |
| Minor parameters | Class-level constants at the top of each component class body. |
| Internal methods | Private (`_`-prefixed). Called only from `run()` or from another private method in the same class. Never called cross-component. |
| External packages | `requests`, `python-dotenv`. No other third-party packages. Standard library otherwise. |
| Python version | 3.10+ (for `dict` union syntax and `match` if desired, but neither is required). |

---

## 1. File: `main.py`

### Purpose
Entry point. Defines the config dict and runs the pipeline.

### Config dict — exact keys

```python
config = {
    # Broker credentials
    "env_path": "secrets/.env",                # str — path to .env file with IG credentials

    # Instrument universe
    "universe": [                              # list[str] — IG epic identifiers
        "CS.D.AAPL.CFD.IP",
        "CS.D.MSFT.CFD.IP",
        "CS.D.GOOGL.CFD.IP",
    ],

    # Data parameters
    "resolution": "DAY",                       # str — price bar resolution: "DAY"
    "lookback": 50,                            # int — number of historical bars to fetch

    # Output
    "output_dir": "data/output",               # str — directory for ledger and analytics files
}
```

### Body

```
from model import Model

config = { ... }  # as above

if __name__ == "__main__":
    model = Model(config)
    model.run()
```

No other logic in this file.

---

## 2. File: `model.py`

### Class: `Model`

### Purpose
Orchestrates the 10-component pipeline in sequence, threading outputs to inputs.

### `__init__(self, config)`
Instantiates all 10 components, each receiving `config`.

### `run(self)` — exact wiring

```
broker_state      = self.broker_connector.run()
market_data       = self.data_pipeline.run(broker_state)
signals           = self.signal_engine.run(market_data)
validated_signals = self.strategy_eval.run(signals, market_data)
target_portfolio  = self.portfolio_constructor.run(validated_signals, broker_state)
orders            = self.order_generator.run(target_portfolio, broker_state)
execution_log     = self.order_executor.run(orders, broker_state)
ledger_snapshot   = self.portfolio_ledger.run(execution_log, broker_state)
analytics         = self.portfolio_analytics.run(ledger_snapshot)
                    self.performance_monitoring.run(analytics)
```

### Attribute names (instance variables)

| Attribute | Class |
|---|---|
| `self.broker_connector` | `BrokerConnector` |
| `self.data_pipeline` | `DataPipeline` |
| `self.signal_engine` | `SignalEngine` |
| `self.strategy_eval` | `StrategyEval` |
| `self.portfolio_constructor` | `PortfolioConstructor` |
| `self.order_generator` | `OrderGenerator` |
| `self.order_executor` | `OrderExecutor` |
| `self.portfolio_ledger` | `PortfolioLedger` |
| `self.portfolio_analytics` | `PortfolioAnalytics` |
| `self.performance_monitoring` | `PerformanceMonitoring` |

### Dependencies
All 10 classes imported from `model_components`.

---

## 3. File: `model_components/__init__.py`

### Exact exports

```python
from .broker_connector import BrokerConnector
from .data_pipeline import DataPipeline
from .signal_engine import SignalEngine
from .strategy_eval import StrategyEval
from .portfolio_constructor import PortfolioConstructor
from .order_generator import OrderGenerator
from .order_executor import OrderExecutor
from .portfolio_ledger import PortfolioLedger
from .portfolio_analytics import PortfolioAnalytics
from .performance_monitoring import PerformanceMonitoring
```

---

## 4. Component Specifications

---

### 4.1 `model_components/broker_connector.py`

**Class:** `BrokerConnector`

**Purpose:** Authenticates with the IG demo REST API and retrieves current account state.

**Minor parameters:**
```python
IG_API_URL_DEFAULT = "https://demo-api.ig.com/gateway/deal"
```

**Signature:** `run(self) -> dict`

**Input contract:** None (reads credentials from `.env` file at path `config["env_path"]`).

**Output contract — `broker_state`:**
```python
{
    "session": {
        "cst": str,              # IG client session token
        "x_security_token": str, # IG security token
        "api_url": str,          # base URL used
        "api_key": str,          # API key (needed for subsequent calls)
    },
    "positions": [               # current open positions from broker
        {
            "deal_id": str,
            "epic": str,
            "direction": str,    # "BUY" or "SELL"
            "size": float,
            "open_level": float,
            "currency": str,
        },
        # ... one dict per open position
    ],
    "cash": float,               # available cash balance
    "instruments": [str],        # copy of config["universe"]
}
```

**Internal methods:**
| Method | Responsibility |
|---|---|
| `_load_credentials(self)` | Reads `.env` file via `python-dotenv`; returns dict with keys `IG_API_KEY`, `IG_USERNAME`, `IG_PASSWORD`, `IG_ACCOUNT_ID`, and optionally `IG_API_URL`. |
| `_authenticate(self, credentials)` | POSTs to `/session` endpoint; returns `cst` and `x_security_token`. |
| `_fetch_positions(self, session)` | GETs `/positions`; returns list of position dicts. |
| `_fetch_account_balance(self, session)` | GETs `/accounts`; returns `float` cash balance for the configured account. |

**Placeholder vs. real:**
- ALL methods: **real implementation**. This component must make actual HTTP calls to the IG demo API.

**Dependencies:** `requests`, `python-dotenv`, `os`.

**Out of scope:** Placing orders, fetching price data, error retry logic.

---

### 4.2 `model_components/data_pipeline.py`

**Class:** `DataPipeline`

**Purpose:** Fetches historical price data from the IG API for every instrument in the universe.

**Minor parameters:**
```python
MAX_BARS = 500  # IG API hard limit per request
```

**Signature:** `run(self, broker_state) -> dict`

**Input contract:** `broker_state` as defined in §4.1.

**Output contract — `market_data`:**
```python
{
    "prices": {
        "<epic>": {              # one key per instrument in universe
            "history": [         # list of OHLCV bars, oldest first
                {
                    "date": str,     # ISO 8601 e.g. "2025-01-15T00:00:00"
                    "open": float,
                    "high": float,
                    "low": float,
                    "close": float,
                    "volume": float, # 0.0 if unavailable
                },
                # ... config["lookback"] bars
            ],
            "metadata": {
                "epic": str,
                "instrument_name": str,
                "currency": str,
            },
        },
    },
}
```

**Internal methods:**
| Method | Responsibility |
|---|---|
| `_fetch_prices(self, session, epic)` | GETs `/prices/{epic}` with resolution and lookback params; returns raw API response. |
| `_parse_prices(self, raw_response, epic)` | Converts IG price response into the `history` + `metadata` structure above. |

**Placeholder vs. real:**
- ALL methods: **real implementation**. Must call IG `/prices/{epic}` endpoint.

**Dependencies:** `requests`, `datetime`.

**Out of scope:** Caching, streaming, any transformation beyond OHLCV extraction.

---

### 4.3 `model_components/signal_engine.py`

**Class:** `SignalEngine`

**Purpose:** Generates a directional trading signal with strength for each instrument based on its price history.

**Minor parameters:**
```python
SHORT_WINDOW = 10   # short moving average period
LONG_WINDOW = 30    # long moving average period
```

**Signature:** `run(self, market_data) -> dict`

**Input contract:** `market_data` as defined in §4.2.

**Output contract — `signals`:**
```python
{
    "signals": [
        {
            "epic": str,
            "direction": str,    # "BUY", "SELL", or "HOLD"
            "strength": float,   # 0.0 to 1.0
        },
        # ... one dict per instrument
    ],
}
```

**Internal methods:**
| Method | Responsibility |
|---|---|
| `_compute_signal(self, history)` | Takes a single instrument's `history` list; returns `(direction, strength)` tuple. |

**Placeholder vs. real:**
- **Placeholder.** Implement a simple moving-average crossover as the default stub: if short MA > long MA → BUY, if short MA < long MA → SELL, else HOLD. Strength = absolute percentage difference between the two MAs, clamped to [0.0, 1.0]. This is intentionally naive and designed to be replaced.

**Dependencies:** None (standard library only).

**Out of scope:** Multiple signal strategies, indicator libraries, machine learning.

---

### 4.4 `model_components/strategy_eval.py`

**Class:** `StrategyEval`

**Purpose:** Filters and scores signals, removing those too weak to act on.

**Minor parameters:**
```python
MIN_STRENGTH = 0.1  # signals below this strength are discarded
```

**Signature:** `run(self, signals, market_data) -> dict`

**Input contract:** `signals` as defined in §4.3; `market_data` as defined in §4.2.

**Output contract — `validated_signals`:**
```python
{
    "validated_signals": [
        {
            "epic": str,
            "direction": str,    # "BUY" or "SELL" only (HOLD removed)
            "strength": float,   # original strength
            "score": float,      # final composite score, 0.0 to 1.0
        },
        # ... zero or more
    ],
}
```

**Internal methods:**
| Method | Responsibility |
|---|---|
| `_filter_signals(self, signals_list)` | Removes entries with direction `"HOLD"` or strength below `MIN_STRENGTH`. |
| `_score_signals(self, filtered_list, market_data)` | Assigns a `score` to each signal. |

**Placeholder vs. real:**
- **Placeholder.** `_filter_signals`: real (straightforward filter). `_score_signals`: stub that sets `score` equal to `strength`. Designed to be replaced with strategy-specific scoring.

**Dependencies:** None.

**Out of scope:** Backtesting, multi-strategy arbitration.

---

### 4.5 `model_components/portfolio_constructor.py`

**Class:** `PortfolioConstructor`

**Purpose:** Converts validated signals into target portfolio weights, respecting available capital.

**Minor parameters:**
```python
MAX_POSITION_WEIGHT = 0.2   # no single position may exceed 20% of equity
```

**Signature:** `run(self, validated_signals, broker_state) -> dict`

**Input contract:** `validated_signals` as defined in §4.4; `broker_state` as defined in §4.1.

**Output contract — `target_portfolio`:**
```python
{
    "targets": [
        {
            "epic": str,
            "direction": str,    # "BUY" or "SELL"
            "weight": float,     # 0.0 to MAX_POSITION_WEIGHT
        },
        # ... one per validated signal that survives weighting
    ],
    "total_equity": float,       # cash + sum of open position values (approximate)
}
```

**Internal methods:**
| Method | Responsibility |
|---|---|
| `_calculate_equity(self, broker_state)` | Returns total equity estimate from cash + positions. |
| `_assign_weights(self, validated_signals, total_equity)` | Distributes weights across signals, capped at `MAX_POSITION_WEIGHT`. |

**Placeholder vs. real:**
- **Placeholder.** `_calculate_equity`: real (cash + sum of `size * open_level` for each position). `_assign_weights`: stub that assigns equal weight = `1.0 / len(signals)`, capped at `MAX_POSITION_WEIGHT`. Designed to be replaced with optimisation-based allocation.

**Dependencies:** None.

**Out of scope:** Risk models, correlation analysis, leverage management.

---

### 4.6 `model_components/order_generator.py`

**Class:** `OrderGenerator`

**Purpose:** Computes the orders needed to move from current positions to the target portfolio.

**Minor parameters:**
```python
MIN_ORDER_SIZE = 0.01   # minimum order size (fractional shares/contracts)
```

**Signature:** `run(self, target_portfolio, broker_state) -> dict`

**Input contract:** `target_portfolio` as defined in §4.5; `broker_state` as defined in §4.1.

**Output contract — `orders`:**
```python
{
    "orders": [
        {
            "epic": str,
            "direction": str,    # "BUY" or "SELL"
            "size": float,       # absolute size to trade
            "order_type": str,   # always "MARKET"
        },
        # ... zero or more
    ],
}
```

**Internal methods:**
| Method | Responsibility |
|---|---|
| `_current_exposure(self, broker_state)` | Builds a dict `{epic: signed_size}` from current positions (positive = long, negative = short). |
| `_compute_deltas(self, targets, current, total_equity)` | For each target, calculates the delta between desired and current exposure; returns order list. Skips orders with absolute size below `MIN_ORDER_SIZE`. |

**Placeholder vs. real:**
- **Real implementation.** The logic is arithmetic: target_size = weight × total_equity (expressed in contracts/shares); delta = target_size − current_size; if delta > 0 → BUY, if delta < 0 → SELL.
- NOTE: size-to-contracts conversion may require a price lookup. Use the last close from `broker_state` positions' `open_level` as a simplification; the BUILDER may refine.

**Ambiguity note:** The architectural brief does not pass `market_data` to this component. If the builder needs a last-price for sizing, they should derive it from `broker_state["positions"]` open levels or add a comment flagging the gap. Do not add `market_data` as an input.

**Dependencies:** None.

**Out of scope:** Limit orders, stop-loss orders, order splitting.

---

### 4.7 `model_components/order_executor.py`

**Class:** `OrderExecutor`

**Purpose:** Submits each order to the IG demo API and records the outcome.

**Minor parameters:**
```python
CURRENCY_CODE = "GBP"   # default dealing currency
```

**Signature:** `run(self, orders, broker_state) -> dict`

**Input contract:** `orders` as defined in §4.6; `broker_state` as defined in §4.1.

**Output contract — `execution_log`:**
```python
{
    "executions": [
        {
            "epic": str,
            "direction": str,
            "size": float,
            "status": str,       # "FILLED", "REJECTED", or "PARTIAL"
            "deal_id": str,      # from broker response; "" if rejected
            "fill_price": float, # 0.0 if rejected
            "timestamp": str,    # ISO 8601
        },
        # ... one per order submitted
    ],
}
```

**Internal methods:**
| Method | Responsibility |
|---|---|
| `_submit_order(self, session, order)` | POSTs to IG `/positions/otc` endpoint; returns raw API response. |
| `_parse_confirmation(self, response, order)` | Extracts deal confirmation from IG response. GETs `/confirms/{deal_reference}` if needed. Builds a single execution dict. |

**Placeholder vs. real:**
- ALL methods: **real implementation**. Must submit actual orders to the IG demo API.

**Dependencies:** `requests`, `datetime`.

**Out of scope:** Order amendment, cancellation, partial fill handling beyond status reporting.

---

### 4.8 `model_components/portfolio_ledger.py`

**Class:** `PortfolioLedger`

**Purpose:** Builds a snapshot of the portfolio after execution and appends the run record to a persistent ledger file.

**Minor parameters:**
```python
LEDGER_FILENAME = "ledger.json"  # written inside config["output_dir"]
```

**Signature:** `run(self, execution_log, broker_state) -> dict`

**Input contract:** `execution_log` as defined in §4.7; `broker_state` as defined in §4.1. Note: `broker_state` is the state fetched at the START of the run. Positions in `broker_state` may not reflect executions that just occurred. The ledger should merge `broker_state["positions"]` with `execution_log` to build the snapshot. See ambiguity note.

**Output contract — `ledger_snapshot`:**
```python
{
    "positions": [               # best-effort post-execution positions
        {
            "deal_id": str,
            "epic": str,
            "direction": str,
            "size": float,
            "open_level": float,
            "currency": str,
        },
    ],
    "cash": float,               # estimated post-execution cash
    "total_equity": float,       # cash + position value estimate
    "history": [                 # records from THIS run's executions
        {
            "timestamp": str,
            "action": str,       # "BUY" or "SELL"
            "epic": str,
            "size": float,
            "price": float,
        },
    ],
    "run_timestamp": str,        # ISO 8601 of this run
}
```

**Internal methods:**
| Method | Responsibility |
|---|---|
| `_build_snapshot(self, execution_log, broker_state)` | Merges executions with broker_state positions to estimate post-execution state. |
| `_estimate_cash(self, broker_state, execution_log)` | Calculates estimated remaining cash: starting cash minus cost of BUYs plus proceeds of SELLs. |
| `_append_to_ledger(self, snapshot)` | Reads existing ledger file (if any) from `config["output_dir"]/LEDGER_FILENAME`, appends this snapshot, writes back. Creates file if absent. |

**Placeholder vs. real:**
- **Real implementation.** File I/O is straightforward JSON read/append/write. Position merging is arithmetic.

**Ambiguity note:** The architectural brief states "BrokerConnector is source of truth for current positions (not ledger)." This means the ledger is a historical record only. Future runs should NOT read the ledger to determine current positions — that comes from `BrokerConnector`. The snapshot here is a best-effort estimate for analytics purposes.

**Dependencies:** `json`, `os`, `datetime`.

**Out of scope:** Database storage, concurrent access handling, ledger correction.

---

### 4.9 `model_components/portfolio_analytics.py`

**Class:** `PortfolioAnalytics`

**Purpose:** Computes portfolio performance metrics from the ledger snapshot and historical ledger data.

**Minor parameters:**
```python
RISK_FREE_RATE = 0.0    # annualised, for Sharpe calculation
LEDGER_FILENAME = "ledger.json"  # same as PortfolioLedger; read from config["output_dir"]
```

**Signature:** `run(self, ledger_snapshot) -> dict`

**Input contract:** `ledger_snapshot` as defined in §4.8.

**Output contract — `analytics`:**
```python
{
    "total_return": float,       # cumulative return across all runs (0.0 if first run)
    "run_return": float,         # return from this run only (0.0 if no executions)
    "max_drawdown": float,       # maximum peak-to-trough decline (0.0 if insufficient data)
    "sharpe_ratio": float,       # annualised Sharpe ratio (0.0 if insufficient data)
    "position_count": int,       # number of open positions
    "cash": float,
    "total_equity": float,
    "run_timestamp": str,        # copied from ledger_snapshot
}
```

**Internal methods:**
| Method | Responsibility |
|---|---|
| `_load_ledger_history(self)` | Reads the full ledger file from disk; returns list of past snapshots. Returns empty list if file does not exist. |
| `_compute_returns(self, history, current_snapshot)` | Calculates `total_return` and `run_return` from equity series. |
| `_compute_drawdown(self, history, current_snapshot)` | Calculates `max_drawdown` from equity series. |
| `_compute_sharpe(self, history, current_snapshot)` | Calculates `sharpe_ratio` from return series. |

**Placeholder vs. real:**
- `_load_ledger_history`: **real** (JSON file read).
- `_compute_returns`: **real** (arithmetic on equity values).
- `_compute_drawdown`: **real** (standard max-drawdown formula on equity series).
- `_compute_sharpe`: **placeholder** if fewer than 2 data points in history; returns `0.0`. Otherwise **real** (standard Sharpe formula: mean(returns) − risk_free / std(returns), annualised).

**Dependencies:** `json`, `os`, `math`.

**Out of scope:** Sortino ratio, alpha/beta, benchmark comparison, charting.

---

### 4.10 `model_components/performance_monitoring.py`

**Class:** `PerformanceMonitoring`

**Purpose:** Prints a human-readable summary of the analytics to stdout.

**Minor parameters:**
```python
SEPARATOR = "=" * 50   # visual divider
```

**Signature:** `run(self, analytics) -> None`

**Input contract:** `analytics` as defined in §4.9.

**Output contract:** `None`. This component produces no data. Side effect: prints to stdout.

**Internal methods:**
| Method | Responsibility |
|---|---|
| `_format_summary(self, analytics)` | Builds a multi-line string summarising all analytics keys. |

**Placeholder vs. real:**
- **Real implementation.** It prints. Nothing more.

**Dependencies:** None.

**Out of scope:** Logging to file, alerting, email notifications, dashboards.

---

## 5. File: `secrets/.env.example`

```
IG_API_KEY=your_api_key_here
IG_USERNAME=your_username_here
IG_PASSWORD=your_password_here
IG_ACCOUNT_ID=your_account_id_here
```

The actual `.env` file (with real credentials) must never be committed. It is placed at `secrets/.env`.

---

## 6. File: `.gitignore`

```
# Credentials
secrets/.env

# Python
__pycache__/
*.pyc
*.pyo

# Output data
data/output/

# OS
.DS_Store
Thumbs.db

# IDE
.vscode/
.idea/
```

---

## 7. Data Flow Summary

```
                       ┌──────────────┐
                       │   main.py    │  config
                       └──────┬───────┘
                              │
                       ┌──────▼───────┐
                       │   model.py   │  orchestrator
                       └──────┬───────┘
                              │
         ┌────────────────────▼────────────────────┐
         │          model_components/               │
         │                                          │
         │  1. BrokerConnector                      │
         │     run() → broker_state                 │
         │         │                                │
         │  2. DataPipeline                         │
         │     run(broker_state) → market_data      │
         │         │                                │
         │  3. SignalEngine                         │
         │     run(market_data) → signals           │
         │         │                                │
         │  4. StrategyEval                         │
         │     run(signals, market_data)            │
         │       → validated_signals                │
         │         │                                │
         │  5. PortfolioConstructor                 │
         │     run(validated_signals, broker_state) │
         │       → target_portfolio                 │
         │         │                                │
         │  6. OrderGenerator                       │
         │     run(target_portfolio, broker_state)  │
         │       → orders                           │
         │         │                                │
         │  7. OrderExecutor                        │
         │     run(orders, broker_state)            │
         │       → execution_log                    │
         │         │                                │
         │  8. PortfolioLedger                      │
         │     run(execution_log, broker_state)     │
         │       → ledger_snapshot                  │
         │         │                                │
         │  9. PortfolioAnalytics                   │
         │     run(ledger_snapshot) → analytics     │
         │         │                                │
         │ 10. PerformanceMonitoring                │
         │     run(analytics) → None                │
         └──────────────────────────────────────────┘
```

---

## 8. Ambiguities Noted

| # | Issue | Resolution adopted |
|---|---|---|
| 1 | `OrderGenerator` needs a last price to convert weights to contract sizes, but does not receive `market_data`. | Use `open_level` from `broker_state["positions"]` as a proxy. For instruments with no current position, the builder should flag the gap with a comment. |
| 2 | `broker_state` is captured once at pipeline start, so post-execution positions are stale within the same run. | `PortfolioLedger` estimates post-execution state by merging executions into the start-of-run `broker_state`. This is an approximation. |
| 3 | `PortfolioAnalytics` and `PortfolioLedger` both reference the same ledger file. | Both use the same `LEDGER_FILENAME` constant and `config["output_dir"]`. Ledger writes before analytics reads, so no conflict within a single run. |

---

## 9. Dependency Matrix

| Component | `requests` | `python-dotenv` | `json` | `os` | `datetime` | `math` |
|---|---|---|---|---|---|---|
| BrokerConnector | ✓ | ✓ | | ✓ | | |
| DataPipeline | ✓ | | | | ✓ | |
| SignalEngine | | | | | | |
| StrategyEval | | | | | | |
| PortfolioConstructor | | | | | | |
| OrderGenerator | | | | | | |
| OrderExecutor | ✓ | | | | ✓ | |
| PortfolioLedger | | | ✓ | ✓ | ✓ | |
| PortfolioAnalytics | | | ✓ | ✓ | | ✓ |
| PerformanceMonitoring | | | | | | |

---

## 10. Requirements File

**File: `requirements.txt`**

```
requests
python-dotenv
```

No other third-party packages. All other imports are Python standard library.
