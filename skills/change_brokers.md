# IBKR Broker Migration — Investigation Report

## 1. Current Architecture (Good News)

The codebase has a **clean adapter pattern** designed for exactly this scenario. The `BrokerAdapter` protocol (`broker_adapter.py`) defines 8 normalised methods, and all downstream pipeline components (`DataPipeline`, `OrderExecutor`, etc.) call only these methods — they never touch the raw broker client. A placeholder `IBKRBrokerAdapter` already exists with all 8 methods stubbed as `NotImplementedError`, and it's already registered in `broker_connector.py`'s `_ADAPTER_REGISTRY`.

**In theory**, switching brokers is a config change (`"broker": "ibkr"`) plus implementing one file. **In practice**, there are several layers of IG-specific assumptions embedded throughout.

---

## 2. Files That Need Changing

| File | Change Type | Effort | Description |
|---|---|---|---|
| `model_components/ibkr_adapter.py` | **Rewrite** | **High** | Implement all 8 `BrokerAdapter` methods using `ib_async` (or `ib_insync`). This is the core work. |
| `requirements.txt` | Add dependency | Trivial | Add `ib_async` (or `ib_insync>=9.81`). |
| `secrets/.env.example` | Uncomment | Trivial | IBKR env vars (`IBKR_HOST`, `IBKR_PORT`, `IBKR_CLIENT_ID`) are already stubbed but commented out. |
| `main.py` | Config change | **Medium** | Change `"broker": "ig"` → `"broker": "ibkr"`. But `_load_universe()` has IG-specific deduplication logic (splits on `.` segments like `IX.D.FTSE`). This needs a broker-aware loader or a neutral universe format. |
| `data/input/universe.json` | **Rewrite** | **Medium** | Currently stores IG "epics" (`IX.D.DAX.IFD.IP`, `CS.D.AAPL.CFD.IP`). IBKR uses completely different identifiers: `(symbol, exchange, secType, currency, conId)`. The entire file must be recreated with IBKR contract specifications. |
| `data/input/discover_universe.py` | **Rewrite** | **Medium** | 100% IG-specific (uses `IGService`, IG search endpoint, IG epic validation). Needs an IBKR equivalent that resolves contracts via IBKR's contract search. |
| `model_components/__init__.py` | No change | — | Already handles optional IBKR import with try/except. |
| `model_components/broker_connector.py` | No change | — | Already registers IBKR adapter conditionally. |

---

## 3. Key Mapping & Concept Differences

### 3a. Instrument Identifiers

| Concept | IG | IBKR |
|---|---|---|
| Identifier | "epic" string, e.g. `IX.D.DAX.IFD.IP` | Contract: `(symbol="DAX", secType="CFD", exchange="SMART", currency="EUR", conId=123456)` |
| Unique key | Epic string | `conId` (integer) |
| Universe format | `{"epic": "CS.D.AAPL.CFD.IP"}` | `{"symbol": "AAPL", "secType": "STK", "exchange": "SMART", "currency": "USD", "conId": 265598}` |

The `instrument_id` field flows through **every pipeline component** (`DataPipeline`, `SignalEngine`, `StrategyEval`, `PortfolioConstructor`, `OrderGenerator`, `OrderExecutor`, `PortfolioLedger`). Currently this is an IG epic string. For IBKR, this would need to become either a `conId` string or a ticker symbol. This is the **single most pervasive mapping change**.

### 3b. Order/Deal Lifecycle

| Concept | IG | IBKR |
|---|---|---|
| Place order | Returns `deal_reference` immediately | Returns `orderId`; status arrives asynchronously via callback |
| Confirm order | `confirm_deal(deal_reference)` → `{status, deal_id}` | Poll `order.orderStatus.status` or use `openOrder`/`orderStatus` callbacks |
| Position close | Explicit `close_position()` with `deal_id` | Place an opposing order (no separate close API) |
| Deal ID | IG `dealId` string | IBKR `orderId` (int) or `execId` string |

The `BrokerAdapter` protocol's `confirm_deal(deal_reference) → {status, deal_id}` pattern is shaped around IG's two-step deal confirmation. IBKR doesn't have this exact pattern — you'd need to poll or wait for async fill confirmations.

### 3c. Historical Data

| Concept | IG | IBKR |
|---|---|---|
| Price bars | Returns `{closePrice: {bid, ask}, highPrice: {bid, ask}, ...}` | Returns actual OHLCV `{open, high, low, close, volume}` |
| `bid_close` field | Available (CFD bid/ask spread) | **Not available** — IBKR returns traded prices, not bid/ask bars |
| Resolution string | `"DAY"` | `"1 day"`, `"1 hour"`, etc. |
| Lookback specification | Number of bars | Duration string like `"50 D"` or number of bars (depends on method) |

The `bid_close` field in the adapter protocol and `DataPipeline` will always be `None` for IBKR. This flows into `universe_series.xlsx` (the `bid_close` sheet would be empty).

### 3d. Connection Model

| Concept | IG | IBKR |
|---|---|---|
| Connection | REST API, session-based auth, stateless | **Requires TWS or IB Gateway running locally**, socket connection, stateful |
| Authentication | Username + password + API key | TWS/Gateway handles auth; API just connects to `host:port` |
| Demo enforcement | `IG_ACC_TYPE=DEMO` checked in code | Use IB Gateway paper trading port (`4002`) vs live (`4001`) |

IBKR requires a **running desktop application** (TWS or IB Gateway) as a prerequisite. This is a significant operational change — you can't just set environment variables and go.

### 3e. Product Types

| Concept | IG | IBKR |
|---|---|---|
| Default product | CFDs (Contracts for Difference) | Stocks, ETFs, Futures, Options, CFDs, Forex |
| Expiry | `"-"` (DFB / no expiry) | Depends on `secType` — stocks have no expiry, futures do |
| Position sizing | CFD size (fractional) | Share count (integer for stocks, fractional only if enabled) |
| Currency | `currency_code` passed on order | Determined by contract definition |

---

## 4. Workload Estimate

| Task | Effort | Notes |
|---|---|---|
| Implement `ibkr_adapter.py` (8 methods) | **3–5 days** | Hardest part: async connection management, contract resolution, order status polling, mapping IBKR responses to adapter schema |
| Design a broker-neutral `universe.json` format | **0.5 day** | Add `ibkr_conId`/`ibkr_symbol` fields alongside `epic`, or create a separate IBKR universe file |
| Rewrite `_load_universe()` in `main.py` | **0.5 day** | Make loader broker-aware |
| Create IBKR version of `discover_universe.py` | **1 day** | IBKR contract search + validation |
| Resolution/lookback mapping | **0.5 day** | Map `"DAY"` → `"1 day"`, etc., inside the adapter |
| IBKR demo-only safety guard | **0.5 day** | Verify port `4002` (paper) and reject `4001` (live) |
| Testing & integration | **2–3 days** | Requires IBKR paper trading account + IB Gateway running |
| **Total** | **~8–11 days** | For a single developer familiar with both APIs |

---

## 5. Tech Debt for NOT Addressing Upfront

If you **don't** make the codebase broker-neutral now, the following debt accumulates:

| Debt Item | Impact | Severity |
|---|---|---|
| **IG epic strings hardcoded as `instrument_id` everywhere** | Every JSON ledger, trade history, xlsx series file, and log will contain IG epics. When you switch, all historical data becomes unreadable or requires a migration script. | **High** |
| **`universe.json` is IG-only** | No place to store IBKR contract specs. You'll need a format migration or parallel file. | **Medium** |
| **`_load_universe()` deduplication is IG-specific** | The 3-dot-segment base extraction (`IX.D.FTSE`) is meaningless for IBKR. Any new universe loader will break the old one. | **Medium** |
| **`discover_universe.py` is throwaway** | 192 lines of IG-only code that can't be reused. | **Low** |
| **`bid_close` field in protocol and data pipeline** | IBKR can't populate this. If your signal engine or analytics ever use `bid_close`, they'll silently get `None` for IBKR. | **Medium** |
| **`deal_reference` / `deal_id` / `confirm_deal` pattern** | Shaped around IG's confirmation flow. IBKR has a fundamentally different async order lifecycle. The protocol itself may need extending. | **Medium** |
| **Existing `universe_series.xlsx` with IG epic columns** | Historical data columns are named by IG epic. After switching, new IBKR data will have different column names, making time series discontinuous. | **High** |
| **Ledger/trades JSON files (`data/output/`)** | All recorded positions and trades reference IG epics and deal IDs. No way to reconcile post-migration. | **Medium** |

### Compounding Risk

The longer you stay IG-only, the more data accumulates in IG-specific formats (ledger snapshots, trade history, xlsx series). A future migration becomes not just a code change but a **data migration** — and historical series continuity is hard to preserve when instrument IDs change.

---

## 6. Recommendations

**If you want to minimise future pain:**

1. **Now**: Make `instrument_id` broker-neutral by adding a mapping layer (e.g., a canonical symbol like `"AAPL"` that maps to IG epic or IBKR conId). This is the single highest-leverage change.
2. **Now**: Refactor `universe.json` to store both IG and IBKR identifiers per instrument.
3. **Now**: Extract `_load_universe()` into a broker-aware utility.
4. **Later**: Implement `ibkr_adapter.py` when ready to switch.

**If you want to defer everything:**

- The adapter pattern means the *pipeline code* is clean. But instrument identity, historical data, and the order confirmation contract will all need reworking later. Budget ~2 extra days of data migration work on top of the 8–11 day estimate above.
