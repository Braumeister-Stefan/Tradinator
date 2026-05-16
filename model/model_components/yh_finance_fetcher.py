"""
Tradinator — YH Finance Fetcher.

Secondary price-data source using the ``yfinance`` library.  Used as a
fallback inside DataPipeline when the primary broker adapter cannot
return price bars for an instrument.

The fetcher converts IBKR canonical instrument_id strings to Yahoo Finance
tickers via a curated look-up table, then downloads OHLCV bars in the same
dict schema that the broker adapter returns, so DataPipeline needs no extra
logic to consume them.

``bid_close`` is always ``None`` for YH-sourced bars because Yahoo
does not provide bid/ask decomposed prices.

DISCLAIMER: Tradinator is a personal experimentation tool for paper trading.
It does not constitute trading advice, investment recommendation, or financial
guidance of any kind. Use at your own risk.
"""

import math

import yfinance as yf


# ---------------------------------------------------------------------------
# IBKR canonical instrument_id → Yahoo Finance ticker mapping
# ---------------------------------------------------------------------------
# Keys match the canonical symbol strings used in IBKR adapter and universe.json.
# Indices use the Yahoo "^" prefix; forex uses the "=X" suffix;
# futures/commodities use the "=F" suffix for continuous front-month.
# ---------------------------------------------------------------------------
INSTRUMENT_TO_YH_TICKER: dict[str, str] = {
    # Indices — UK
    "FTSE":    "^FTSE",
    # Indices — US
    "SPX":     "^GSPC",    # S&P 500
    "DOW":     "^DJI",     # Dow Jones
    "NDX":     "^NDX",     # NASDAQ 100
    "RUT":     "^RUT",     # Russell 2000
    # Indices — EU
    "DAX":     "^GDAXI",
    "CAC":     "^FCHI",
    "STOXX50": "^STOXX50E",
    "AEX":     "^AEX",
    "IBEX":    "^IBEX",
    "SMI":     "^SSMI",
    "FTSEMIB": "FTSEMIB.MI",
    "OMX":     "^OMX",     # OMX Stockholm 30
    # Indices — APAC
    "NIKKEI":  "^N225",
    "ASX200":  "^AXJO",
    "HSI":     "^HSI",
    # Forex — major pairs (IBKR CASH secType, symbol is the base currency)
    "EURUSD":  "EURUSD=X",
    "GBPUSD":  "GBPUSD=X",
    "USDJPY":  "USDJPY=X",
    "AUDUSD":  "AUDUSD=X",
    "USDCAD":  "USDCAD=X",
    "EURGBP":  "EURGBP=X",
    "EURJPY":  "EURJPY=X",
    "USDCHF":  "USDCHF=X",
    "NZDUSD":  "NZDUSD=X",
    # Forex — additional crosses
    "USDHKD":  "USDHKD=X",
    "USDINR":  "USDINR=X",
    "USDBRL":  "USDBRL=X",
    "USDMXN":  "USDMXN=X",
    "USDSGD":  "USDSGD=X",
    "USDNOK":  "USDNOK=X",
    "USDSEK":  "USDSEK=X",
    "USDDKK":  "USDDKK=X",
    "USDPLN":  "USDPLN=X",
    "USDCZK":  "USDCZK=X",
    "USDHUF":  "USDHUF=X",
    "USDRUB":  "USDRUB=X",  # Russian Ruble; data unreliable since 2022 sanctions
    "USDCNH":  "USDCNH=X",
    "USDKRW":  "USDKRW=X",
    "USDTHB":  "USDTHB=X",
    "USDIDR":  "USDIDR=X",
    # Energy futures (NYMEX/ICE continuous front-month)
    "CL":      "CL=F",     # WTI Crude Oil
    "BZ":      "BZ=F",     # Brent Crude Oil
    "NG":      "NG=F",     # Natural Gas
    # Metal futures (COMEX continuous front-month)
    "GC":      "GC=F",     # Gold
    "SI":      "SI=F",     # Silver
    "HG":      "HG=F",     # Copper
    "PA":      "PA=F",     # Palladium
    "PL":      "PL=F",     # Platinum
    # Agricultural/soft commodities (CBOT/ICE)
    "ZL":      "ZL=F",     # Soybean Oil (CBOT)
    "ZC":      "ZC=F",     # Corn (CBOT)
    "CC":      "CC=F",     # Cocoa (ICE)
    "CT":      "CT=F",     # Cotton No.2 (ICE)
    "KC":      "KC=F",     # Coffee Arabica (ICE)
    "HE":      "HE=F",     # Lean Hogs (CME)
    "OJ":      "OJ=F",     # Orange Juice (ICE)
    "ZR":      "ZR=F",     # Rough Rice (CBOT; thinly traded, expect data gaps)
    "ZS":      "ZS=F",     # Soybeans (CBOT)
    "SB":      "SB=F",     # Sugar No.11 (ICE)
    "ZM":      "ZM=F",     # Soybean Meal (CBOT)
    # US Dollar Index
    "DX":      "DX-Y.NYB",
    # Crypto
    "DOGE":    "DOGE-USD",
    "LINK":    "LINK-USD",
    "XLM":     "XLM-USD",
    "UNI":     "UNI-USD",
}

# ---------------------------------------------------------------------------
# Resolution → yfinance interval
# ---------------------------------------------------------------------------
_RESOLUTION_TO_INTERVAL: dict[str, str] = {
    "DAY":   "1d",
    "HOUR":  "1h",
    "MIN":   "1m",
    "WEEK":  "1wk",
    "MONTH": "1mo",
}

# Minimum bars period must cover — use a generous multiplier so the
# returned slice always contains at least `lookback` trading bars even
# after weekends/holidays.
_LOOKBACK_MULTIPLIER = 2.5


class YHFinanceFetcher:
    """Secondary price-data source using Yahoo Finance via ``yfinance``."""

    def fetch_historical_prices(
        self, instrument_id: str, resolution: str, lookback: int
    ) -> list[dict]:
        """Fetch OHLCV bars from Yahoo Finance for *instrument_id*.

        Parameters
        ----------
        instrument_id:
            IBKR canonical instrument identifier (e.g. ``"DAX"``, ``"EURUSD"``).
        resolution:
            Price bar resolution string (e.g. ``"DAY"``).
        lookback:
            Number of bars requested.  The method fetches a period long
            enough to guarantee at least this many trading bars.

        Returns
        -------
        list[dict]
            Each dict matches the broker adapter bar schema::

                {
                    "close":     float | None,
                    "high":      float | None,
                    "low":       float | None,
                    "open":      float | None,
                    "volume":    float | None,
                    "bid_close": None,          # not available from Yahoo
                    "timestamp": str,           # ISO-8601 UTC string
                }

            Returns an empty list when no ticker mapping exists for *instrument_id*,
            when Yahoo returns no data, or when any error occurs.
        """
        ticker = INSTRUMENT_TO_YH_TICKER.get(instrument_id)
        if ticker is None:
            print(f"[YHFinanceFetcher] No ticker mapping for {instrument_id} — skipping YH Finance fallback.")
            return []

        interval = _RESOLUTION_TO_INTERVAL.get(resolution.upper(), "1d")
        period = self._lookback_to_period(lookback, resolution)

        try:
            df = yf.download(
                ticker,
                period=period,
                interval=interval,
                auto_adjust=True,
                progress=False,
                # Silence multi-level column warning for single-ticker download
                multi_level_index=False,
            )
        except Exception as exc:
            print(f"[YHFinanceFetcher] WARNING: download failed for {instrument_id} ({ticker}) — {exc}")
            return []

        if df is None or df.empty:
            return []

        # Keep only the most recent `lookback` rows.
        df = df.tail(lookback)

        bars: list[dict] = []
        for ts, row in df.iterrows():
            def _safe(val) -> float | None:
                try:
                    v = float(val)
                    return None if (math.isnan(v) or math.isinf(v)) else v
                except (TypeError, ValueError):
                    return None

            # Timestamp: convert to UTC ISO-8601 string.
            try:
                timestamp = ts.isoformat()
            except Exception:
                timestamp = str(ts)

            bars.append({
                "close":     _safe(row.get("Close")),
                "high":      _safe(row.get("High")),
                "low":       _safe(row.get("Low")),
                "open":      _safe(row.get("Open")),
                "volume":    _safe(row.get("Volume")),
                "bid_close": None,
                "timestamp": timestamp,
            })

        return bars

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _lookback_to_period(lookback: int, resolution: str) -> str:
        """Convert a bar count to a yfinance period string.

        Uses a generous multiplier to account for non-trading days so
        that at least *lookback* bars are present in the download.
        """
        # Calendar days needed (roughly).
        calendar_days = math.ceil(lookback * _LOOKBACK_MULTIPLIER)

        res = resolution.upper()
        if res in ("HOUR", "MIN"):
            # yfinance intraday history is limited; cap at 60 days.
            days = min(calendar_days, 60)
            return f"{days}d"

        # Daily and above: express in days, months, or years.
        if calendar_days <= 30:
            return f"{calendar_days}d"
        if calendar_days <= 365:
            months = math.ceil(calendar_days / 30)
            return f"{months}mo"
        years = math.ceil(calendar_days / 365)
        return f"{years}y"
