"""
Tradinator — YH Finance Fetcher.

Secondary price-data source using the ``yfinance`` library.  Used as a
fallback inside DataPipeline when the primary broker adapter cannot
return price bars for an instrument.

The fetcher converts IG epic strings to Yahoo Finance tickers via a
curated look-up table, then downloads OHLCV bars in the same dict
schema that the broker adapter returns, so DataPipeline needs no extra
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
# Epic → Yahoo Finance ticker mapping
# ---------------------------------------------------------------------------
# Covers all 30 instruments in the current universe.json.
# Indices use the Yahoo "^" prefix; forex uses the "=X" suffix;
# futures/commodities use the "=F" suffix for continuous front-month.
# ---------------------------------------------------------------------------
EPIC_TO_YH_TICKER: dict[str, str] = {
    # Indices — UK
    "IX.D.FTSE.DAILY.IP":    "^FTSE",
    # Indices — US
    "IX.D.SPTRD.DAILY.IP":   "^GSPC",
    "IX.D.DOW.DAILY.IP":     "^DJI",
    "IX.D.NASDAQ.DAILY.IP":  "^NDX",
    "IX.D.RUSSELL.DAILY.IP": "^RUT",
    # Indices — EU
    "IX.D.DAX.DAILY.IP":     "^GDAXI",
    "IX.D.CAC.DAILY.IP":     "^FCHI",
    "IX.D.STXE.DAILY.IP":    "^STOXX50E",
    "IX.D.AEX.DAILY.IP":     "^AEX",
    "IX.D.IBEX.DAILY.IP":    "^IBEX",
    "IX.D.SMI.DAILY.IP":     "^SSMI",
    "IX.D.ITLY.DAILY.IP":    "FTSEMIB.MI",
    # Indices — APAC
    "IX.D.NIKKEI.DAILY.IP":  "^N225",
    "IX.D.ASX.DAILY.IP":     "^AXJO",
    "IX.D.HSENG.DAILY.IP":   "^HSI",
    # Forex — mini CFD epics map to the equivalent spot pair
    "CS.D.EURUSD.MINI.IP":   "EURUSD=X",
    "CS.D.GBPUSD.MINI.IP":   "GBPUSD=X",
    "CS.D.USDJPY.MINI.IP":   "USDJPY=X",
    "CS.D.AUDUSD.MINI.IP":   "AUDUSD=X",
    "CS.D.USDCAD.MINI.IP":   "USDCAD=X",
    "CS.D.EURGBP.MINI.IP":   "EURGBP=X",
    "CS.D.EURJPY.MINI.IP":   "EURJPY=X",
    "CS.D.USDCHF.MINI.IP":   "USDCHF=X",
    "CS.D.NZDUSD.MINI.IP":   "NZDUSD=X",
    # Commodities (CME/NYMEX/COMEX hard commodities)
    "CC.D.CL.UMP.IP":        "CL=F",   # WTI Crude Oil
    "CC.D.LCO.UMP.IP":       "BZ=F",   # Brent Crude Oil
    "CC.D.GC.UMP.IP":        "GC=F",   # Gold
    "CC.D.SILVER.UMP.IP":    "SI=F",   # Silver
    "CC.D.NGAS.UMP.IP":      "NG=F",   # Natural Gas
    "CC.D.COPPER.UMP.IP":    "HG=F",   # Copper
    # Soft commodities (ICE/CBOT) — CO.D.* prefix (P6-fix)
    # Note: month-specific contracts (Month1/Month2/Month3) map to the
    # continuous front-month Yahoo ticker; exact contract rollover differs.
    "CO.D.CC.Month1.IP":     "CC=F",   # Cocoa (ICE)
    "CO.D.KC.Month1.IP":     "KC=F",   # Coffee Arabica (ICE)
    "CO.D.C.Month1.IP":      "ZC=F",   # Corn (CBOT)
    "CO.D.CT.Month2.IP":     "CT=F",   # Cotton No.2 (ICE)
    "CO.D.DX.Month1.IP":     "DX-Y.NYB",  # US Dollar Index (ICE)
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
        self, epic: str, resolution: str, lookback: int
    ) -> list[dict]:
        """Fetch OHLCV bars from Yahoo Finance for *epic*.

        Parameters
        ----------
        epic:
            IG epic identifier (e.g. ``"IX.D.FTSE.DAILY.IP"``).
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

            Returns an empty list when no ticker mapping exists for *epic*,
            when Yahoo returns no data, or when any error occurs.
        """
        ticker = EPIC_TO_YH_TICKER.get(epic)
        if ticker is None:
            print(f"[YHFinanceFetcher] No ticker mapping for {epic} — skipping YH Finance fallback.")
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
            print(f"[YHFinanceFetcher] WARNING: download failed for {epic} ({ticker}) — {exc}")
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
                    return None if math.isnan(v) else v
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
