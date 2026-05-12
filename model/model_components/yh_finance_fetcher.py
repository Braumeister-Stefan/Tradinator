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
# Covers all 32 instruments in the current universe.json.
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
    # Weekend spread-bets — same underlying as weekday equivalents
    "IX.D.SUNDAX.DAILY.IP":     "^GDAXI",
    "IX.D.SUNDOW.DAILY.IP":     "^DJI",
    "IX.D.SUNEURUSD.DAILY.IP":  "EURUSD=X",
    "IX.D.SUNFUN.DAILY.IP":     "^FTSE",
    "IX.D.SUNGBPUSD.DAILY.IP":  "GBPUSD=X",
    "IX.D.SUNUSDJPY.DAILY.IP":  "USDJPY=X",
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
    # Forex — TODAY.IP spreadbet variants (same underlying as MINI.IP)
    "CS.D.AUDUSD.TODAY.IP":  "AUDUSD=X",
    "CS.D.EURGBP.TODAY.IP":  "EURGBP=X",
    "CS.D.EURUSD.TODAY.IP":  "EURUSD=X",
    "CS.D.GBPUSD.TODAY.IP":  "GBPUSD=X",
    "CS.D.NZDUSD.TODAY.IP":  "NZDUSD=X",
    "CS.D.USDCAD.TODAY.IP":  "USDCAD=X",
    "CS.D.USDCHF.TODAY.IP":  "USDCHF=X",
    "CS.D.USDJPY.TODAY.IP":  "USDJPY=X",
    # Forex — exotic MINI.IP crosses
    "CS.D.HKDJPY.MINI.IP":   "HKDJPY=X",
    "CS.D.NOKJPY.MINI.IP":   "NOKJPY=X",
    "CS.D.SEKJPY.MINI.IP":   "SEKJPY=X",
    "CS.D.SGDJPY.MINI.IP":   "SGDJPY=X",
    "CS.D.ZARJPY.MINI.IP":   "ZARJPY=X",
    "CS.D.USDBRL.MINI.IP":   "USDBRL=X",
    "CS.D.USDCLP.MINI.IP":   "USDCLP=X",
    "CS.D.USDCNH.MINI.IP":   "USDCNH=X",
    "CS.D.USDCZK.MINI.IP":   "USDCZK=X",
    "CS.D.USDDKK.MINI.IP":   "USDDKK=X",
    "CS.D.USDHKD.MINI.IP":   "USDHKD=X",
    "CS.D.USDHUF.MINI.IP":   "USDHUF=X",
    "CS.D.USDIDR.MINI.IP":   "USDIDR=X",
    "CS.D.USDINR.MINI.IP":   "USDINR=X",
    "CS.D.USDKRW.MINI.IP":   "USDKRW=X",
    "CS.D.USDNOK.MINI.IP":   "USDNOK=X",
    "CS.D.USDRUB.MINI.IP":   "USDRUB=X",  # Russian Ruble; data unreliable since 2022 sanctions
    "CS.D.USDSEK.MINI.IP":   "USDSEK=X",
    "CS.D.USDSGD.MINI.IP":   "USDSGD=X",
    "CS.D.USDTHB.MINI.IP":   "USDTHB=X",
    # Crypto
    "CS.D.DOGUSD.TODAY.IP":  "DOGE-USD",
    "CS.D.EOSUSD.CFD.IP":    "EOS-USD",
    "CS.D.EOSUSD.TODAY.IP":  "EOS-USD",
    "CS.D.LNKUSD.TODAY.IP":  "LINK-USD",
    "CS.D.NEOUSD.TODAY.IP":  "NEO-USD",
    "CS.D.UNIUSD.TODAY.IP":  "UNI-USD",
    "CS.D.XLMUSD.CFD.IP":    "XLM-USD",
    "CS.D.XLMUSD.TODAY.IP":  "XLM-USD",
    # Spot metals — Yahoo Finance lacks true OTC spot; COMEX futures are the closest proxy
    "CS.D.USCGC.TODAY.IP":   "GC=F",   # Gold spot → COMEX Gold continuous futures
    "CS.D.USCSI.TODAY.IP":   "SI=F",   # Silver spot → COMEX Silver continuous futures
    # Commodities (CME/NYMEX/COMEX hard commodities — UMP rolling)
    "CC.D.CL.UMP.IP":        "CL=F",   # WTI Crude Oil
    "CC.D.LCO.UMP.IP":       "BZ=F",   # Brent Crude Oil
    "CC.D.GC.UMP.IP":        "GC=F",   # Gold
    "CC.D.SILVER.UMP.IP":    "SI=F",   # Silver
    "CC.D.NGAS.UMP.IP":      "NG=F",   # Natural Gas
    "CC.D.COPPER.UMP.IP":    "HG=F",   # Copper
    # Commodities (USS rolling DFB — agricultural/soft)
    "CC.D.BO.USS.IP":        "ZL=F",   # Soybean Oil (CBOT)
    "CC.D.C.USS.IP":         "ZC=F",   # Corn (CBOT)
    "CC.D.CC.USS.IP":        "CC=F",   # Cocoa (ICE)
    "CC.D.CT.USS.IP":        "CT=F",   # Cotton No.2 (ICE)
    "CC.D.KC.USS.IP":        "KC=F",   # Coffee Arabica (ICE)
    "CC.D.LH.USS.IP":        "HE=F",   # Lean Hogs (CME)
    "CC.D.OJ.USS.IP":        "OJ=F",   # Orange Juice (ICE FCOJ-A)
    "CC.D.RR.USS.IP":        "ZR=F",   # Rough Rice (CBOT; thinly traded, expect data gaps)
    "CC.D.S.USS.IP":         "ZS=F",   # Soybeans (CBOT)
    "CC.D.SB.USS.IP":        "SB=F",   # Sugar No.11 (ICE)
    "CC.D.SM.USS.IP":        "ZM=F",   # Soybean Meal (CBOT)
    # Energy futures (EN.D.* near-dated contracts → Yahoo front-month continuous)
    "EN.D.CL.Month1.IP":     "CL=F",   # WTI Crude Oil (M1)
    "EN.D.LCO.Month4.IP":    "BZ=F",   # Brent Crude (M4; Yahoo only has front-month)
    # Metal futures (MT.D.* near-dated → Yahoo front-month continuous)
    "MT.D.GC.Month2.IP":     "GC=F",   # Gold (M2; front-month continuous as proxy)
    "MT.D.HG.Month1.IP":     "HG=F",   # Copper (M1)
    "MT.D.HG.Month2.IP":     "HG=F",   # Copper (M2; front-month continuous as proxy)
    "MT.D.PA.Month1.IP":     "PA=F",   # Palladium (M1)
    "MT.D.PL.Month2.IP":     "PL=F",   # Platinum (M2; front-month continuous as proxy)
    # Soft commodities (ICE/CBOT) — CO.D.* prefix
    # Note: month-specific contracts (Month1/Month2/Month3) map to the
    # continuous front-month Yahoo ticker; exact contract rollover differs.
    "CO.D.CC.Month1.IP":     "CC=F",   # Cocoa (ICE)
    "CO.D.KC.Month1.IP":     "KC=F",   # Coffee Arabica (ICE)
    "CO.D.C.Month1.IP":      "ZC=F",   # Corn (CBOT)
    "CO.D.CT.Month2.IP":     "CT=F",   # Cotton No.2 (ICE)
    "CO.D.DX.Month1.IP":     "DX-Y.NYB",  # US Dollar Index (ICE)
    # No Yahoo Finance ticker available:
    #   CS.D.CRYPTOB10.TODAY.IP — IG proprietary Crypto Basket Top 10 (no Yahoo equivalent)
    #   IR.D.FBTS.Month1.IP — Euro-Schatz 2Y (EUREX; Yahoo Finance does not carry EUREX futures)
    #   IR.D.FGBL.Month1.IP — Euro-Bund 10Y (EUREX)
    #   IR.D.FGBM.Month1.IP — Euro-Bobl 5Y (EUREX)
    #   IR.D.FGBX.Month1.IP — Euro-Buxl 30Y (EUREX)
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
