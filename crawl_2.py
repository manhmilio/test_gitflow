#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
Vietnam Stock Market - Monthly Panel Data Collector
=============================================================================
Thu thập và tính toán monthly panel data cho 350 công ty HOSE/HNX, 10 năm.

Required packages:
    pip install vnstock3 pandas numpy scipy openpyxl requests python-dateutil

Usage:
    python vn_stock_panel_collector.py

Output (in ./output/):
    vn_stock_panel.csv               — Main panel data
    vn_stock_panel.xlsx              — Same data + metadata + column dictionary
    unavailable_variables.csv        — Variables not collectible from vnstock
    vnstock_collection.log           — Full execution log
=============================================================================
"""

import os
import sys
import time
import logging
import warnings
import threading
import traceback
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from typing import Optional, List, Dict, Tuple, Any
from collections import deque

import numpy as np
import pandas as pd
from scipy import stats
import requests

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
_END_DATE  = date.today()
_START_DATE = _END_DATE - relativedelta(years=10)

CONFIG: Dict[str, Any] = {
    "MAX_REQUESTS_PER_MINUTE": 155,   # hard ceiling 160 → use 155 for safety margin
    "MAX_STOCKS": 350,
    "START_DATE": _START_DATE.strftime("%Y-%m-%d"),
    "END_DATE":   _END_DATE.strftime("%Y-%m-%d"),
    "OUTPUT_DIR": "output",
    "LOG_FILE":   "vnstock_collection.log",
    "EXCHANGES":  ["HOSE", "HNX"],
    "RETRY_ATTEMPTS": 3,
    "RETRY_DELAY":    5,       # seconds; multiplied by attempt number
    "INTER_STOCK_PAUSE": 0.4,  # polite pause between stocks (seconds)
    "CHECKPOINT_EVERY":  50,   # save partial CSV every N stocks
}

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
os.makedirs(CONFIG["OUTPUT_DIR"], exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(
            os.path.join(CONFIG["OUTPUT_DIR"], CONFIG["LOG_FILE"]),
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# RATE LIMITER  (sliding-window, thread-safe)
# ─────────────────────────────────────────────────────────────────────────────
class RateLimiter:
    """
    Sliding-window rate limiter.
    Guarantees ≤ max_calls_per_minute requests within any 60-second window.
    Thread-safe via a re-entrant lock.
    """

    def __init__(self, max_calls_per_minute: int = 155):
        self.max_calls   = max_calls_per_minute
        self.window_sec  = 60.0
        self._calls: deque = deque()
        self._lock   = threading.Lock()
        self.total_calls = 0

    def acquire(self) -> None:
        """Block the calling thread until a slot is available, then consume it."""
        with self._lock:
            while True:
                now    = time.monotonic()
                cutoff = now - self.window_sec
                # Purge expired timestamps
                while self._calls and self._calls[0] <= cutoff:
                    self._calls.popleft()

                if len(self._calls) < self.max_calls:
                    self._calls.append(time.monotonic())
                    self.total_calls += 1
                    return

                # Must wait until the oldest call exits the window
                wait = self.window_sec - (now - self._calls[0]) + 0.05
                logger.debug(f"[RateLimiter] {len(self._calls)}/{self.max_calls} — sleeping {wait:.2f}s")
                time.sleep(max(wait, 0.05))


RATE_LIMITER = RateLimiter(max_calls_per_minute=CONFIG["MAX_REQUESTS_PER_MINUTE"])


def safe_call(func, *args, symbol: str = "", **kwargs):
    """
    Execute `func` with retry logic and rate limiting.
    Returns None on permanent failure (after all retries).
    """
    for attempt in range(CONFIG["RETRY_ATTEMPTS"]):
        RATE_LIMITER.acquire()
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            if attempt < CONFIG["RETRY_ATTEMPTS"] - 1:
                delay = CONFIG["RETRY_DELAY"] * (attempt + 1)
                logger.warning(
                    f"[{symbol}] Attempt {attempt + 1} failed: {exc}. "
                    f"Retry in {delay}s…"
                )
                time.sleep(delay)
            else:
                logger.error(f"[{symbol}] All {CONFIG['RETRY_ATTEMPTS']} attempts failed: {exc}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# VNSTOCK CLIENT BOOTSTRAP
# ─────────────────────────────────────────────────────────────────────────────
try:
    from vnstock3 import Vnstock as _VnstockCls          # type: ignore
    logger.info("vnstock3 loaded.")
except ImportError:
    try:
        from vnstock import Vnstock as _VnstockCls       # type: ignore
        logger.info("vnstock (v2) loaded.")
    except ImportError:
        logger.critical(
            "vnstock3 is not installed. Run:  pip install vnstock3"
        )
        sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# UTILITY HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _find_col(df: pd.DataFrame, candidates: List[str]) -> pd.Series:
    """
    Return the first column whose normalised name matches any candidate.
    Returns a Series of NaN if none found.
    Matching is case-insensitive and ignores spaces / underscores.
    """
    def _norm(s: str) -> str:
        return s.lower().replace(" ", "").replace("_", "").replace("/", "")

    col_map = {_norm(c): c for c in df.columns}
    for cand in candidates:
        key = _norm(cand)
        if key in col_map:
            return df[col_map[key]].copy()
    return pd.Series(np.nan, index=df.index, name="__missing__")


def _get_series(df: Optional[pd.DataFrame], col: str) -> pd.Series:
    """Safe column getter that returns empty Series instead of raising."""
    if df is None or df.empty or col not in df.columns:
        return pd.Series(dtype=float)
    return df[col]


def _pct_change_yoy(s: pd.Series) -> pd.Series:
    """12-month YoY percentage change (safe: returns NaN for zeros)."""
    shifted = s.shift(12)
    shifted = shifted.replace(0, np.nan)
    return s / shifted - 1


# ─────────────────────────────────────────────────────────────────────────────
# RAW DATA COLLECTOR
# ─────────────────────────────────────────────────────────────────────────────
class VNStockDataCollector:
    """
    Wraps vnstock3 API calls with rate limiting and retry logic.
    All public methods return pandas DataFrames or None on failure.
    """

    def __init__(self):
        self.start = CONFIG["START_DATE"]
        self.end   = CONFIG["END_DATE"]

    # ── Stock universe ────────────────────────────────────────────────────────
    def get_stock_list(self) -> pd.DataFrame:
        """Return DataFrame of listed stocks on HOSE + HNX."""
        logger.info("Fetching stock universe…")
        try:
            RATE_LIMITER.acquire()
            vc    = _VnstockCls()
            stock = vc.stock(symbol="VCI", source="VCI")
            df    = stock.listing.all_symbols()
            if df is None or df.empty:
                raise ValueError("Empty response from listing.all_symbols()")
        except Exception as exc:
            logger.error(f"get_stock_list failed: {exc}")
            return pd.DataFrame()

        # Normalise column names
        df.columns = [c.strip() for c in df.columns]

        # Filter for HOSE / HNX
        exch_col = next(
            (c for c in df.columns
             if c.lower() in ("exchange", "comgroupcode", "exchangecode", "san")),
            None,
        )
        if exch_col:
            df = df[df[exch_col].str.upper().isin(CONFIG["EXCHANGES"])].copy()
            logger.info(
                f"  {len(df)} stocks on "
                f"{df[exch_col].value_counts().to_dict()}"
            )
        else:
            logger.warning("Exchange column not found — using full list.")

        return df.head(CONFIG["MAX_STOCKS"]).reset_index(drop=True)

    # ── Price / trading data ──────────────────────────────────────────────────
    def get_price_history(self, symbol: str) -> Optional[pd.DataFrame]:
        """Daily OHLCV from VCI source."""
        def _fetch():
            vc  = _VnstockCls()
            stk = vc.stock(symbol=symbol, source="VCI")
            return stk.trading.history(
                start=self.start, end=self.end, interval="1D", to_df=True
            )

        df = safe_call(_fetch, symbol=symbol)
        if df is None or df.empty:
            return None
        df.columns = [c.lower() for c in df.columns]
        # Ensure 'date' column
        for candidate in ("time", "date", "tradingdate"):
            if candidate in df.columns:
                df["date"] = pd.to_datetime(df[candidate], errors="coerce")
                break
        if "date" not in df.columns:
            logger.warning(f"[{symbol}] Price data has no recognisable date column.")
            return None
        df["symbol"] = symbol
        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        return df

    # ── Financial statements (quarterly) ─────────────────────────────────────
    def get_balance_sheet(self, symbol: str) -> Optional[pd.DataFrame]:
        def _fetch():
            vc  = _VnstockCls()
            stk = vc.stock(symbol=symbol, source="TCBS")
            return stk.finance.balance_sheet(period="quarter", lang="en")
        df = safe_call(_fetch, symbol=symbol)
        if df is not None and not df.empty:
            df["symbol"] = symbol
        return df if (df is not None and not df.empty) else None

    def get_income_statement(self, symbol: str) -> Optional[pd.DataFrame]:
        def _fetch():
            vc  = _VnstockCls()
            stk = vc.stock(symbol=symbol, source="TCBS")
            return stk.finance.income_statement(period="quarter", lang="en")
        df = safe_call(_fetch, symbol=symbol)
        if df is not None and not df.empty:
            df["symbol"] = symbol
        return df if (df is not None and not df.empty) else None

    def get_cash_flow(self, symbol: str) -> Optional[pd.DataFrame]:
        def _fetch():
            vc  = _VnstockCls()
            stk = vc.stock(symbol=symbol, source="TCBS")
            return stk.finance.cash_flow(period="quarter", lang="en")
        df = safe_call(_fetch, symbol=symbol)
        if df is not None and not df.empty:
            df["symbol"] = symbol
        return df if (df is not None and not df.empty) else None

    def get_financial_ratios(self, symbol: str) -> Optional[pd.DataFrame]:
        def _fetch():
            vc  = _VnstockCls()
            stk = vc.stock(symbol=symbol, source="TCBS")
            return stk.finance.ratio(period="quarter", lang="en")
        df = safe_call(_fetch, symbol=symbol)
        if df is not None and not df.empty:
            df["symbol"] = symbol
        return df if (df is not None and not df.empty) else None

    def get_company_info(self, symbol: str) -> Optional[Dict]:
        """Return company overview as a dict."""
        def _fetch():
            vc  = _VnstockCls()
            stk = vc.stock(symbol=symbol, source="TCBS")
            return stk.company.overview()
        result = safe_call(_fetch, symbol=symbol)
        if result is None:
            return None
        if isinstance(result, pd.DataFrame) and not result.empty:
            return result.iloc[0].to_dict()
        if isinstance(result, dict):
            return result
        return None

    def get_dividends(self, symbol: str) -> Optional[pd.DataFrame]:
        """Fetch dividend event history."""
        def _fetch():
            vc  = _VnstockCls()
            stk = vc.stock(symbol=symbol, source="TCBS")
            # Try .dividends(), fall back to .events()
            try:
                return stk.company.dividends()
            except Exception:
                ev = stk.company.events()
                if ev is not None and not ev.empty:
                    mask = ev.apply(
                        lambda r: any(
                            kw in str(r).lower()
                            for kw in ("dividend", "cổ tức", "cotuc", " div")
                        ),
                        axis=1,
                    )
                    return ev[mask]
                return None
        df = safe_call(_fetch, symbol=symbol)
        if df is not None and not df.empty:
            df["symbol"] = symbol
        return df if (df is not None and not df.empty) else None

    # ── Market index ──────────────────────────────────────────────────────────
    def get_vnindex(self) -> Optional[pd.DataFrame]:
        """VN-Index daily history (used for beta calculation)."""
        def _fetch():
            vc  = _VnstockCls()
            stk = vc.stock(symbol="VNINDEX", source="VCI")
            return stk.trading.history(
                start=self.start, end=self.end, interval="1D", to_df=True
            )
        df = safe_call(_fetch, symbol="VNINDEX")
        if df is None or df.empty:
            return None
        df.columns = [c.lower() for c in df.columns]
        for c in ("time", "date", "tradingdate"):
            if c in df.columns:
                df["date"] = pd.to_datetime(df[c], errors="coerce")
                break
        if "date" not in df.columns:
            return None
        return df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# FINANCIAL STATEMENT NORMALISER
# ─────────────────────────────────────────────────────────────────────────────
class FinancialNormaliser:
    """
    Convert raw vnstock financial DataFrames to quarterly-PeriodIndex frames.
    vnstock3/TCBS typically returns columns: yearReport, lengthReport (1-4), …
    """

    @staticmethod
    def to_quarterly(df: Optional[pd.DataFrame]) -> pd.DataFrame:
        """
        Returns a DataFrame indexed by pd.PeriodIndex(freq='Q').
        Handles multiple date formats returned by vnstock versions.
        """
        if df is None or df.empty:
            return pd.DataFrame()

        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]

        # ── Case 1: yearReport + lengthReport columns ──────────────────────
        yr_col  = next((c for c in df.columns if c.lower() in
                        ("yearreport", "year", "nam", "year_report")), None)
        qtr_col = next((c for c in df.columns if c.lower() in
                        ("lengthreport", "quarter", "quy", "length_report",
                         "quarterreport")), None)
        if yr_col and qtr_col:
            periods = []
            for _, row in df[[yr_col, qtr_col]].iterrows():
                try:
                    yr  = int(float(str(row[yr_col])))
                    qtr = int(float(str(row[qtr_col])))
                    periods.append(pd.Period(f"{yr}Q{qtr}", freq="Q"))
                except Exception:
                    periods.append(pd.NaT)
            df.index = pd.PeriodIndex(periods, freq="Q")
            df = df.drop(columns=[yr_col, qtr_col], errors="ignore")
            return df.sort_index()

        # ── Case 2: Already a DatetimeIndex ───────────────────────────────
        if isinstance(df.index, pd.DatetimeIndex):
            df.index = df.index.to_period("Q")
            return df.sort_index()

        # ── Case 3: Already a PeriodIndex ─────────────────────────────────
        if isinstance(df.index, pd.PeriodIndex):
            if str(df.index.freq).startswith("Q"):
                return df.sort_index()
            df.index = df.index.to_timestamp().to_period("Q")
            return df.sort_index()

        # ── Case 4: String / object index that looks like dates ───────────
        try:
            df.index = pd.to_datetime(df.index, errors="raise").to_period("Q")
            return df.sort_index()
        except Exception:
            pass

        # ── Case 5: Look for a date-like column ───────────────────────────
        for col in df.columns:
            if any(kw in col.lower() for kw in
                   ("date", "time", "period", "fiscal", "report", "quarter")):
                try:
                    df.index = pd.to_datetime(df[col], errors="raise").to_period("Q")
                    df = df.drop(columns=[col], errors="ignore")
                    return df.sort_index()
                except Exception:
                    continue

        logger.debug("Cannot determine quarterly period for financial DF — returning as-is.")
        return df

    @staticmethod
    def extract_balance_sheet(df: pd.DataFrame) -> pd.DataFrame:
        """Map raw columns to standard names. Returns quarterly DataFrame."""
        if df.empty:
            return pd.DataFrame()
        out = pd.DataFrame(index=df.index)
        out["cash"]                = _find_col(df, ["cash_and_cash_equivalents","cash","tien",
                                                     "cashequivalents","cash_equivalents"])
        out["short_term_invest"]   = _find_col(df, ["short_term_investments","shortterminvestments",
                                                     "shortterminvest"])
        out["receivables"]         = _find_col(df, ["accounts_receivable","receivables","phai_thu",
                                                     "short_term_receivables","receivable"])
        out["inventory"]           = _find_col(df, ["inventory","inventories","hang_ton","tonkho"])
        out["current_assets"]      = _find_col(df, ["total_current_assets","current_assets",
                                                     "short_term_assets","taisannganhan"])
        out["fixed_assets"]        = _find_col(df, ["fixed_assets","property_plant_equipment",
                                                     "ppe","net_ppe","taisancodinh"])
        out["intangible_assets"]   = _find_col(df, ["intangible_assets","intangibles",
                                                     "taisan_vohinh"])
        out["total_assets"]        = _find_col(df, ["total_assets","taisantong","asset","totalasset"])
        out["current_liabilities"] = _find_col(df, ["current_liabilities","short_term_liabilities",
                                                     "no_ngan_han","currentliability"])
        out["long_term_debt"]      = _find_col(df, ["long_term_debt","long_term_liabilities",
                                                     "no_dai_han","longtermdebt"])
        out["total_liabilities"]   = _find_col(df, ["total_liabilities","tong_no","totalliability",
                                                     "liabilities"])
        out["total_equity"]        = _find_col(df, ["equity","total_equity","stockholders_equity",
                                                     "owner_equity","vonchusohuu","shareholders_equity"])
        out["shares_outstanding"]  = _find_col(df, ["shares_outstanding","outstanding_shares",
                                                     "so_co_phieu","sharesissued","issued_shares"])
        # total_debt proxy: current + long-term liabilities
        if out["long_term_debt"].isna().all() and not out["total_liabilities"].isna().all():
            out["long_term_debt"] = out["total_liabilities"] - out["current_liabilities"].fillna(0)
        out["total_debt"] = out["current_liabilities"].fillna(0) + out["long_term_debt"].fillna(0)
        return out

    @staticmethod
    def extract_income_statement(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        out = pd.DataFrame(index=df.index)
        out["sales"]              = _find_col(df, ["revenue","net_revenue","sales","doanhthu",
                                                    "net_sales","total_revenue","netrevenue"])
        out["cogs"]               = _find_col(df, ["cost_of_goods_sold","cogs","gia_von",
                                                    "cost_of_revenue","costofsales"])
        out["gross_profit"]       = _find_col(df, ["gross_profit","loinhuan_gop","grossprofit"])
        out["operating_income"]   = _find_col(df, ["operating_income","ebit","operating_profit",
                                                    "loi_nhuan_hoat_dong","operatingprofit"])
        out["net_income"]         = _find_col(df, ["net_income","net_profit","profit_after_tax",
                                                    "loinhuan_sau_thue","netprofit","profitaftertax"])
        out["eps"]                = _find_col(df, ["eps","earnings_per_share","basic_eps","epsbasic"])
        out["sga"]                = _find_col(df, ["sga","selling_general_admin","selling_expenses",
                                                    "general_admin_expenses","banhang_quanly"])
        out["depreciation"]       = _find_col(df, ["depreciation","depreciation_amortization",
                                                    "khau_hao","da","depreciationamortization"])
        out["interest_expense"]   = _find_col(df, ["interest_expense","chi_phi_lai_vay","interestcost",
                                                    "interest_cost","financialexpense"])
        # Derive gross profit if missing
        if out["gross_profit"].isna().all():
            out["gross_profit"] = out["sales"] - out["cogs"].fillna(0)
        return out

    @staticmethod
    def extract_cash_flow(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        out = pd.DataFrame(index=df.index)
        out["operating_cf"]     = _find_col(df, ["operating_cash_flow","net_cash_from_operations",
                                                   "cfo","cashfromoperations","luuchuyen_kd"])
        out["capex"]            = _find_col(df, ["capital_expenditure","capex","purchase_of_ppe",
                                                   "acquisition_of_fixed_assets","mua_taisan",
                                                   "purchaseoffixedassets","capex_value"])
        out["investing_cf"]     = _find_col(df, ["investing_cash_flow","net_cash_from_investing",
                                                   "cashfrominvesting","luuchuyen_dt"])
        out["financing_cf"]     = _find_col(df, ["financing_cash_flow","net_cash_from_financing",
                                                   "cashfromfinancing","luuchuyen_tc"])
        out["equity_issuance"]  = _find_col(df, ["proceeds_from_stock_issuance","equity_issuance",
                                                   "stock_issuance","phat_hanh_cp","issuanceofstock"])
        out["dividends_paid"]   = _find_col(df, ["dividends_paid","payment_of_dividends","tra_cotuc",
                                                   "dividendpaid","cotuc_da_tra"])
        return out

    @staticmethod
    def extract_ratios(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        out = pd.DataFrame(index=df.index)
        out["roe"]               = _find_col(df, ["roe","return_on_equity","loi_nhuan_von_chu"])
        out["roa"]               = _find_col(df, ["roa","return_on_assets","loi_nhuan_taisan"])
        out["roic"]              = _find_col(df, ["roic","return_on_invested_capital","returnonic"])
        out["current_ratio"]     = _find_col(df, ["current_ratio","liquidity_ratio","thanh_toan_ngan_han"])
        out["quick_ratio"]       = _find_col(df, ["quick_ratio","acid_test","thanh_toan_nhanh"])
        out["leverage"]          = _find_col(df, ["debt_to_equity","leverage","de_ratio","d_e",
                                                    "financial_leverage","debtequity"])
        out["pe_ratio"]          = _find_col(df, ["pe","pe_ratio","price_to_earnings","p_e"])
        out["pb_ratio"]          = _find_col(df, ["pb","pb_ratio","price_to_book","p_b"])
        out["dividend_yield"]    = _find_col(df, ["dividend_yield","div_yield","toc_do_co_tuc"])
        out["market_cap"]        = _find_col(df, ["market_cap","market_capitalization","von_hoa",
                                                    "marketcap"])
        out["shares_outstanding"]= _find_col(df, ["shares_outstanding","outstanding_shares",
                                                    "so_co_phieu","sharesissued"])
        return out


# ─────────────────────────────────────────────────────────────────────────────
# MARKET VARIABLE ENGINE
# ─────────────────────────────────────────────────────────────────────────────
class MarketVariableEngine:
    """
    Compute monthly market/trading variables from daily price+volume data.
    All outputs are pd.DataFrames indexed by pd.PeriodIndex(freq='M').
    """

    @staticmethod
    def daily_returns(price_df: pd.DataFrame) -> pd.Series:
        """Daily close-to-close returns, indexed by date."""
        close_col = next(
            (c for c in price_df.columns if c in ("close", "closeprice", "adjclose")),
            None,
        )
        if close_col is None:
            return pd.Series(dtype=float)
        s = price_df.set_index("date")[close_col].sort_index().astype(float)
        return s.pct_change().dropna()

    @staticmethod
    def monthly_close(price_df: pd.DataFrame) -> pd.Series:
        """Month-end closing price as PeriodIndex(M) series."""
        close_col = next(
            (c for c in price_df.columns if c in ("close", "closeprice", "adjclose")),
            None,
        )
        if close_col is None:
            return pd.Series(dtype=float)
        s = price_df.set_index("date")[close_col].sort_index().astype(float)
        monthly = s.resample("ME").last()
        monthly.index = monthly.index.to_period("M")
        return monthly

    @staticmethod
    def compute_monthly_trading_vars(
        price_df: pd.DataFrame,
        shares_outstanding: Optional[float],
    ) -> pd.DataFrame:
        """
        Returns DataFrame with columns:
          stock_variance, return_volatility, maximum_return,
          zero_trading_frequency, dollar_volume, dollar_volume_volatility,
          turnover, turnover_volatility, amihud_illiquidity
        Index: PeriodIndex(freq='M')
        """
        if price_df is None or price_df.empty:
            return pd.DataFrame()

        df = price_df.set_index("date").sort_index().copy()
        close_col  = next((c for c in df.columns if c in ("close","closeprice","adjclose")), None)
        volume_col = next((c for c in df.columns if c in ("volume","matchvol","vol","tradingvolume")), None)

        if close_col is None:
            return pd.DataFrame()

        df["ret"] = df[close_col].astype(float).pct_change()
        if volume_col:
            df["dv"] = df[close_col].astype(float) * df[volume_col].astype(float)
            df["to"] = (
                df[volume_col].astype(float) / shares_outstanding
                if shares_outstanding and shares_outstanding > 0
                else np.nan
            )

        rows: Dict[pd.Period, Dict] = {}
        for period, grp in df.groupby(df.index.to_period("M")):
            r = {}
            rets = grp["ret"].dropna()
            r["stock_variance"]        = rets.var()             if len(rets) >= 5 else np.nan
            r["return_volatility"]     = rets.std()             if len(rets) >= 5 else np.nan
            r["maximum_return"]        = rets.max()             if len(rets) > 0  else np.nan

            if volume_col in df.columns:
                r["zero_trading_frequency"] = float((grp[volume_col].astype(float) == 0).mean())
                r["dollar_volume"]          = grp["dv"].sum()
                r["dollar_volume_volatility"] = grp["dv"].std()
                if not np.isnan(grp["to"].values).all():
                    r["turnover"]           = grp["to"].mean()
                    r["turnover_volatility"]= grp["to"].std()
                else:
                    r["turnover"] = r["turnover_volatility"] = np.nan
                # Amihud (2002): mean(|R_t| / DV_t) × 1e6 for scaling
                dv_pos = grp["dv"].copy().astype(float)
                dv_pos = dv_pos[dv_pos > 0]
                if len(dv_pos) >= 5:
                    common_idx = rets.index.intersection(dv_pos.index)
                    if len(common_idx) >= 5:
                        r["amihud_illiquidity"] = (
                            rets.loc[common_idx].abs() / dv_pos.loc[common_idx]
                        ).mean() * 1e6
                    else:
                        r["amihud_illiquidity"] = np.nan
                else:
                    r["amihud_illiquidity"] = np.nan
            else:
                for k in ("zero_trading_frequency","dollar_volume","dollar_volume_volatility",
                          "turnover","turnover_volatility","amihud_illiquidity"):
                    r[k] = np.nan

            rows[period] = r

        result = pd.DataFrame.from_dict(rows, orient="index")
        result.index.name = "yearmonth"
        return result

    @staticmethod
    def compute_beta_idio_by_month(
        stock_daily: pd.Series,
        market_daily: pd.Series,
        lookback_days: int = 252,
    ) -> pd.DataFrame:
        """
        For each month-end, regress trailing `lookback_days` daily stock returns
        on market returns. Returns columns: beta, beta_squared, idiosyncratic_volatility.
        Index: PeriodIndex(freq='M')
        """
        aligned = pd.concat(
            [stock_daily.rename("s"), market_daily.rename("m")], axis=1
        ).dropna()
        if aligned.empty:
            return pd.DataFrame()

        aligned.index = pd.to_datetime(aligned.index)
        month_ends    = aligned.resample("ME").last().index
        rows: Dict[pd.Period, Dict] = {}

        for me in month_ends:
            period = me.to_period("M")
            window = aligned[me - pd.DateOffset(days=lookback_days):me]
            if len(window) < 30:
                rows[period] = {"beta": np.nan, "beta_squared": np.nan,
                                "idiosyncratic_volatility": np.nan}
                continue
            y, x = window["s"].values, window["m"].values
            try:
                slope, intercept, *_ = stats.linregress(x, y)
                resid    = y - (intercept + slope * x)
                idio_vol = resid.std() * np.sqrt(252)
                rows[period] = {
                    "beta": slope,
                    "beta_squared": slope ** 2,
                    "idiosyncratic_volatility": idio_vol,
                }
            except Exception:
                rows[period] = {"beta": np.nan, "beta_squared": np.nan,
                                "idiosyncratic_volatility": np.nan}

        return pd.DataFrame.from_dict(rows, orient="index")

    @staticmethod
    def compute_price_delay_by_month(
        stock_daily: pd.Series,
        market_daily: pd.Series,
        n_lags: int = 4,
        lookback_days: int = 252,
    ) -> pd.DataFrame:
        """
        Hou & Moskowitz (2005) price delay.
        D = 1 - R²(restricted) / R²(unrestricted)
        Restricted:   R_i = a + b_0 * R_m
        Unrestricted: R_i = a + b_0 * R_m + sum_k b_k * R_m(-k)
        Index: PeriodIndex(freq='M')
        """
        aligned = pd.concat(
            [stock_daily.rename("s"), market_daily.rename("m")], axis=1
        ).dropna()
        if aligned.empty:
            return pd.DataFrame()

        aligned.index = pd.to_datetime(aligned.index)
        month_ends    = aligned.resample("ME").last().index
        rows: Dict[pd.Period, Dict] = {}

        for me in month_ends:
            period = me.to_period("M")
            window = aligned[me - pd.DateOffset(days=lookback_days):me].copy()

            for lag in range(1, n_lags + 1):
                window[f"m_lag{lag}"] = window["m"].shift(lag)

            window = window.dropna()
            if len(window) < 60:
                rows[period] = {"price_delay": np.nan}
                continue

            y    = window["s"].values
            lag_cols = [f"m_lag{k}" for k in range(1, n_lags + 1)]

            X_r  = np.column_stack([np.ones(len(y)), window["m"].values])
            X_u  = np.column_stack([np.ones(len(y)), window["m"].values,
                                    window[lag_cols].values])

            def _r2(X_, y_):
                coef, *_ = np.linalg.lstsq(X_, y_, rcond=None)
                ss_res   = np.sum((y_ - X_ @ coef) ** 2)
                ss_tot   = np.sum((y_ - y_.mean()) ** 2)
                return 1 - ss_res / ss_tot if ss_tot > 0 else np.nan

            try:
                r2_r = _r2(X_r, y)
                r2_u = _r2(X_u, y)
                delay = 1 - r2_r / r2_u if (r2_u and r2_u > 0) else np.nan
                rows[period] = {"price_delay": delay}
            except Exception:
                rows[period] = {"price_delay": np.nan}

        return pd.DataFrame.from_dict(rows, orient="index")


# ─────────────────────────────────────────────────────────────────────────────
# PANEL BUILDER  (single stock)
# ─────────────────────────────────────────────────────────────────────────────
class PanelBuilder:
    """
    Build a monthly panel (PeriodIndex='M') for one stock.
    Uses quarterly financial data (forward-filled up to 3 months).
    """

    MONTHLY_INDEX = pd.period_range(
        start=CONFIG["START_DATE"][:7],
        end=CONFIG["END_DATE"][:7],
        freq="M",
    )

    @staticmethod
    def _q2m(series: pd.Series) -> pd.Series:
        """
        Convert a quarterly PeriodIndex series to monthly PeriodIndex,
        forward-filling each quarter's value into its 3 calendar months.
        """
        if series.empty or series.isna().all():
            return pd.Series(np.nan, index=PanelBuilder.MONTHLY_INDEX, dtype=float)

        mapping: Dict[pd.Period, float] = {}
        for q_idx, val in series.dropna().items():
            try:
                if isinstance(q_idx, pd.Period):
                    m_end = q_idx.asfreq("M", how="end")
                else:
                    m_end = pd.Period(str(q_idx), freq="Q").asfreq("M", how="end")
                mapping[m_end] = val
            except Exception:
                pass

        if not mapping:
            return pd.Series(np.nan, index=PanelBuilder.MONTHLY_INDEX, dtype=float)

        s = pd.Series(mapping).reindex(PanelBuilder.MONTHLY_INDEX)
        return s.ffill(limit=3)   # one quarter = 3 months max

    # ── Main entry point ─────────────────────────────────────────────────────
    def build(
        self,
        symbol:          str,
        price_df:        Optional[pd.DataFrame],
        bs_raw:          Optional[pd.DataFrame],
        is_raw:          Optional[pd.DataFrame],
        cf_raw:          Optional[pd.DataFrame],
        ratio_raw:       Optional[pd.DataFrame],
        company_info:    Optional[Dict],
        market_daily:    pd.Series,
        dividend_df:     Optional[pd.DataFrame],
    ) -> pd.DataFrame:

        panel = pd.DataFrame(index=self.MONTHLY_INDEX)
        panel.index.name = "yearmonth"

        # ── Normalise financial statements ───────────────────────────────
        bs   = FinancialNormaliser.to_quarterly(bs_raw)
        is_  = FinancialNormaliser.to_quarterly(is_raw)
        cf   = FinancialNormaliser.to_quarterly(cf_raw)
        rt   = FinancialNormaliser.to_quarterly(ratio_raw)

        bs_v  = FinancialNormaliser.extract_balance_sheet(bs)   if not bs.empty  else pd.DataFrame()
        is_v  = FinancialNormaliser.extract_income_statement(is_) if not is_.empty else pd.DataFrame()
        cf_v  = FinancialNormaliser.extract_cash_flow(cf)         if not cf.empty  else pd.DataFrame()
        rt_v  = FinancialNormaliser.extract_ratios(rt)            if not rt.empty  else pd.DataFrame()

        q2m = self._q2m   # shorthand

        # ── Stock price (monthly close) ───────────────────────────────────
        if price_df is not None and not price_df.empty:
            monthly_close = MarketVariableEngine.monthly_close(price_df)
            panel["stock_price"] = monthly_close.reindex(self.MONTHLY_INDEX)
        else:
            panel["stock_price"] = np.nan

        # ── Shares outstanding (needed for turnover & market_cap) ─────────
        shares_out_val: Optional[float] = None
        if not rt_v.empty and "shares_outstanding" in rt_v.columns:
            sv = rt_v["shares_outstanding"].dropna()
            if not sv.empty:
                shares_out_val = float(sv.iloc[-1])
        if shares_out_val is None and not bs_v.empty and "shares_outstanding" in bs_v.columns:
            sv = bs_v["shares_outstanding"].dropna()
            if not sv.empty:
                shares_out_val = float(sv.iloc[-1])

        # ── Market / trading variables ────────────────────────────────────
        if price_df is not None and not price_df.empty:
            mkt_vars = MarketVariableEngine.compute_monthly_trading_vars(
                price_df, shares_out_val
            )
            for col in mkt_vars.columns:
                panel[col] = mkt_vars[col].reindex(self.MONTHLY_INDEX)

            stock_daily = MarketVariableEngine.daily_returns(price_df)

            if not market_daily.empty and not stock_daily.empty:
                # beta, beta_squared, idiosyncratic_volatility
                bidf = MarketVariableEngine.compute_beta_idio_by_month(
                    stock_daily, market_daily
                )
                for col in bidf.columns:
                    panel[col] = bidf[col].reindex(self.MONTHLY_INDEX)

                # price_delay
                pddf = MarketVariableEngine.compute_price_delay_by_month(
                    stock_daily, market_daily
                )
                if "price_delay" in pddf.columns:
                    panel["price_delay"] = pddf["price_delay"].reindex(self.MONTHLY_INDEX)

        # ── Balance sheet → monthly ───────────────────────────────────────
        for col in ("cash","inventory","total_assets","total_equity","total_debt",
                    "long_term_debt","current_assets","current_liabilities",
                    "receivables","fixed_assets","intangible_assets","shares_outstanding"):
            panel[col] = q2m(_get_series(bs_v, col)) if not bs_v.empty else np.nan

        # ── Income statement → monthly ────────────────────────────────────
        for col in ("sales","cogs","gross_profit","operating_income","net_income",
                    "eps","sga","depreciation","interest_expense"):
            panel[col] = q2m(_get_series(is_v, col)) if not is_v.empty else np.nan

        # ── Cash flow → monthly ───────────────────────────────────────────
        for col in ("operating_cf","capex","equity_issuance","dividends_paid"):
            panel[col] = q2m(_get_series(cf_v, col)) if not cf_v.empty else np.nan

        # ── Pre-computed ratios → monthly ─────────────────────────────────
        for col in ("roe","roa","roic","current_ratio","quick_ratio","leverage",
                    "dividend_yield","pe_ratio","pb_ratio"):
            panel[col] = q2m(_get_series(rt_v, col)) if not rt_v.empty else np.nan

        # market_cap: from ratio df, or compute price × shares
        if not rt_v.empty and "market_cap" in rt_v.columns:
            panel["market_cap"] = q2m(rt_v["market_cap"])
        if panel.get("market_cap", pd.Series(np.nan)).isna().all():
            panel["market_cap"] = panel["stock_price"] * (shares_out_val or np.nan)

        # If shares_outstanding column is all NaN, fill with constant
        if "shares_outstanding" in panel.columns and panel["shares_outstanding"].isna().all():
            panel["shares_outstanding"] = shares_out_val

        # ── Derived / computed variables ──────────────────────────────────
        panel = self._compute_derived(panel)

        # ── Momentum ─────────────────────────────────────────────────────
        panel = self._compute_momentum(panel)

        # ── Dividend events ───────────────────────────────────────────────
        panel = self._compute_dividend_events(panel, dividend_df)

        # ── Company static attributes ─────────────────────────────────────
        if company_info:
            # industry_code
            for k in ("industrycode","industryCode","icbcode","icbCode","comgroupcode",
                      "comGroupCode","sector","industry"):
                if k in company_info and company_info[k]:
                    panel["industry_code"] = str(company_info[k])
                    break

            # company_age (months since listing / founding)
            for k in ("listingDate","listing_date","foundedYear","established_year",
                      "listedDate","listed_date"):
                if k in company_info and company_info[k]:
                    try:
                        origin = pd.to_datetime(str(company_info[k]), errors="coerce")
                        if pd.notnull(origin):
                            panel["company_age"] = [
                                max(0, (p.to_timestamp() - origin).days / 30.44)
                                for p in panel.index
                            ]
                            break
                    except Exception:
                        pass

        panel["symbol"] = symbol
        return panel

    # ── Derived variable computation ─────────────────────────────────────────
    def _compute_derived(self, p: pd.DataFrame) -> pd.DataFrame:

        def safe_div(a, b):
            b_safe = b.replace(0, np.nan) if isinstance(b, pd.Series) else (np.nan if b == 0 else b)
            return a / b_safe

        mc = p.get("market_cap", pd.Series(np.nan, index=p.index))

        # ── Valuation ──────────────────────────────────────────────────────
        if "eps" in p.columns and "stock_price" in p.columns:
            p["earnings_to_price"] = safe_div(p["eps"], p["stock_price"])
        if "total_equity" in p.columns:
            p["book_to_market"] = safe_div(p["total_equity"], mc)
        if "dividends_paid" in p.columns:
            p["dividend_to_price"] = safe_div(p["dividends_paid"].abs(), mc)
        elif "dividend_yield" in p.columns:
            p["dividend_to_price"] = p["dividend_yield"]
        if "operating_cf" in p.columns:
            p["cashflow_to_price"] = safe_div(p["operating_cf"], mc)
        if "cash" in p.columns:
            p["cash_to_price"] = safe_div(p["cash"], mc)
        if "sales" in p.columns:
            p["sales_to_price"] = safe_div(p["sales"], mc)

        # ── Leverage (fallback if ratio not available) ────────────────────
        if p.get("leverage", pd.Series(np.nan)).isna().all():
            if "total_debt" in p.columns and "total_equity" in p.columns:
                p["leverage"] = safe_div(p["total_debt"], p["total_equity"])

        # ── Current / quick ratio (fallback) ─────────────────────────────
        if p.get("current_ratio", pd.Series(np.nan)).isna().all():
            if "current_assets" in p.columns and "current_liabilities" in p.columns:
                p["current_ratio"] = safe_div(p["current_assets"], p["current_liabilities"])
        if p.get("quick_ratio", pd.Series(np.nan)).isna().all():
            if all(c in p.columns for c in ("current_assets","inventory","current_liabilities")):
                p["quick_ratio"] = safe_div(
                    p["current_assets"] - p["inventory"].fillna(0),
                    p["current_liabilities"]
                )

        # ── Profitability ─────────────────────────────────────────────────
        ta = p.get("total_assets", pd.Series(np.nan, index=p.index))
        if "operating_income" in p.columns:
            p["operating_profitability"] = safe_div(p["operating_income"], ta)
        if "gross_profit" in p.columns:
            p["gross_profitability"] = safe_div(p["gross_profit"], ta)
        elif "sales" in p.columns and "cogs" in p.columns:
            p["gross_profitability"] = safe_div(p["sales"] - p["cogs"].fillna(0), ta)
        if p.get("roic", pd.Series(np.nan)).notna().any():
            p["return_on_invested_capital"] = p["roic"]
        elif "net_income" in p.columns and "long_term_debt" in p.columns and "total_equity" in p.columns:
            ic = p["total_equity"] + p["long_term_debt"].fillna(0)
            p["return_on_invested_capital"] = safe_div(p["net_income"], ic)

        # ── Growth (YoY 12-month lag) ─────────────────────────────────────
        for var, col in (
            ("earnings_growth",        "net_income"),
            ("sales_growth",           "sales"),
            ("asset_growth",           "total_assets"),
            ("capex_growth",           "capex"),
            ("long_term_debt_growth",  "long_term_debt"),
            ("depreciation_growth",    "depreciation"),
            ("current_ratio_growth",   "current_ratio"),
            ("quick_ratio_growth",     "quick_ratio"),
        ):
            if col in p.columns:
                p[var] = _pct_change_yoy(p[col])

        # earnings_increase_streak
        if "earnings_growth" in p.columns:
            streak, count = [], 0
            for val in p["earnings_growth"]:
                count = (count + 1) if (pd.notna(val) and val > 0) else 0
                streak.append(count)
            p["earnings_increase_streak"] = streak

        # ── Investment ────────────────────────────────────────────────────
        if "capex" in p.columns:
            p["investment_rate"] = safe_div(p["capex"].abs(), ta)
        if "asset_growth" in p.columns:
            p["corporate_investment"] = p["asset_growth"]

        # ── Financing ─────────────────────────────────────────────────────
        if "equity_issuance" in p.columns:
            p["net_equity_issuance"] = safe_div(p["equity_issuance"], mc.shift(1))

        # ── Accruals ──────────────────────────────────────────────────────
        if "net_income" in p.columns and "operating_cf" in p.columns:
            p["total_accruals"]  = p["net_income"] - p["operating_cf"]
            p["absolute_accruals"] = p["total_accruals"].abs()
            p["percent_accruals"]  = safe_div(p["total_accruals"], p["net_income"].abs())
        if "total_accruals" in p.columns:
            p["accrual_volatility"] = p["total_accruals"].rolling(12, min_periods=6).std()
        if "operating_cf" in p.columns:
            p["cashflow_volatility"] = p["operating_cf"].rolling(12, min_periods=6).std()
            if "total_debt" in p.columns:
                p["cashflow_to_debt"] = safe_div(p["operating_cf"], p["total_debt"])

        # ── Operating efficiency ──────────────────────────────────────────
        if "sales" in p.columns:
            for ratio_var, denom_col in (
                ("sales_to_inventory",   "inventory"),
                ("sales_to_cash",        "cash"),
                ("sales_to_receivables", "receivables"),
            ):
                if denom_col in p.columns:
                    p[ratio_var] = safe_div(p["sales"], p[denom_col])

        if "sales_to_inventory" in p.columns:
            p["sales_inventory_change"] = _pct_change_yoy(p["sales_to_inventory"])

        if "sales_growth" in p.columns:
            if "inventory" in p.columns:
                p["sales_minus_inventory_growth"] = (
                    p["sales_growth"] - _pct_change_yoy(p["inventory"])
                )
            if "receivables" in p.columns:
                p["sales_minus_receivables_growth"] = (
                    p["sales_growth"] - _pct_change_yoy(p["receivables"])
                )
            if "sga" in p.columns:
                p["sales_minus_sga_growth"] = (
                    p["sales_growth"] - _pct_change_yoy(p["sga"])
                )
            if "gross_profit" in p.columns and "sales" in p.columns:
                gross_margin        = safe_div(p["gross_profit"], p["sales"])
                gross_margin_change = _pct_change_yoy(gross_margin)
                p["gross_margin_minus_sales_growth"] = gross_margin_change - p["sales_growth"]

        if "fixed_assets" in p.columns:
            p["asset_tangibility"] = safe_div(p["fixed_assets"], ta)

        return p

    def _compute_momentum(self, p: pd.DataFrame) -> pd.DataFrame:
        if "stock_price" not in p.columns or p["stock_price"].isna().all():
            return p
        price = p["stock_price"].copy()
        m_ret = price.pct_change(1)

        def _cumret(series: pd.Series, window: int, skip: int = 1) -> pd.Series:
            """Rolling geometric cumulative return, skipping `skip` most recent months."""
            shifted = series.shift(skip)
            return shifted.rolling(window, min_periods=max(window // 2, 3)).apply(
                lambda x: (1 + x).prod() - 1, raw=True
            )

        p["momentum_6m"]      = _cumret(m_ret, 6,  skip=1)
        p["momentum_12m"]     = _cumret(m_ret, 12, skip=1)
        p["momentum_36m"]     = _cumret(m_ret, 36, skip=1)
        p["momentum_change"]  = p["momentum_6m"] - p["momentum_12m"]
        return p

    def _compute_dividend_events(
        self,
        p:           pd.DataFrame,
        dividend_df: Optional[pd.DataFrame],
    ) -> pd.DataFrame:
        p["dividend_initiation"] = 0
        p["dividend_omission"]   = 0

        if dividend_df is None or dividend_df.empty:
            return p

        try:
            div = dividend_df.copy()
            date_col = next(
                (c for c in div.columns
                 if any(k in c.lower() for k in ("date","time","period","ngay"))),
                None,
            )
            if date_col is None:
                return p

            div["_m"] = pd.to_datetime(div[date_col], errors="coerce").dt.to_period("M")
            div_months = set(div["_m"].dropna().values)

            has_div = p.index.map(lambda m: 1 if m in div_months else 0)
            has_div = pd.Series(has_div, index=p.index)

            # Initiation: dividend after ≥12 months with no dividend
            prior_no_div = has_div.rolling(12, min_periods=12).sum().shift(1)
            p["dividend_initiation"] = (
                (has_div == 1) & (prior_no_div == 0)
            ).astype(int)

            # Omission: no dividend after ≥6 consecutive months with dividend
            prior_with_div = has_div.rolling(12, min_periods=6).sum().shift(1)
            p["dividend_omission"] = (
                (has_div == 0) & (prior_with_div >= 6)
            ).astype(int)

        except Exception as exc:
            logger.debug(f"Dividend events error: {exc}")

        return p


# ─────────────────────────────────────────────────────────────────────────────
# CROSS-SECTIONAL INDUSTRY AGGREGATOR
# ─────────────────────────────────────────────────────────────────────────────
class IndustryAggregator:
    """
    Computes variables that require the full cross-section:
      industry_adjusted_book_to_market, industry_adjusted_cashflow_to_price,
      industry_adjusted_capex_growth, industry_adjusted_asset_turnover_change,
      industry_momentum, market_share, industry_concentration.
    Requires 'industry_code' column in the panel.
    """

    @staticmethod
    def compute(panel: pd.DataFrame) -> pd.DataFrame:
        if "industry_code" not in panel.columns:
            logger.warning("industry_code missing — skipping industry variables.")
            return panel

        df = panel.copy()
        grp_cols = ["yearmonth", "industry_code"]

        def _add_ind_adj(src_col: str, out_col: str, agg: str = "median"):
            if src_col not in df.columns:
                return
            ind_val = (
                df.groupby(grp_cols)[src_col]
                .transform(agg)
            )
            df[out_col] = df[src_col] - ind_val

        _add_ind_adj("book_to_market",    "industry_adjusted_book_to_market")
        _add_ind_adj("cashflow_to_price", "industry_adjusted_cashflow_to_price")
        _add_ind_adj("capex_growth",      "industry_adjusted_capex_growth")

        # asset_turnover_change
        if "sales" in df.columns and "total_assets" in df.columns:
            df["_asset_turnover"] = (
                df["sales"] /
                df["total_assets"].replace(0, np.nan)
            )
            df["_at_change"] = (
                df.sort_values("yearmonth")
                .groupby("symbol")["_asset_turnover"]
                .pct_change(12)
            )
            _add_ind_adj("_at_change", "industry_adjusted_asset_turnover_change")
            df.drop(columns=["_asset_turnover", "_at_change"], errors="ignore", inplace=True)

        # industry_momentum
        if "momentum_12m" in df.columns:
            df["industry_momentum"] = df.groupby(grp_cols)["momentum_12m"].transform("mean")

        # market_share and HHI
        if "sales" in df.columns:
            ind_sales = df.groupby(grp_cols)["sales"].transform("sum").replace(0, np.nan)
            df["market_share"] = df["sales"] / ind_sales
            # HHI = sum of squared market shares per (yearmonth, industry)
            df["industry_concentration"] = df.groupby(grp_cols)["market_share"].transform(
                lambda s: (s ** 2).sum()
            )

        return df


# ─────────────────────────────────────────────────────────────────────────────
# MACRO DATA COLLECTOR
# ─────────────────────────────────────────────────────────────────────────────
class MacroCollector:
    """
    Collects macro interest rate proxies from the World Bank public API.

    treasury_bill_rate  → proxied by Vietnam SBV discount rate (FR.INR.DISC)
    term_spread         → lending rate (FR.INR.LNDP) minus deposit rate (FR.INR.DPST)
    default_yield_spread→ UNAVAILABLE (no public corporate bond API for Vietnam)
    """

    WB_BASE = "https://api.worldbank.org/v2/country/VN/indicator"

    INDICATORS = {
        "FR.INR.DISC": "discount_rate",
        "FR.INR.LNDP": "lending_rate",
        "FR.INR.DPST": "deposit_rate",
    }

    @classmethod
    def fetch(cls) -> pd.DataFrame:
        frames: List[pd.DataFrame] = []

        for ind_code, col_name in cls.INDICATORS.items():
            url = f"{cls.WB_BASE}/{ind_code}?format=json&mrv=20&per_page=100"
            try:
                RATE_LIMITER.acquire()
                resp = requests.get(url, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, list) and len(data) > 1:
                    rows = [
                        {"year": int(r["date"]), col_name: r["value"]}
                        for r in data[1]
                        if r.get("value") is not None
                    ]
                    if rows:
                        frames.append(pd.DataFrame(rows).set_index("year"))
            except Exception as exc:
                logger.warning(f"World Bank {ind_code} fetch failed: {exc}")

        if not frames:
            logger.warning("No macro data retrieved from World Bank.")
            return pd.DataFrame()

        macro_annual = pd.concat(frames, axis=1)

        # Expand to monthly (year-long forward fill within each year)
        monthly_idx = pd.period_range(
            start=CONFIG["START_DATE"][:7],
            end=CONFIG["END_DATE"][:7],
            freq="M",
        )
        expanded = (
            pd.DataFrame({"year": monthly_idx.year}, index=monthly_idx)
            .merge(macro_annual, on="year", how="left")
            .drop(columns=["year"])
        )
        expanded.index = monthly_idx

        if "discount_rate" in expanded.columns:
            expanded["treasury_bill_rate"] = expanded["discount_rate"] / 100
        if "lending_rate" in expanded.columns and "deposit_rate" in expanded.columns:
            expanded["term_spread"] = (
                (expanded["lending_rate"] - expanded["deposit_rate"]) / 100
            )

        return expanded[
            [c for c in ("treasury_bill_rate", "term_spread") if c in expanded.columns]
        ]


# ─────────────────────────────────────────────────────────────────────────────
# UNAVAILABLE VARIABLES REGISTRY
# ─────────────────────────────────────────────────────────────────────────────
UNAVAILABLE_VARIABLES: List[Dict[str, str]] = [
    {
        "variable": "default_yield_spread",
        "category": "Macro",
        "status": "UNAVAILABLE",
        "reason": (
            "Requires Vietnamese corporate bond yields minus risk-free rate. "
            "No public machine-readable API exists for VBMA (Vietnam Bond Market Association) "
            "or VBI (Vietnam Bond Index) data. "
            "Manual collection from https://vbma.org.vn required."
        ),
    },
    {
        "variable": "rd_to_market_cap",
        "category": "Firm Characteristics",
        "status": "UNAVAILABLE",
        "reason": (
            "R&D expenditure is not a mandatory disclosure line item under Vietnamese GAAP "
            "(VAS – Vietnam Accounting Standards). "
            "Vietnamese listed companies rarely report R&D separately; "
            "it is typically bundled into SG&A or management expenses. "
            "Not available in vnstock financial data."
        ),
    },
    {
        "variable": "treasury_bill_rate",
        "category": "Macro",
        "status": "PARTIAL – proxy used",
        "reason": (
            "No direct Vietnamese T-bill rate series available via vnstock or free API. "
            "Proxied using SBV discount rate from World Bank API (annual, expanded to monthly). "
            "If World Bank API is unreachable, this variable will be NaN. "
            "For accurate data, use SBV website (https://sbv.gov.vn) or HNX bond market data."
        ),
    },
    {
        "variable": "term_spread",
        "category": "Macro",
        "status": "PARTIAL – proxy used",
        "reason": (
            "True term spread (10Y government bond yield minus 3-month T-bill) requires "
            "HNX/VBMA government bond yield curve data, which has no free API. "
            "Proxied as lending_rate minus deposit_rate from World Bank Vietnam data (annual). "
            "Proxy does not accurately represent the term structure of government bond yields."
        ),
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# COLUMN DICTIONARY  (for XLSX metadata sheet)
# ─────────────────────────────────────────────────────────────────────────────
COLUMN_DESCRIPTIONS: Dict[str, str] = {
    "symbol":               "Stock ticker (e.g. VCB, FPT)",
    "yearmonth":            "Year-Month period string YYYY-MM",
    "industry_code":        "Industry classification code (ICB / VNDirect sector code)",
    # Direct
    "stock_price":          "Monthly closing price (VND) — end-of-month",
    "market_cap":           "Market capitalisation = price × shares_outstanding (VND)",
    "cash":                 "Cash and cash equivalents (VND) — quarterly, forward-filled",
    "inventory":            "Inventory (VND) — quarterly, forward-filled",
    "sales":                "Net revenue / sales (VND) — quarterly, forward-filled",
    "beta":                 "Market beta — OLS slope on trailing 252-day daily returns vs VN-Index",
    "dividend_yield":       "Annual dividend yield (%) from financial ratios",
    "current_ratio":        "Current assets / current liabilities",
    "quick_ratio":          "(Current assets − inventory) / current liabilities",
    "leverage":             "Total debt / total equity (D/E ratio)",
    "roe":                  "Return on equity (%)",
    "roa":                  "Return on assets (%)",
    # Market
    "turnover":             "Avg daily trading volume / shares_outstanding (monthly avg)",
    "dollar_volume":        "Total monthly trading value (price × volume, VND)",
    "stock_variance":       "Variance of daily returns within the month",
    "return_volatility":    "Std dev of daily returns within the month",
    "idiosyncratic_volatility": "Annualised CAPM residual std dev (trailing 252-day window)",
    "beta_squared":         "Beta² (from trailing 252-day OLS regression)",
    "maximum_return":       "Maximum single-day return within the month",
    "amihud_illiquidity":   "Amihud (2002): mean(|R|/DV)×1e6; higher = less liquid",
    "price_delay":          "Hou & Moskowitz (2005): 1 − R²(restricted)/R²(unrestricted); 4 lags",
    "turnover_volatility":  "Std dev of daily turnover within the month",
    "dollar_volume_volatility": "Std dev of daily dollar volume within the month",
    "zero_trading_frequency":   "Fraction of trading days with zero volume",
    # Valuation
    "earnings_to_price":    "EPS / closing price (E/P ratio)",
    "book_to_market":       "Total equity (book) / market cap",
    "dividend_to_price":    "Dividends paid / market cap",
    "cashflow_to_price":    "Operating cash flow / market cap",
    "industry_adjusted_cashflow_to_price": "CF/P minus industry median CF/P",
    "cash_to_price":        "Cash and equivalents / market cap",
    "sales_to_price":       "Net revenue / market cap",
    "industry_adjusted_book_to_market": "B/M minus industry median B/M",
    # Profitability
    "operating_profitability":      "Operating income / total assets",
    "gross_profitability":          "Gross profit / total assets (Novy-Marx 2013)",
    "return_on_invested_capital":   "Net income / (total equity + long-term debt)",
    # Growth
    "earnings_growth":       "YoY growth in net income (12-month lag)",
    "earnings_increase_streak": "Consecutive months of positive YoY earnings growth",
    "sales_growth":          "YoY growth in sales (12-month lag)",
    "asset_growth":          "YoY growth in total assets (12-month lag)",
    "capex_growth":          "YoY growth in |capex| (12-month lag)",
    "long_term_debt_growth": "YoY growth in long-term debt (12-month lag)",
    "depreciation_growth":   "YoY growth in depreciation (12-month lag)",
    "current_ratio_growth":  "YoY growth in current ratio (12-month lag)",
    "quick_ratio_growth":    "YoY growth in quick ratio (12-month lag)",
    # Investment
    "investment_rate":             "|Capex| / total assets",
    "corporate_investment":        "Proxy: asset growth rate (YoY)",
    "industry_adjusted_capex_growth": "Capex growth minus industry median capex growth",
    # Financing
    "net_equity_issuance":  "Equity issuance proceeds / lagged market cap",
    # Accruals
    "total_accruals":       "Net income − operating cash flow",
    "absolute_accruals":    "|Total accruals|",
    "percent_accruals":     "Total accruals / |net income|",
    "accrual_volatility":   "Rolling 12-month std dev of total accruals",
    "cashflow_volatility":  "Rolling 12-month std dev of operating cash flow",
    "cashflow_to_debt":     "Operating cash flow / total debt",
    # Operating efficiency
    "industry_adjusted_asset_turnover_change": "Asset turnover YoY Δ minus industry median",
    "sales_inventory_change":         "YoY Δ in sales/inventory ratio",
    "sales_minus_inventory_growth":   "Sales growth − inventory growth",
    "sales_minus_receivables_growth": "Sales growth − receivables growth",
    "sales_minus_sga_growth":         "Sales growth − SG&A growth",
    "gross_margin_minus_sales_growth":"Gross margin growth − sales growth",
    "sales_to_inventory":   "Sales / inventory",
    "sales_to_cash":        "Sales / cash",
    "sales_to_receivables": "Sales / receivables (receivables turnover)",
    "asset_tangibility":    "Fixed assets / total assets",
    # Momentum
    "momentum_6m":          "Cumulative return months t−7 to t−2 (skipping t−1)",
    "momentum_12m":         "Cumulative return months t−13 to t−2",
    "momentum_36m":         "Cumulative return months t−37 to t−2",
    "momentum_change":      "momentum_6m − momentum_12m",
    "industry_momentum":    "Equal-weighted avg momentum_12m within industry",
    # Corporate events
    "dividend_omission":    "1 if no dividend after ≥6 consecutive months of paying",
    "dividend_initiation":  "1 if first dividend after ≥12 months without dividends",
    # Firm characteristics
    "company_age":          "Months since listing / founding date",
    "market_share":         "Firm sales / total industry sales",
    "rd_to_market_cap":     "[UNAVAILABLE] R&D not reported under Vietnamese GAAP",
    # Macro
    "treasury_bill_rate":   "SBV discount rate proxy (World Bank annual, monthly-expanded) [PARTIAL]",
    "term_spread":          "Lending rate − deposit rate (World Bank proxy) [PARTIAL]",
    "default_yield_spread": "[UNAVAILABLE] No public corporate bond spread API for Vietnam",
    # Industry
    "industry_concentration": "Herfindahl–Hirschman Index (HHI) of sales market shares",
    # Raw inputs retained for manual computation
    "total_assets":         "[input] Total assets (VND, quarterly, forward-filled)",
    "total_equity":         "[input] Total equity/book value (VND, quarterly, forward-filled)",
    "total_debt":           "[input] Total debt (current + long-term, VND, quarterly)",
    "long_term_debt":       "[input] Long-term debt (VND, quarterly, forward-filled)",
    "current_assets":       "[input] Current assets (VND, quarterly, forward-filled)",
    "current_liabilities":  "[input] Current liabilities (VND, quarterly, forward-filled)",
    "receivables":          "[input] Accounts receivable (VND, quarterly, forward-filled)",
    "fixed_assets":         "[input] Fixed assets / PP&E (VND, quarterly, forward-filled)",
    "intangible_assets":    "[input] Intangible assets (VND, quarterly, forward-filled)",
    "net_income":           "[input] Net income after tax (VND, quarterly, forward-filled)",
    "operating_income":     "[input] Operating income / EBIT (VND, quarterly, forward-filled)",
    "gross_profit":         "[input] Gross profit (VND, quarterly, forward-filled)",
    "cogs":                 "[input] Cost of goods sold (VND, quarterly, forward-filled)",
    "eps":                  "[input] Earnings per share (VND, quarterly, forward-filled)",
    "sga":                  "[input] SG&A expenses (VND, quarterly, forward-filled)",
    "depreciation":         "[input] Depreciation & amortisation (VND, quarterly)",
    "interest_expense":     "[input] Interest expense (VND, quarterly, forward-filled)",
    "operating_cf":         "[input] Operating cash flow (VND, quarterly, forward-filled)",
    "capex":                "[input] Capital expenditures (VND, quarterly, forward-filled)",
    "equity_issuance":      "[input] Equity issuance proceeds (VND, quarterly)",
    "dividends_paid":       "[input] Dividends paid (VND, quarterly, forward-filled)",
    "shares_outstanding":   "[input] Shares outstanding",
    "pe_ratio":             "[input] Price/Earnings ratio from ratio data",
    "pb_ratio":             "[input] Price/Book ratio from ratio data",
    "roic":                 "[input] ROIC from financial ratio data",
}


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────
class VNStockPanelOrchestrator:

    def __init__(self):
        self.data_collector  = VNStockDataCollector()
        self.panel_builder   = PanelBuilder()
        self.ind_aggregator  = IndustryAggregator()
        self.macro_collector = MacroCollector()
        os.makedirs(CONFIG["OUTPUT_DIR"], exist_ok=True)

    # ── Entry point ───────────────────────────────────────────────────────────
    def run(self) -> None:
        logger.info("=" * 72)
        logger.info("  VIETNAM STOCK PANEL DATA COLLECTION")
        logger.info(f"  Period : {CONFIG['START_DATE']}  →  {CONFIG['END_DATE']}")
        logger.info(f"  Stocks : up to {CONFIG['MAX_STOCKS']}  ({', '.join(CONFIG['EXCHANGES'])})")
        logger.info(f"  Rate   : ≤ {CONFIG['MAX_REQUESTS_PER_MINUTE']} requests / minute")
        logger.info("=" * 72)

        # ── 1. Stock universe ─────────────────────────────────────────────
        stock_list_df = self.data_collector.get_stock_list()
        if stock_list_df.empty:
            logger.critical("Cannot fetch stock list. Aborting.")
            return

        symbol_col = next(
            (c for c in stock_list_df.columns
             if c.lower() in ("ticker","symbol","code","stockcode")),
            stock_list_df.columns[0],
        )
        symbols: List[str] = (
            stock_list_df[symbol_col]
            .dropna()
            .astype(str)
            .str.strip()
            .str.upper()
            .unique()
            .tolist()
        )
        symbols = [s for s in symbols if s][:CONFIG["MAX_STOCKS"]]
        logger.info(f"Symbol list ({len(symbols)}): {symbols[:15]}…")

        # ── 2. VN-Index (market benchmark) ───────────────────────────────
        logger.info("Fetching VN-Index…")
        vnindex_df    = self.data_collector.get_vnindex()
        market_daily  = pd.Series(dtype=float)
        if vnindex_df is not None and not vnindex_df.empty:
            market_daily = MarketVariableEngine.daily_returns(vnindex_df)
            logger.info(f"VN-Index: {len(market_daily)} daily return observations")
        else:
            logger.warning("VN-Index unavailable — beta / idio_vol will be NaN.")

        # ── 3. Macro data ─────────────────────────────────────────────────
        logger.info("Fetching macro data (World Bank)…")
        macro_df = self.macro_collector.fetch()

        # ── 4. Per-stock collection and panel building ────────────────────
        all_panels:     List[pd.DataFrame] = []
        failed_symbols: List[str]          = []

        for idx, symbol in enumerate(symbols, start=1):
            logger.info(f"[{idx:3d}/{len(symbols)}] {symbol}")
            try:
                price_df     = self.data_collector.get_price_history(symbol)
                bs_raw       = self.data_collector.get_balance_sheet(symbol)
                is_raw       = self.data_collector.get_income_statement(symbol)
                cf_raw       = self.data_collector.get_cash_flow(symbol)
                ratio_raw    = self.data_collector.get_financial_ratios(symbol)
                company_info = self.data_collector.get_company_info(symbol)
                dividend_df  = self.data_collector.get_dividends(symbol)

                # Skip if we have no usable data at all
                if (price_df is None and bs_raw is None and
                        is_raw is None and cf_raw is None):
                    logger.warning(f"  [{symbol}] No data returned — skipping.")
                    failed_symbols.append(symbol)
                    continue

                stock_panel = self.panel_builder.build(
                    symbol       = symbol,
                    price_df     = price_df,
                    bs_raw       = bs_raw,
                    is_raw       = is_raw,
                    cf_raw       = cf_raw,
                    ratio_raw    = ratio_raw,
                    company_info = company_info,
                    market_daily = market_daily,
                    dividend_df  = dividend_df,
                )
                if not stock_panel.empty:
                    all_panels.append(stock_panel)

                # Checkpoint
                if idx % CONFIG["CHECKPOINT_EVERY"] == 0:
                    self._save_checkpoint(all_panels, idx)

                time.sleep(CONFIG["INTER_STOCK_PAUSE"])

            except KeyboardInterrupt:
                logger.warning("Interrupted — saving collected data.")
                break
            except Exception as exc:
                logger.error(f"  [{symbol}] Unhandled error: {exc}\n{traceback.format_exc()}")
                failed_symbols.append(symbol)

        if not all_panels:
            logger.critical("No panel data collected. Exiting.")
            return

        # ── 5. Combine all stock panels ───────────────────────────────────
        logger.info(f"Combining {len(all_panels)} stock panels…")
        full_panel = pd.concat(all_panels, ignore_index=False)
        full_panel = full_panel.reset_index().rename(columns={"index": "yearmonth"})
        full_panel["yearmonth"] = full_panel["yearmonth"].astype(str)

        # ── 6. Industry cross-sectional variables ─────────────────────────
        logger.info("Computing industry cross-sectional variables…")
        full_panel = self.ind_aggregator.compute(full_panel)

        # ── 7. Macro variables ────────────────────────────────────────────
        if not macro_df.empty:
            macro_str          = macro_df.copy()
            macro_str.index    = macro_str.index.astype(str)
            macro_reset        = macro_str.reset_index().rename(columns={"index": "yearmonth"})
            full_panel         = full_panel.merge(macro_reset, on="yearmonth", how="left")
        else:
            full_panel["treasury_bill_rate"] = np.nan
            full_panel["term_spread"]        = np.nan

        full_panel["default_yield_spread"] = np.nan   # always unavailable

        # ── 8. Column ordering ────────────────────────────────────────────
        id_cols   = [c for c in ("symbol","yearmonth","industry_code") if c in full_panel.columns]
        data_cols = [c for c in full_panel.columns if c not in id_cols]
        full_panel = full_panel[id_cols + sorted(data_cols)]
        full_panel = full_panel.sort_values(["symbol","yearmonth"]).reset_index(drop=True)

        # ── 9. Export ─────────────────────────────────────────────────────
        logger.info("Exporting outputs…")
        self._export(full_panel)
        self._export_unavailable()

        # ── Summary ───────────────────────────────────────────────────────
        n_ok  = len(symbols) - len(failed_symbols)
        logger.info("=" * 72)
        logger.info(f"  Done.  Stocks processed : {n_ok}/{len(symbols)}")
        logger.info(f"  Panel  rows             : {len(full_panel):,}")
        logger.info(f"  Panel  columns          : {len(full_panel.columns)}")
        if failed_symbols:
            logger.info(f"  Failed symbols          : {failed_symbols}")
        logger.info(f"  Total API calls         : {RATE_LIMITER.total_calls}")
        logger.info(f"  Output directory        : {CONFIG['OUTPUT_DIR']}/")
        logger.info("=" * 72)

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _save_checkpoint(self, panels: List[pd.DataFrame], n: int) -> None:
        try:
            partial = pd.concat(panels, ignore_index=False).reset_index()
            partial["yearmonth"] = partial["yearmonth"].astype(str)
            path = os.path.join(CONFIG["OUTPUT_DIR"], f"checkpoint_{n:04d}.csv")
            partial.to_csv(path, index=False, encoding="utf-8-sig")
            logger.info(f"  Checkpoint saved → {path}  ({len(partial):,} rows)")
        except Exception as exc:
            logger.warning(f"Checkpoint save failed: {exc}")

    def _export(self, panel: pd.DataFrame) -> None:
        out_dir = CONFIG["OUTPUT_DIR"]

        # ── CSV ───────────────────────────────────────────────────────────
        csv_path = os.path.join(out_dir, "vn_stock_panel.csv")
        panel.to_csv(csv_path, index=False, encoding="utf-8-sig")
        logger.info(f"CSV  → {csv_path}  ({os.path.getsize(csv_path)/1e6:.1f} MB)")

        # ── XLSX ──────────────────────────────────────────────────────────
        xlsx_path = os.path.join(out_dir, "vn_stock_panel.xlsx")
        try:
            with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
                MAX_ROWS = 900_000
                if len(panel) <= MAX_ROWS:
                    panel.to_excel(writer, sheet_name="Panel_Data", index=False)
                else:
                    for i, start in enumerate(range(0, len(panel), MAX_ROWS)):
                        chunk = panel.iloc[start:start + MAX_ROWS]
                        chunk.to_excel(writer, sheet_name=f"Panel_Part{i+1}", index=False)

                # Metadata sheet
                pd.DataFrame({
                    "Field": ["Generated", "Start", "End", "Exchanges",
                              "Stocks", "Rows", "Columns", "Rate_Limit"],
                    "Value": [
                        datetime.now().isoformat(timespec="seconds"),
                        CONFIG["START_DATE"], CONFIG["END_DATE"],
                        ", ".join(CONFIG["EXCHANGES"]),
                        panel["symbol"].nunique() if "symbol" in panel.columns else "N/A",
                        f"{len(panel):,}",
                        len(panel.columns),
                        f"{CONFIG['MAX_REQUESTS_PER_MINUTE']} req/min",
                    ],
                }).to_excel(writer, sheet_name="Metadata", index=False)

                # Column dictionary sheet
                col_dict_rows = [
                    {
                        "column":      col,
                        "description": COLUMN_DESCRIPTIONS.get(col, "Computed variable"),
                        "dtype":       str(panel[col].dtype),
                        "non_null_pct": f"{panel[col].notna().mean()*100:.1f}%",
                    }
                    for col in panel.columns
                ]
                pd.DataFrame(col_dict_rows).to_excel(
                    writer, sheet_name="Column_Dictionary", index=False
                )

            logger.info(f"XLSX → {xlsx_path}")
        except Exception as exc:
            logger.error(f"XLSX export failed: {exc}")

    def _export_unavailable(self) -> None:
        df   = pd.DataFrame(UNAVAILABLE_VARIABLES)
        path = os.path.join(CONFIG["OUTPUT_DIR"], "unavailable_variables.csv")
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info(f"Unavailable variables → {path}")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        VNStockPanelOrchestrator().run()
    except KeyboardInterrupt:
        logger.warning("Collection stopped by user.")
    except Exception as exc:
        logger.critical(f"Fatal error: {exc}\n{traceback.format_exc()}")
        sys.exit(1)