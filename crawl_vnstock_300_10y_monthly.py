"""
Best-effort VNStock crawler for 300 Vietnamese listed firms, 10 years, monthly panel.

Outputs:
- output/vnstock_300_10y_monthly_raw.csv
- output/vnstock_300_10y_monthly_features.csv
- output/vnstock_300_10y_monthly_features.xlsx
- output/vnstock_feature_notes.csv
- output/vnstock_run_log.csv

Install:
    pip install -U vnstock pandas numpy openpyxl tqdm requests

Notes:
- API signatures in vnstock may differ by version/source. This script uses defensive wrappers.
- It respects 59 requests/minute and ~2998 requests/hour through a shared rate limiter.
- Some factors require macro data or unavailable accounting fields. They are skipped and recorded in notes.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm


# =========================
# CONFIG
# =========================
DEFAULT_OUTPUT_DIR = "output"
DEFAULT_YEARS = 10
DEFAULT_N_SYMBOLS = 300
REQ_PER_MIN = 59
REQ_PER_HOUR = 2998
VNINDEX_SYMBOL = "VNINDEX"
RISK_FREE_MONTHLY = 0.0  # nếu không có T-bill rate thì để 0, hoặc tự merge sau.


# =========================
# RATE LIMITER
# =========================
class RateLimiter:
    def __init__(self, per_minute: int = 59, per_hour: int = 2998):
        self.per_minute = per_minute
        self.per_hour = per_hour
        self.minute_calls: deque[float] = deque()
        self.hour_calls: deque[float] = deque()

    def wait(self):
        now = time.time()
        while self.minute_calls and now - self.minute_calls[0] >= 60:
            self.minute_calls.popleft()
        while self.hour_calls and now - self.hour_calls[0] >= 3600:
            self.hour_calls.popleft()

        sleep_for = 0.0
        if len(self.minute_calls) >= self.per_minute:
            sleep_for = max(sleep_for, 60 - (now - self.minute_calls[0]) + 0.1)
        if len(self.hour_calls) >= self.per_hour:
            sleep_for = max(sleep_for, 3600 - (now - self.hour_calls[0]) + 0.1)
        if sleep_for > 0:
            time.sleep(sleep_for)

        now = time.time()
        self.minute_calls.append(now)
        self.hour_calls.append(now)


def safe_call(fn: Callable[[], Any], limiter: RateLimiter, label: str, log_rows: list, retries: int = 3, sleep: float = 2.0) -> Any:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            limiter.wait()
            return fn()
        except Exception as e:
            last_err = e
            wait_time = sleep * attempt
            log_rows.append({"time": datetime.now().isoformat(), "label": label, "status": "retry", "attempt": attempt, "error": repr(e)})
            time.sleep(wait_time)
    log_rows.append({"time": datetime.now().isoformat(), "label": label, "status": "failed", "attempt": retries, "error": repr(last_err)})
    return None


# =========================
# VNSTOCK ADAPTERS
# =========================
def import_vnstock_objects():
    """Import common vnstock classes across versions."""
    try:
        from vnstock import Vnstock  # type: ignore
        return {"Vnstock": Vnstock}
    except Exception:
        pass

    objs: Dict[str, Any] = {}
    try:
        from vnstock import Quote, Company, Finance, Listing  # type: ignore
        objs.update({"Quote": Quote, "Company": Company, "Finance": Finance, "Listing": Listing})
    except Exception:
        try:
            from vnstock import Quote, Company  # type: ignore
            objs.update({"Quote": Quote, "Company": Company})
        except Exception as e:
            raise ImportError("Không import được vnstock. Hãy chạy: pip install -U vnstock") from e
    return objs


def get_stock_handle(symbol: Optional[str] = None, source: str = "VCI"):
    objs = import_vnstock_objects()
    if "Vnstock" in objs:
        v = objs["Vnstock"]()
        if symbol is None:
            return v
        # vnstock3 style
        try:
            return v.stock(symbol=symbol, source=source)
        except TypeError:
            return v.stock(symbol=symbol)
    return objs


def try_methods(obj: Any, candidates: List[Tuple[str, Dict[str, Any]]]) -> Optional[pd.DataFrame]:
    """Try method names with kwargs; return DataFrame if possible."""
    for name, kwargs in candidates:
        try:
            target = obj
            parts = name.split(".")
            for p in parts:
                target = getattr(target, p)
            res = target(**kwargs)
            if isinstance(res, pd.DataFrame):
                return res.copy()
            if isinstance(res, list):
                return pd.DataFrame(res)
            if isinstance(res, dict):
                return pd.DataFrame([res])
        except Exception:
            continue
    return None


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]
    return df


def first_col(df: pd.DataFrame, names: Iterable[str]) -> Optional[str]:
    lower = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    for c in df.columns:
        cc = c.lower()
        for n in names:
            if n.lower() in cc:
                return c
    return None


def get_all_symbols(limiter: RateLimiter, n: int, log_rows: list, source: str = "VCI") -> List[str]:
    objs = import_vnstock_objects()
    df = None

    if "Vnstock" in objs:
        v = get_stock_handle(None, source)
        candidates = [
            ("stock.listing.all_symbols", {}),
            ("stock.listing.symbols_by_exchange", {}),
            ("listing.all_symbols", {}),
        ]
        df = safe_call(lambda: try_methods(v, candidates), limiter, "listing", log_rows)
    else:
        Listing = objs.get("Listing")
        if Listing:
            try:
                listing = Listing()
                df = safe_call(lambda: try_methods(listing, [("all_symbols", {}), ("symbols_by_exchange", {})]), limiter, "listing", log_rows)
            except Exception:
                df = None

    if df is None or df.empty:
        raise RuntimeError("Không lấy được danh sách mã. Hãy cung cấp --symbols-csv có cột ticker/symbol.")

    df = normalize_columns(df)
    sym_col = first_col(df, ["symbol", "ticker", "code"])
    if sym_col is None:
        raise RuntimeError(f"Không tìm thấy cột mã cổ phiếu trong listing: {df.columns.tolist()}")

    symbols = (
        df[sym_col]
        .dropna()
        .astype(str)
        .str.upper()
        .str.strip()
        .drop_duplicates()
        .tolist()
    )

    # Try to prioritize listed common stocks by exchange/type if available.
    bad = {"VNINDEX", "HNXINDEX", "UPCOMINDEX", "VN30", "HNX30"}
    symbols = [s for s in symbols if s and s not in bad and len(s) <= 6]
    return symbols[:n]


def get_price_history(symbol: str, start: str, end: str, limiter: RateLimiter, log_rows: list, source: str = "VCI") -> pd.DataFrame:
    def _call():
        stock = get_stock_handle(symbol, source)
        candidates = [
            ("quote.history", {"start": start, "end": end, "interval": "1D"}),
            ("quote.history", {"start": start, "end": end}),
            ("history", {"symbol": symbol, "start": start, "end": end, "interval": "1D"}),
            ("historical_data", {"symbol": symbol, "start_date": start, "end_date": end}),
        ]
        if isinstance(stock, dict):
            Quote = stock.get("Quote")
            q = Quote(symbol=symbol, source=source) if Quote else None
            return try_methods(q, candidates) if q is not None else None
        return try_methods(stock, candidates)

    df = safe_call(_call, limiter, f"price:{symbol}", log_rows)
    if df is None or df.empty:
        return pd.DataFrame()
    df = normalize_columns(df)
    date_col = first_col(df, ["time", "date", "trading_date"])
    close_col = first_col(df, ["close", "close_price", "closing_price"])
    vol_col = first_col(df, ["volume", "match_volume", "trading_volume"])
    if date_col is None or close_col is None:
        return pd.DataFrame()
    out = pd.DataFrame({
        "ticker": symbol,
        "date": pd.to_datetime(df[date_col], errors="coerce"),
        "close": pd.to_numeric(df[close_col], errors="coerce"),
        "volume": pd.to_numeric(df[vol_col], errors="coerce") if vol_col else np.nan,
    })
    out = out.dropna(subset=["date", "close"]).sort_values("date")
    return out


def get_financials(symbol: str, limiter: RateLimiter, log_rows: list, source: str = "VCI") -> Dict[str, pd.DataFrame]:
    def _call_fin(methods: List[Tuple[str, Dict[str, Any]]], label: str):
        def inner():
            stock = get_stock_handle(symbol, source)
            if isinstance(stock, dict):
                Finance = stock.get("Finance")
                if Finance is None:
                    return None
                try:
                    f = Finance(symbol=symbol, period="quarter", source=source)
                except Exception:
                    f = Finance(symbol=symbol)
                return try_methods(f, methods)
            return try_methods(stock, methods)
        return safe_call(inner, limiter, f"{label}:{symbol}", log_rows)

    stm = _call_fin([
        ("finance.balance_sheet", {"period": "quarter", "lang": "en"}),
        ("finance.balance_sheet", {"period": "quarter"}),
        ("balance_sheet", {"period": "quarter", "lang": "en"}),
        ("balance_sheet", {"period": "quarter"}),
    ], "balance_sheet")
    inc = _call_fin([
        ("finance.income_statement", {"period": "quarter", "lang": "en"}),
        ("finance.income_statement", {"period": "quarter"}),
        ("income_statement", {"period": "quarter", "lang": "en"}),
        ("income_statement", {"period": "quarter"}),
    ], "income_statement")
    cf = _call_fin([
        ("finance.cash_flow", {"period": "quarter", "lang": "en"}),
        ("finance.cash_flow", {"period": "quarter"}),
        ("cash_flow", {"period": "quarter", "lang": "en"}),
        ("cash_flow", {"period": "quarter"}),
    ], "cash_flow")
    ratio = _call_fin([
        ("finance.ratio", {"period": "quarter", "lang": "en"}),
        ("finance.ratio", {"period": "quarter"}),
        ("ratio", {"period": "quarter", "lang": "en"}),
        ("ratio", {"period": "quarter"}),
    ], "ratio")

    result = {}
    for k, v in {"balance_sheet": stm, "income_statement": inc, "cash_flow": cf, "ratio": ratio}.items():
        if isinstance(v, pd.DataFrame) and not v.empty:
            result[k] = normalize_columns(v)
        else:
            result[k] = pd.DataFrame()
    return result


def get_company_profile(symbol: str, limiter: RateLimiter, log_rows: list, source: str = "VCI") -> pd.DataFrame:
    def _call():
        stock = get_stock_handle(symbol, source)
        candidates = [
            ("company.overview", {}),
            ("company.profile", {}),
            ("company.info", {}),
            ("overview", {"symbol": symbol}),
            ("profile", {"symbol": symbol}),
        ]
        if isinstance(stock, dict):
            Company = stock.get("Company")
            c = Company(symbol=symbol, source=source) if Company else None
            return try_methods(c, candidates) if c is not None else None
        return try_methods(stock, candidates)
    df = safe_call(_call, limiter, f"profile:{symbol}", log_rows)
    return normalize_columns(df) if isinstance(df, pd.DataFrame) and not df.empty else pd.DataFrame()


# =========================
# DATA TRANSFORM
# =========================
def daily_to_monthly(px: pd.DataFrame) -> pd.DataFrame:
    if px.empty:
        return px
    px = px.copy().sort_values("date")
    px["month"] = px["date"].dt.to_period("M").dt.to_timestamp("M")
    g = px.groupby(["ticker", "month"], as_index=False)
    m = g.agg(
        stock_price=("close", "last"),
        monthly_volume=("volume", "sum"),
        trading_days=("date", "count"),
        max_daily_return=("close", lambda s: s.pct_change().max()),
        stock_variance=("close", lambda s: s.pct_change().var()),
        return_volatility=("close", lambda s: s.pct_change().std()),
        zero_trading_frequency=("volume", lambda s: np.mean(pd.to_numeric(s, errors="coerce").fillna(0) == 0)),
    )
    m["monthly_return"] = m.groupby("ticker")["stock_price"].pct_change()
    m["dollar_volume"] = m["stock_price"] * m["monthly_volume"]
    return m


def standardize_period(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    year_col = first_col(df, ["year", "report_year", "fiscal_year"])
    q_col = first_col(df, ["quarter", "report_quarter"])
    date_col = first_col(df, ["date", "report_date", "period", "time"])
    if date_col:
        df["period_date"] = pd.to_datetime(df[date_col], errors="coerce")
    elif year_col and q_col:
        y = pd.to_numeric(df[year_col], errors="coerce")
        q = pd.to_numeric(df[q_col], errors="coerce")
        month = q.map({1: 3, 2: 6, 3: 9, 4: 12})
        df["period_date"] = pd.to_datetime(dict(year=y, month=month, day=1), errors="coerce") + pd.offsets.MonthEnd(0)
    elif year_col:
        y = pd.to_numeric(df[year_col], errors="coerce")
        df["period_date"] = pd.to_datetime(dict(year=y, month=12, day=31), errors="coerce")
    else:
        df["period_date"] = pd.NaT
    df = df.dropna(subset=["period_date"]).sort_values("period_date")
    return df


def pick_num(df: pd.DataFrame, names: List[str]) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    col = first_col(df, names)
    if col is None:
        return pd.Series(np.nan, index=df.index)
    return pd.to_numeric(df[col], errors="coerce")


def build_accounting_panel(symbol: str, fin: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    bs = standardize_period(fin.get("balance_sheet", pd.DataFrame()))
    inc = standardize_period(fin.get("income_statement", pd.DataFrame()))
    cf = standardize_period(fin.get("cash_flow", pd.DataFrame()))
    ratio = standardize_period(fin.get("ratio", pd.DataFrame()))

    # Keep one row per quarter/end period, then outer merge.
    base = pd.DataFrame({"period_date": sorted(set(pd.concat([x["period_date"] for x in [bs, inc, cf, ratio] if not x.empty], ignore_index=True).dropna()))}) if any(not x.empty for x in [bs, inc, cf, ratio]) else pd.DataFrame(columns=["period_date"])
    if base.empty:
        return base
    base["ticker"] = symbol

    def attach(prefix: str, df: pd.DataFrame, cols: Dict[str, List[str]]) -> pd.DataFrame:
        out = pd.DataFrame({"period_date": df["period_date"]}) if not df.empty else pd.DataFrame(columns=["period_date"])
        for new, aliases in cols.items():
            out[new] = pick_num(df, aliases) if not df.empty else np.nan
        return out.groupby("period_date", as_index=False).last()

    bs_cols = {
        "cash": ["cash", "cash_and_cash_equivalents", "cash_&_cash_equivalents", "cash_short_term_investments"],
        "current_assets": ["current_assets", "total_current_assets"],
        "current_liabilities": ["current_liabilities", "total_current_liabilities"],
        "inventory": ["inventory", "inventories"],
        "receivables": ["accounts_receivable", "short_term_receivables", "receivables"],
        "total_assets": ["total_assets", "assets"],
        "total_liabilities": ["total_liabilities", "liabilities"],
        "equity": ["owner_equity", "shareholder_equity", "total_equity", "equity"],
        "long_term_debt": ["long_term_debt", "long_term_borrowings", "non_current_borrowings"],
        "ppe": ["property_plant_equipment", "fixed_assets", "tangible_fixed_assets"],
        "treasury_stock": ["treasury_stock", "treasury_shares"],
        "shares_outstanding": ["shares_outstanding", "outstanding_share", "issue_share", "listed_shares"],
    }
    inc_cols = {
        "sales": ["revenue", "net_revenue", "sales", "total_revenue"],
        "gross_profit": ["gross_profit"],
        "operating_profit": ["operating_profit", "ebit", "profit_from_operating_activities"],
        "net_income": ["net_income", "profit_after_tax", "net_profit_after_tax"],
        "sga": ["selling_general_admin", "selling_expenses", "admin_expenses", "sga"],
        "interest_expense": ["interest_expense"],
        "tax_expense": ["tax_expense", "corporate_income_tax"],
        "pretax_income": ["profit_before_tax", "income_before_tax", "pretax_income"],
        "depreciation": ["depreciation", "depreciation_and_amortization"],
        "eps": ["eps", "earnings_per_share"],
    }
    cf_cols = {
        "operating_cashflow": ["cash_flow_from_operating_activities", "net_cash_flow_from_operating_activities", "operating_cash_flow", "cfo"],
        "capex": ["capital_expenditure", "purchase_of_fixed_assets", "capex"],
        "dividend_paid": ["dividends_paid", "dividend_paid"],
        "depreciation_cf": ["depreciation", "depreciation_and_amortization"],
        "stock_issuance": ["proceeds_from_stock_issuance", "issuance_of_shares"],
        "stock_repurchase": ["repurchase_of_stock", "purchase_of_treasury_stock"],
    }
    ratio_cols = {
        "roe": ["roe", "return_on_equity"],
        "roa": ["roa", "return_on_assets"],
        "current_ratio": ["current_ratio"],
        "quick_ratio": ["quick_ratio"],
        "debt_to_equity": ["debt_to_equity", "leverage"],
        "dividend_yield": ["dividend_yield"],
        "book_value_per_share": ["book_value_per_share", "bvps"],
        "pe": ["pe", "price_to_earning"],
        "pb": ["pb", "price_to_book"],
    }

    for tbl, cols in [(bs, bs_cols), (inc, inc_cols), (cf, cf_cols), (ratio, ratio_cols)]:
        if tbl.empty:
            continue
        base = base.merge(attach("", tbl, cols), on="period_date", how="left")

    base = base.sort_values("period_date")
    base["month"] = base["period_date"].dt.to_period("M").dt.to_timestamp("M")

    # Direct ratios/fallback calculations
    base["current_ratio_calc"] = base.get("current_assets", np.nan) / base.get("current_liabilities", np.nan)
    base["quick_ratio_calc"] = (base.get("current_assets", np.nan) - base.get("inventory", np.nan)) / base.get("current_liabilities", np.nan)
    base["leverage"] = base.get("total_liabilities", np.nan) / base.get("total_assets", np.nan)
    base["roe_calc"] = base.get("net_income", np.nan) / base.get("equity", np.nan)
    base["roa_calc"] = base.get("net_income", np.nan) / base.get("total_assets", np.nan)
    base["return_on_invested_capital"] = base.get("operating_profit", np.nan) * (1 - 0.2) / (base.get("equity", np.nan) + base.get("long_term_debt", np.nan) - base.get("cash", np.nan))
    base["operating_profitability"] = base.get("operating_profit", np.nan) / base.get("equity", np.nan)
    base["gross_profitability"] = base.get("gross_profit", np.nan) / base.get("total_assets", np.nan)
    base["earnings_growth"] = base.groupby("ticker")["net_income"].pct_change(4)
    base["asset_growth"] = base.groupby("ticker")["total_assets"].pct_change(4)
    base["sales_growth"] = base.groupby("ticker")["sales"].pct_change(4)
    base["inventory_growth"] = base.groupby("ticker")["inventory"].pct_change(4)
    base["capex_growth"] = base.groupby("ticker")["capex"].pct_change(4)
    base["long_term_debt_growth"] = base.groupby("ticker")["long_term_debt"].pct_change(4)
    base["current_ratio_growth"] = base.groupby("ticker")["current_ratio_calc"].pct_change(4)
    base["quick_ratio_growth"] = base.groupby("ticker")["quick_ratio_calc"].pct_change(4)
    base["depreciation_growth"] = base.groupby("ticker")["depreciation"].pct_change(4) if "depreciation" in base else np.nan
    base["investment_rate"] = base.get("capex", np.nan) / base.get("total_assets", np.nan)
    base["corporate_investment"] = base.groupby("ticker")["total_assets"].diff(4) / base.groupby("ticker")["total_assets"].shift(4)
    base["sales_to_inventory"] = base.get("sales", np.nan) / base.get("inventory", np.nan)
    base["cashflow_to_debt"] = base.get("operating_cashflow", np.nan) / base.get("total_liabilities", np.nan)
    base["sales_to_cash"] = base.get("sales", np.nan) / base.get("cash", np.nan)
    base["sales_to_receivables"] = base.get("sales", np.nan) / base.get("receivables", np.nan)
    base["asset_tangibility"] = base.get("ppe", np.nan) / base.get("total_assets", np.nan)
    base["tax_burden"] = base.get("net_income", np.nan) / base.get("pretax_income", np.nan)
    base["total_accruals"] = base.get("net_income", np.nan) - base.get("operating_cashflow", np.nan)
    base["absolute_accruals"] = base["total_accruals"].abs()
    base["percent_accruals"] = base["total_accruals"] / base.get("total_assets", np.nan)
    base["sales_inventory_change"] = base.groupby("ticker")["sales_to_inventory"].diff(4)
    base["sales_minus_inventory_growth"] = base["sales_growth"] - base["inventory_growth"]
    base["sales_minus_receivables_growth"] = base["sales_growth"] - base.groupby("ticker")["receivables"].pct_change(4)
    base["sales_minus_sga_growth"] = base["sales_growth"] - base.groupby("ticker")["sga"].pct_change(4)
    base["gross_margin"] = base.get("gross_profit", np.nan) / base.get("sales", np.nan)
    base["gross_margin_minus_sales_growth"] = base.groupby("ticker")["gross_margin"].diff(4) - base["sales_growth"]
    base["dividend_omission"] = ((base.groupby("ticker")["dividend_paid"].shift(4).fillna(0) < 0) & (base["dividend_paid"].fillna(0) == 0)).astype(float) if "dividend_paid" in base else np.nan
    base["dividend_initiation"] = ((base.groupby("ticker")["dividend_paid"].shift(4).fillna(0) == 0) & (base["dividend_paid"].fillna(0) < 0)).astype(float) if "dividend_paid" in base else np.nan
    base["long_term_operating_assets_growth"] = base.groupby("ticker")["ppe"].pct_change(4) if "ppe" in base else np.nan
    base["net_equity_issuance"] = (base.get("stock_issuance", np.nan).fillna(0) - base.get("stock_repurchase", np.nan).fillna(0)) / base.get("market_cap", np.nan) if "market_cap" in base else np.nan

    # streak of YoY earnings increases
    inc_bool = base["net_income"] > base.groupby("ticker")["net_income"].shift(4)
    streak = []
    current = 0
    for val in inc_bool.fillna(False):
        current = current + 1 if val else 0
        streak.append(current)
    base["earnings_increase_streak"] = streak
    return base


def merge_monthly_accounting(price_m: pd.DataFrame, acc_q: pd.DataFrame) -> pd.DataFrame:
    if price_m.empty:
        return pd.DataFrame()
    if acc_q.empty:
        return price_m
    price_m = price_m.sort_values(["ticker", "month"])
    acc_q = acc_q.sort_values(["ticker", "month"])
    out = []
    for ticker, g in price_m.groupby("ticker"):
        a = acc_q[acc_q["ticker"] == ticker].sort_values("month")
        if a.empty:
            out.append(g)
        else:
            out.append(pd.merge_asof(g.sort_values("month"), a.drop(columns=["period_date"], errors="ignore").sort_values("month"), on="month", by="ticker", direction="backward"))
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def add_market_factors(df: pd.DataFrame, market_m: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy().sort_values(["ticker", "month"])
    market = market_m[["month", "monthly_return"]].rename(columns={"monthly_return": "market_return"}) if not market_m.empty else pd.DataFrame(columns=["month", "market_return"])
    df = df.merge(market, on="month", how="left")
    df["excess_return"] = df["monthly_return"] - RISK_FREE_MONTHLY
    df["market_excess_return"] = df["market_return"] - RISK_FREE_MONTHLY

    # Rolling beta 36 months, idio vol from residuals
    def calc_beta(g):
        g = g.sort_values("month").copy()
        beta_vals, idio_vals, delay_vals = [], [], []
        for i in range(len(g)):
            w = g.iloc[max(0, i - 35): i + 1]
            if len(w) >= 24 and w["market_excess_return"].notna().sum() >= 24 and w["excess_return"].notna().sum() >= 24:
                x = w["market_excess_return"].astype(float).values
                y = w["excess_return"].astype(float).values
                mask = np.isfinite(x) & np.isfinite(y)
                if mask.sum() >= 24 and np.nanvar(x[mask]) > 0:
                    b = np.cov(y[mask], x[mask])[0, 1] / np.var(x[mask])
                    beta_vals.append(b)
                    resid = y[mask] - (np.nanmean(y[mask]) + b * (x[mask] - np.nanmean(x[mask])))
                    idio_vals.append(np.nanstd(resid))
                else:
                    beta_vals.append(np.nan); idio_vals.append(np.nan)
            else:
                beta_vals.append(np.nan); idio_vals.append(np.nan)
            delay_vals.append(np.nan)  # price delay requires lagged-market-return regression; left for specialized model
        g["beta"] = beta_vals
        g["idiosyncratic_volatility"] = idio_vals
        g["price_delay"] = delay_vals
        return g

    df = df.groupby("ticker", group_keys=False).apply(calc_beta)
    df["beta_squared"] = df["beta"] ** 2
    return df


def add_panel_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy().sort_values(["ticker", "month"])

    # Market cap/share outstanding fallback. If shares missing, these remain NaN.
    if "shares_outstanding" in df.columns:
        df["market_cap"] = df["stock_price"] * df["shares_outstanding"]
    elif "market_cap" not in df.columns:
        df["market_cap"] = np.nan

    df["turnover"] = df["monthly_volume"] / df.get("shares_outstanding", np.nan)
    df["book_to_market"] = df.get("equity", np.nan) / df["market_cap"]
    df["earnings_to_price"] = df.get("net_income", np.nan) / df["market_cap"]
    df["cashflow_to_price"] = df.get("operating_cashflow", np.nan) / df["market_cap"]
    df["cash_to_price"] = df.get("cash", np.nan) / df["market_cap"]
    df["sales_to_price"] = df.get("sales", np.nan) / df["market_cap"]
    df["dividend_to_price"] = (-df.get("dividend_paid", np.nan)) / df["market_cap"]
    df["rd_to_market_cap"] = np.nan  # R&D rarely available in VN financial statements

    # Momentum
    for n in [6, 12, 36]:
        df[f"momentum_{n}m"] = df.groupby("ticker")["stock_price"].pct_change(n)
    df["momentum_change"] = df["momentum_6m"] - df["momentum_12m"]
    df["maximum_return"] = df.groupby("ticker")["monthly_return"].transform(lambda s: s.rolling(12, min_periods=6).max())
    df["turnover_volatility"] = df.groupby("ticker")["turnover"].transform(lambda s: s.rolling(12, min_periods=6).std())
    df["dollar_volume_volatility"] = df.groupby("ticker")["dollar_volume"].transform(lambda s: s.rolling(12, min_periods=6).std())
    df["accrual_volatility"] = df.groupby("ticker")["percent_accruals"].transform(lambda s: s.rolling(12, min_periods=6).std()) if "percent_accruals" in df else np.nan
    df["cashflow_volatility"] = df.groupby("ticker")["operating_cashflow"].transform(lambda s: s.rolling(12, min_periods=6).std()) if "operating_cashflow" in df else np.nan
    df["roa_volatility"] = df.groupby("ticker")["roa_calc"].transform(lambda s: s.rolling(12, min_periods=6).std()) if "roa_calc" in df else np.nan
    df["amihud_illiquidity"] = df["monthly_return"].abs() / df["dollar_volume"].replace(0, np.nan)

    # Industry adjusted and industry momentum if industry_code exists.
    if "industry_code" in df.columns:
        ind_keys = ["industry_code", "month"]
        for col, new in [
            ("book_to_market", "industry_adjusted_book_to_market"),
            ("cashflow_to_price", "industry_adjusted_cashflow_to_price"),
            ("capex_growth", "industry_adjusted_capex_growth"),
        ]:
            if col in df:
                df[new] = df[col] - df.groupby(ind_keys)[col].transform("median")
        df["industry_momentum"] = df.groupby(ind_keys)["momentum_12m"].transform("mean")
        # HHI using market share in sales by industry-month
        df["market_share"] = df.get("sales", np.nan) / df.groupby(ind_keys)["sales"].transform("sum") if "sales" in df else np.nan
        df["industry_concentration"] = df.groupby(ind_keys)["market_share"].transform(lambda s: np.nansum(np.square(s))) if "market_share" in df else np.nan
        df["industry_adjusted_asset_turnover_change"] = np.nan
        if "sales" in df and "total_assets" in df:
            df["asset_turnover"] = df["sales"] / df["total_assets"]
            df["asset_turnover_change"] = df.groupby("ticker")["asset_turnover"].diff(12)
            df["industry_adjusted_asset_turnover_change"] = df["asset_turnover_change"] - df.groupby(ind_keys)["asset_turnover_change"].transform("median")
    else:
        for c in ["industry_adjusted_book_to_market", "industry_adjusted_cashflow_to_price", "industry_adjusted_capex_growth", "industry_momentum", "market_share", "industry_concentration", "industry_adjusted_asset_turnover_change"]:
            df[c] = np.nan

    # Macro placeholders
    df["treasury_bill_rate"] = np.nan
    df["term_spread"] = np.nan
    df["default_yield_spread"] = np.nan
    return df


def enrich_profile_fields(panel: pd.DataFrame, profiles: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for ticker, p in profiles.items():
        industry_code = np.nan
        listing_date = pd.NaT
        company_name = np.nan
        if p is not None and not p.empty:
            p = normalize_columns(p)
            industry_col = first_col(p, ["industry_code", "icb_code", "industry", "industry_id", "com_group_code"])
            name_col = first_col(p, ["company_name", "short_name", "organ_name", "name"])
            date_col = first_col(p, ["listing_date", "listed_date", "established_date", "founding_date"])
            if industry_col: industry_code = p[industry_col].dropna().iloc[0] if p[industry_col].dropna().size else np.nan
            if name_col: company_name = p[name_col].dropna().iloc[0] if p[name_col].dropna().size else np.nan
            if date_col: listing_date = pd.to_datetime(p[date_col].dropna().iloc[0], errors="coerce") if p[date_col].dropna().size else pd.NaT
        rows.append({"ticker": ticker, "industry_code": industry_code, "listing_date": listing_date, "company_name": company_name})
    meta = pd.DataFrame(rows)
    out = panel.merge(meta, on="ticker", how="left")
    out["company_age"] = (out["month"] - out["listing_date"]).dt.days / 365.25
    return out


def build_notes() -> pd.DataFrame:
    rows = [
        ("stock_price", "Lấy trực tiếp từ lịch sử giá, cuối tháng.", "direct/calc"),
        ("monthly_return", "Tính từ stock_price theo tháng.", "calculated"),
        ("cash/current_ratio/quick_ratio/leverage/inventory/depreciation/ROE/ROA", "Lấy trực tiếp nếu vnstock ratio/BCTC có cột; nếu thiếu thì tính từ BCTC.", "direct_or_calculated"),
        ("turnover", "monthly_volume / shares_outstanding. Nếu thiếu shares_outstanding thì NaN.", "calculated"),
        ("dollar_volume", "stock_price * monthly_volume.", "calculated"),
        ("market_cap", "stock_price * shares_outstanding. Nếu không có shares_outstanding thì không tính được.", "calculated"),
        ("beta/idiosyncratic_volatility/beta_squared", "Tính rolling 36 tháng so với VNINDEX. Cần đủ dữ liệu thị trường.", "calculated"),
        ("book_to_market/earnings_to_price/cashflow_to_price/cash_to_price/sales_to_price/dividend_to_price", "Tính từ BCTC và market_cap.", "calculated"),
        ("industry_adjusted_*", "Tính bằng cách trừ median cùng industry_code và tháng; nếu thiếu industry_code thì NaN.", "calculated_if_industry_available"),
        ("momentum_6m/12m/36m/momentum_change/maximum_return/return_volatility", "Tính từ giá và lợi suất tháng.", "calculated"),
        ("amihud_illiquidity", "abs(monthly_return)/dollar_volume.", "calculated"),
        ("zero_trading_frequency", "Tỷ lệ ngày volume bằng 0 trong tháng.", "calculated"),
        ("treasury_bill_rate/term_spread/default_yield_spread", "Vnstock thường không cung cấp chuỗi macro yield đầy đủ; script tạo cột NaN để merge nguồn ngoài sau.", "not_available_in_vnstock"),
        ("rd_to_market_cap", "Chi phí R&D thường không tách riêng trong BCTC VN qua vnstock; để NaN nếu không có cột.", "usually_not_available"),
        ("price_delay", "Cần mô hình hồi quy lagged market return chuyên biệt; script để NaN để tránh tính sai.", "not_implemented"),
        ("net_equity_issuance", "Tính nếu cash flow có issuance/repurchase; nhiều mã VN sẽ thiếu.", "conditional"),
        ("Company Age", "Ưu tiên listing_date/established_date từ profile; nếu thiếu thì NaN.", "conditional"),
    ]
    return pd.DataFrame(rows, columns=["feature", "note", "status"])


# =========================
# MAIN
# =========================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols-csv", default=None, help="CSV có cột ticker/symbol. Nếu bỏ trống sẽ lấy listing từ vnstock.")
    parser.add_argument("--n-symbols", type=int, default=DEFAULT_N_SYMBOLS)
    parser.add_argument("--years", type=int, default=DEFAULT_YEARS)
    parser.add_argument("--source", default="VCI", help="Nguồn vnstock, thường VCI hoặc TCBS tùy phiên bản.")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--start", default=None, help="YYYY-MM-DD, mặc định hôm nay - years")
    parser.add_argument("--end", default=None, help="YYYY-MM-DD, mặc định hôm nay")
    args = parser.parse_args()

    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    log_rows: List[Dict[str, Any]] = []
    limiter = RateLimiter(REQ_PER_MIN, REQ_PER_HOUR)

    end_dt = pd.to_datetime(args.end).date() if args.end else date.today()
    start_dt = pd.to_datetime(args.start).date() if args.start else (end_dt - timedelta(days=int(args.years * 365.25)))
    start, end = start_dt.isoformat(), end_dt.isoformat()

    if args.symbols_csv:
        s = pd.read_csv(args.symbols_csv)
        c = first_col(normalize_columns(s), ["ticker", "symbol", "code"])
        if c is None:
            raise RuntimeError("symbols-csv cần cột ticker/symbol/code")
        symbols = s[c].dropna().astype(str).str.upper().str.strip().drop_duplicates().head(args.n_symbols).tolist()
    else:
        symbols = get_all_symbols(limiter, args.n_symbols, log_rows, args.source)

    print(f"Symbols: {len(symbols)} | Period: {start} -> {end} | Limit: {REQ_PER_MIN}/min, {REQ_PER_HOUR}/hour")

    all_prices_d, all_acc, profiles = [], [], {}

    # Market index first for beta.
    vnindex_d = get_price_history(VNINDEX_SYMBOL, start, end, limiter, log_rows, args.source)
    vnindex_m = daily_to_monthly(vnindex_d) if not vnindex_d.empty else pd.DataFrame()

    for symbol in tqdm(symbols, desc="Crawling symbols"):
        px = get_price_history(symbol, start, end, limiter, log_rows, args.source)
        if not px.empty:
            all_prices_d.append(px)
        fin = get_financials(symbol, limiter, log_rows, args.source)
        acc = build_accounting_panel(symbol, fin)
        if not acc.empty:
            all_acc.append(acc)
        profiles[symbol] = get_company_profile(symbol, limiter, log_rows, args.source)

    prices_d = pd.concat(all_prices_d, ignore_index=True) if all_prices_d else pd.DataFrame()
    prices_m = daily_to_monthly(prices_d) if not prices_d.empty else pd.DataFrame()
    acc_q = pd.concat(all_acc, ignore_index=True) if all_acc else pd.DataFrame()

    panel = merge_monthly_accounting(prices_m, acc_q)
    panel = enrich_profile_fields(panel, profiles)
    panel = add_panel_features(panel)
    panel = add_market_factors(panel, vnindex_m)

    # reorder important columns
    first_cols = [
        "ticker", "company_name", "industry_code", "month", "stock_price", "monthly_return", "monthly_volume",
        "dollar_volume", "turnover", "market_cap", "company_age", "cash", "current_ratio", "current_ratio_calc",
        "quick_ratio", "quick_ratio_calc", "leverage", "dividend_yield", "dividend_to_price", "earnings_to_price",
        "book_to_market", "roe", "roe_calc", "roa", "roa_calc", "return_on_invested_capital", "operating_profitability",
        "gross_profitability", "earnings_growth", "earnings_increase_streak", "sales_growth", "inventory"
    ]
    cols = [c for c in first_cols if c in panel.columns] + [c for c in panel.columns if c not in first_cols]
    panel = panel[cols]

    raw_path = outdir / "vnstock_300_10y_monthly_raw.csv"
    feat_csv = outdir / "vnstock_300_10y_monthly_features.csv"
    feat_xlsx = outdir / "vnstock_300_10y_monthly_features.xlsx"
    notes_path = outdir / "vnstock_feature_notes.csv"
    log_path = outdir / "vnstock_run_log.csv"

    prices_m.to_csv(raw_path, index=False, encoding="utf-8-sig")
    panel.to_csv(feat_csv, index=False, encoding="utf-8-sig")
    build_notes().to_csv(notes_path, index=False, encoding="utf-8-sig")
    pd.DataFrame(log_rows).to_csv(log_path, index=False, encoding="utf-8-sig")

    try:
        with pd.ExcelWriter(feat_xlsx, engine="openpyxl") as writer:
            panel.to_excel(writer, index=False, sheet_name="features")
            build_notes().to_excel(writer, index=False, sheet_name="notes")
            pd.DataFrame(log_rows).to_excel(writer, index=False, sheet_name="run_log")
    except Exception as e:
        print(f"Không ghi được XLSX: {e}. CSV vẫn đã được ghi.")

    print("DONE")
    print(f"Raw monthly: {raw_path}")
    print(f"Features CSV: {feat_csv}")
    print(f"Features XLSX: {feat_xlsx}")
    print(f"Notes: {notes_path}")
    print(f"Log: {log_path}")


if __name__ == "__main__":
    main()
