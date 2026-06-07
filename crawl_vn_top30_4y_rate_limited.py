"""
Crawl dữ liệu 30 mã cổ phiếu Việt Nam trong 4 năm bằng vnstock.

Output chính theo panel monthly:
- company_name
- ticker
- month
- closing_price
- monthly_volume_traded
- monthly_return_percent
- shares_outstanding

Ghi chú quan trọng:
- vnstock Quote.history thường trả dữ liệu daily OHLCV.
- Script lấy daily rồi aggregate về monthly:
    closing_price = giá đóng cửa của ngày giao dịch cuối cùng trong tháng
    monthly_volume_traded = tổng volume trong tháng
    monthly_return_percent = phần trăm thay đổi closing_price so với tháng trước
- shares_outstanding lấy từ Company.overview(). Đây thường là snapshot gần nhất,
  không phải historical shares outstanding từng tháng. Vì đời đủ khổ rồi, API miễn phí
  cũng không tự nhiên cho sạch như CRSP.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any

import pandas as pd
from tqdm import tqdm

try:
    from vnstock import Quote, Company
except ImportError as exc:
    raise ImportError(
        "Không import được Quote/Company từ vnstock. "
        "Hãy kiểm tra môi trường ảo và phiên bản vnstock. "
        "Thử: pip install -U vnstock"
    ) from exc


# =========================================================
# CONFIG
# =========================================================

# 4 năm: 20/05/2022 -> 20/05/2026
START_DATE = "2022-05-20"
END_DATE = "2026-05-20"

# Chỉ lấy 30 mã
LIMIT = 30

# Nguồn giá. Nếu VCI bị chặn IP hoặc lỗi, đổi sang KBS nếu bản vnstock của bạn hỗ trợ.
PRICE_SOURCE = "VCI"
COMPANY_SOURCES = ["VCI", "TCBS"]

OUTPUT_CSV = "vnstock_top30_monthly_2022_2026.csv"
OUTPUT_XLSX = "vnstock_top30_monthly_2022_2026.xlsx"
ERROR_FILE = "vnstock_top30_monthly_errors.csv"
MISSING_SHARE_FILE = "vnstock_top30_missing_shares.csv"

# Giới hạn tốc độ request để tránh bị API chặn.
# 59 request/phút nghĩa là cách nhau tối thiểu khoảng 1.017 giây.
REQUESTS_PER_MINUTE = 59
REQUEST_INTERVAL_SECONDS = 60 / REQUESTS_PER_MINUTE

# True: vẫn giữ mã nếu thiếu shares_outstanding, để bạn thấy mã nào thiếu.
# False: loại khỏi output chính các dòng thiếu shares_outstanding.
KEEP_MISSING_SHARES = True

# Danh sách 30 mã vốn hóa lớn, ưu tiên blue-chips/liquid stocks.
# Bạn có thể chỉnh thủ công nếu muốn đúng bộ mẫu nghiên cứu.
FALLBACK_SYMBOLS = [
    "VCB", "BID", "CTG", "TCB", "VPB", "MBB", "ACB", "HDB", "STB", "VIB",
    "LPB", "SHB", "TPB", "SSB", "EIB", "MSB", "OCB",
    "VIC", "VHM", "VRE", "BCM", "KDH", "KBC", "NVL", "PDR", "DXG", "DIG",
    "HPG", "HSG", "NKG",
]



# =========================================================
# RATE LIMITER
# =========================================================

class RateLimiter:
    """
    Bộ giới hạn tốc độ request đơn giản.

    Mục tiêu:
    - Không gửi quá REQUESTS_PER_MINUTE request/phút.
    - Áp dụng cho cả Quote.history() và Company.overview().
    - Chạy tuần tự, chậm nhưng đỡ bị API đá ra ngoài như một vị khách không mời.
    """

    def __init__(self, requests_per_minute: int) -> None:
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute phải lớn hơn 0.")

        self.min_interval = 60 / requests_per_minute
        self.last_request_time: float | None = None
        self.request_count = 0

    def wait(self) -> None:
        now = time.monotonic()

        if self.last_request_time is not None:
            elapsed = now - self.last_request_time
            sleep_for = self.min_interval - elapsed

            if sleep_for > 0:
                time.sleep(sleep_for)

        self.last_request_time = time.monotonic()
        self.request_count += 1


REQUEST_LIMITER = RateLimiter(REQUESTS_PER_MINUTE)


# =========================================================
# UTILS
# =========================================================

def normalize_col_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Tìm tên cột trong DataFrame theo nhiều biến thể tên khác nhau."""
    if df is None or df.empty:
        return None

    normalized_map = {normalize_col_name(col): col for col in df.columns}

    for candidate in candidates:
        key = normalize_col_name(candidate)
        if key in normalized_map:
            return normalized_map[key]

    return None


def safe_first_value(df: pd.DataFrame, candidates: list[str]) -> Any | None:
    if df is None or df.empty:
        return None

    col = find_column(df, candidates)
    if col is None:
        return None

    values = df[col].dropna()
    if values.empty:
        return None

    return values.iloc[0]


def to_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None

    if isinstance(value, str):
        value = value.strip().replace(",", "")
        if value in {"", "nan", "None", "null"}:
            return None

    try:
        return float(value)
    except Exception:
        return None


def normalize_shares_outstanding(value: Any) -> int | None:
    """
    Chuẩn hóa shares outstanding.

    Một số nguồn trả:
    - đơn vị triệu cổ phiếu, ví dụ 5589.1
    - hoặc số cổ phiếu thực, ví dụ 5589100000

    Logic:
    - Nếu value < 1,000,000 thì coi là triệu cổ phiếu.
    - Nếu value >= 1,000,000 thì coi là số cổ phiếu thực.
    """
    value_float = to_float(value)

    if value_float is None or value_float <= 0:
        return None

    if value_float < 1_000_000:
        return int(round(value_float * 1_000_000))

    return int(round(value_float))


def infer_shares_from_market_cap(market_cap_raw: Any, latest_close: float | None) -> int | None:
    """
    Fallback yếu: nếu không có shares_outstanding nhưng có market_cap và latest_close,
    thử suy ra shares = market_cap / price.

    Cảnh báo:
    - market_cap trong dữ liệu Việt Nam có thể có đơn vị khác nhau.
    - Chỉ dùng khi shares_outstanding trực tiếp bị thiếu.
    """
    market_cap = to_float(market_cap_raw)
    price = to_float(latest_close)

    if market_cap is None or price is None or market_cap <= 0 or price <= 0:
        return None

    # Heuristic đơn vị:
    # Nếu market_cap quá nhỏ, nhiều khả năng là tỷ VND. Đổi sang VND bằng * 1e9.
    # Nếu price trong vnstock thường là nghìn VND, đổi sang VND bằng * 1000.
    adjusted_market_cap = market_cap * 1_000_000_000 if market_cap < 1_000_000_000 else market_cap
    adjusted_price = price * 1_000 if price < 1_000 else price

    shares = adjusted_market_cap / adjusted_price

    if shares <= 0:
        return None

    return int(round(shares))


# =========================================================
# SYMBOLS
# =========================================================

def get_top_symbols(limit: int = 30) -> list[str]:
    """Dùng danh sách fallback, không dùng Screener vì nhiều version vnstock không export Screener."""
    print(f"Dùng danh sách fallback {limit} mã vốn hóa lớn.")
    return FALLBACK_SYMBOLS[:limit]


# =========================================================
# COMPANY DATA
# =========================================================

@dataclass
class CompanyInfo:
    ticker: str
    company_name: str | None
    exchange: str | None
    shares_outstanding: int | None
    shares_outstanding_raw: Any | None
    market_cap_raw: Any | None
    company_source: str | None
    shares_outstanding_method: str
    company_error: str | None = None


def get_company_info(symbol: str, latest_close: float | None = None) -> CompanyInfo:
    """Lấy thông tin công ty và số cổ phiếu đang lưu hành, thử qua nhiều source."""
    last_error = None

    for source in COMPANY_SOURCES:
        try:
            company = Company(symbol=symbol, source=source)
            REQUEST_LIMITER.wait()
            overview = company.overview()

            if overview is None or overview.empty:
                continue

            company_name = safe_first_value(overview, [
                "shortName", "organName", "companyName", "name", "ticker"
            ])

            exchange = safe_first_value(overview, [
                "exchange", "exchangeName", "floor", "comGroupCode"
            ])

            outstanding_raw = safe_first_value(overview, [
                "outstandingShare", "outstandingShares", "outstanding_share",
                "sharesOutstanding", "listedShare", "issueShare", "listedShares"
            ])

            market_cap_raw = safe_first_value(overview, [
                "marketCap", "market_cap", "marketCapital", "marketCapitalization"
            ])

            shares = normalize_shares_outstanding(outstanding_raw)
            method = "company_overview_outstanding_share"

            if shares is None:
                shares = infer_shares_from_market_cap(market_cap_raw, latest_close)
                method = "estimated_from_market_cap_and_latest_close" if shares else "missing"

            return CompanyInfo(
                ticker=symbol,
                company_name=company_name,
                exchange=exchange,
                shares_outstanding=shares,
                shares_outstanding_raw=outstanding_raw,
                market_cap_raw=market_cap_raw,
                company_source=source,
                shares_outstanding_method=method,
            )

        except Exception as e:
            last_error = e

    return CompanyInfo(
        ticker=symbol,
        company_name=None,
        exchange=None,
        shares_outstanding=None,
        shares_outstanding_raw=None,
        market_cap_raw=None,
        company_source=None,
        shares_outstanding_method="missing",
        company_error=str(last_error),
    )


# =========================================================
# PRICE DATA
# =========================================================

def get_daily_price(symbol: str) -> pd.DataFrame:
    """Lấy daily close + volume trong giai đoạn config."""
    quote = Quote(symbol=symbol, source=PRICE_SOURCE)

    REQUEST_LIMITER.wait()
    df = quote.history(
        start=START_DATE,
        end=END_DATE,
        interval="1D",
    )

    if df is None or df.empty:
        raise ValueError("Không có dữ liệu giá lịch sử.")

    date_col = find_column(df, ["time", "date", "tradingDate", "trading_date"])
    close_col = find_column(df, ["close", "closePrice", "matchedPrice"])
    volume_col = find_column(df, ["volume", "matchVolume", "totalVolume"])

    if date_col is None:
        raise ValueError(f"Không tìm thấy cột ngày. Columns: {list(df.columns)}")
    if close_col is None:
        raise ValueError(f"Không tìm thấy cột close. Columns: {list(df.columns)}")
    if volume_col is None:
        raise ValueError(f"Không tìm thấy cột volume. Columns: {list(df.columns)}")

    result = df[[date_col, close_col, volume_col]].copy()
    result = result.rename(columns={
        date_col: "trading_date",
        close_col: "close",
        volume_col: "volume",
    })

    result["ticker"] = symbol
    result["trading_date"] = pd.to_datetime(result["trading_date"])
    result["close"] = pd.to_numeric(result["close"], errors="coerce")
    result["volume"] = pd.to_numeric(result["volume"], errors="coerce")

    result = result.dropna(subset=["trading_date", "close", "volume"])
    result = result.sort_values("trading_date")

    return result[["ticker", "trading_date", "close", "volume"]]


def daily_to_monthly(price_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate daily data về monthly.

    Công thức:
    - month = tháng của trading_date
    - closing_price = close cuối cùng trong tháng
    - monthly_volume_traded = tổng volume trong tháng
    - monthly_return_percent = pct_change(closing_price) * 100
    """
    if price_df.empty:
        return price_df

    df = price_df.copy()
    df = df.sort_values(["ticker", "trading_date"])
    df["month"] = df["trading_date"].dt.to_period("M").dt.to_timestamp("M")

    monthly = (
        df.groupby(["ticker", "month"], as_index=False)
        .agg(
            closing_price=("close", "last"),
            monthly_volume_traded=("volume", "sum"),
            trading_days_in_month=("trading_date", "count"),
            first_trading_date=("trading_date", "min"),
            last_trading_date=("trading_date", "max"),
        )
    )

    monthly = monthly.sort_values(["ticker", "month"])
    monthly["monthly_return_percent"] = (
        monthly.groupby("ticker")["closing_price"].pct_change() * 100
    )

    return monthly


# =========================================================
# CRAWL ONE SYMBOL
# =========================================================

def crawl_one_symbol(symbol: str) -> pd.DataFrame:
    daily_df = get_daily_price(symbol)
    monthly_df = daily_to_monthly(daily_df)

    latest_close = None
    if not monthly_df.empty:
        latest_close = monthly_df["closing_price"].dropna().iloc[-1]

    company_info = get_company_info(symbol, latest_close=latest_close)

    monthly_df["company_name"] = company_info.company_name
    monthly_df["exchange"] = company_info.exchange
    monthly_df["shares_outstanding"] = company_info.shares_outstanding
    monthly_df["shares_outstanding_raw"] = company_info.shares_outstanding_raw
    monthly_df["market_cap_raw"] = company_info.market_cap_raw
    monthly_df["company_source"] = company_info.company_source
    monthly_df["price_source"] = PRICE_SOURCE
    monthly_df["shares_outstanding_method"] = company_info.shares_outstanding_method
    monthly_df["shares_outstanding_assumption"] = (
        "Latest reported shares outstanding from Company.overview is applied to all months; "
        "this is not exact historical monthly shares unless the API source itself provides historical values."
    )

    monthly_df = monthly_df[
        [
            "company_name",
            "ticker",
            "month",
            "closing_price",
            "monthly_volume_traded",
            "monthly_return_percent",
            "shares_outstanding",
            "exchange",
            "trading_days_in_month",
            "first_trading_date",
            "last_trading_date",
            "shares_outstanding_raw",
            "market_cap_raw",
            "shares_outstanding_method",
            "shares_outstanding_assumption",
            "company_source",
            "price_source",
        ]
    ]

    return monthly_df


# =========================================================
# VALIDATION / REPORTS
# =========================================================

def build_missing_share_report(final_df: pd.DataFrame) -> pd.DataFrame:
    if final_df.empty:
        return pd.DataFrame()

    report = (
        final_df.groupby("ticker", as_index=False)
        .agg(
            company_name=("company_name", "first"),
            shares_outstanding=("shares_outstanding", "first"),
            shares_outstanding_raw=("shares_outstanding_raw", "first"),
            market_cap_raw=("market_cap_raw", "first"),
            shares_outstanding_method=("shares_outstanding_method", "first"),
            n_months=("month", "nunique"),
        )
    )

    report["is_missing_shares_outstanding"] = report["shares_outstanding"].isna()
    return report.sort_values(["is_missing_shares_outstanding", "ticker"], ascending=[False, True])


def save_outputs(final_df: pd.DataFrame, errors: list[dict[str, Any]]) -> None:
    missing_report = build_missing_share_report(final_df)

    output_df = final_df.copy()
    if not KEEP_MISSING_SHARES:
        output_df = output_df.dropna(subset=["shares_outstanding"])

    output_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        output_df.to_excel(writer, index=False, sheet_name="monthly_panel")
        missing_report.to_excel(writer, index=False, sheet_name="share_report")
        if errors:
            pd.DataFrame(errors).to_excel(writer, index=False, sheet_name="errors")

    missing_report.to_csv(MISSING_SHARE_FILE, index=False, encoding="utf-8-sig")

    if errors:
        pd.DataFrame(errors).to_csv(ERROR_FILE, index=False, encoding="utf-8-sig")


# =========================================================
# MAIN
# =========================================================

def main() -> None:
    print("=" * 80)
    print("CRAWL VNSTOCK MONTHLY DATA: TOP 30, 4 YEARS, SHARES OUTSTANDING")
    print("=" * 80)
    print(f"Date range: {START_DATE} -> {END_DATE}")
    print(f"Limit symbols: {LIMIT}")
    print(f"Price source: {PRICE_SOURCE}")
    print(f"Company sources: {COMPANY_SOURCES}")
    print(f"Request limit: {REQUESTS_PER_MINUTE} requests/minute")
    print(f"Request interval: {REQUEST_INTERVAL_SECONDS:.3f} seconds/request")
    print("=" * 80)

    symbols = get_top_symbols(LIMIT)

    all_data: list[pd.DataFrame] = []
    errors: list[dict[str, Any]] = []

    for symbol in tqdm(symbols, desc="Crawling symbols"):
        try:
            df_symbol = crawl_one_symbol(symbol)
            all_data.append(df_symbol)

        except Exception as e:
            errors.append({"ticker": symbol, "error": str(e)})

    if not all_data:
        print("Không crawl được dữ liệu nào. Một bi kịch nhỏ, nhưng vẫn là bi kịch.")
        if errors:
            pd.DataFrame(errors).to_csv(ERROR_FILE, index=False, encoding="utf-8-sig")
        return

    final_df = pd.concat(all_data, ignore_index=True)
    final_df = final_df.sort_values(["ticker", "month"], ascending=[True, True])

    save_outputs(final_df, errors)

    n_tickers = final_df["ticker"].nunique()
    n_rows = len(final_df)
    missing_share_tickers = (
        final_df.loc[final_df["shares_outstanding"].isna(), "ticker"]
        .drop_duplicates()
        .tolist()
    )

    print("\nHoàn tất.")
    print(f"Số mã yêu cầu: {len(symbols)}")
    print(f"Số mã crawl được: {n_tickers}")
    print(f"Số dòng monthly panel: {n_rows}")
    print(f"Số mã lỗi: {len(errors)}")
    print(f"Số mã thiếu shares_outstanding: {len(missing_share_tickers)}")

    if missing_share_tickers:
        print("Mã thiếu shares_outstanding:", ", ".join(missing_share_tickers))

    print(f"File CSV: {OUTPUT_CSV}")
    print(f"File Excel: {OUTPUT_XLSX}")
    print(f"File báo cáo shares: {MISSING_SHARE_FILE}")

    if errors:
        print(f"File lỗi: {ERROR_FILE}")


if __name__ == "__main__":
    main()
