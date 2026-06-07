import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
import json

BASE_PAGE_URL = "https://finance.vietstock.vn/MWG/thong-ke-giao-dich.htm"
API_URL = "https://finance.vietstock.vn/data/getstockdealdetailpaging_v2"

CODE = "MWG"
TOTAL_PAGES = 506
PAGE_SIZE = 18

OUTPUT_CSV = "MWG_giao_dich.csv"
OUTPUT_EXCEL = "MWG_giao_dich.xlsx"


# def get_token(session: requests.Session) -> str:
#     response = session.get(BASE_PAGE_URL, timeout=20)
#     response.raise_for_status()

#     soup = BeautifulSoup(response.text, "html.parser")

#     token_input = soup.find("input", {"name": "__RequestVerificationToken"})

#     if not token_input:
#         raise Exception("Không tìm thấy __RequestVerificationToken trên trang.")

#     return str(token_input["value"])
# token = "9FpEpzMaLmwOA1T0nndWsx2sRWUU0Uxw-NphLjO2wCSbTYd2gF2kBJ6RpvrxgcqR4galC53NHqWsz4DF34PUCClx4-NY-_HPjMUvexixhSg1"

import os

def fetch_page(session: requests.Session, token: str, page: int):
    payload = {
        "code": CODE,
        "page": page,
        "pageSize": PAGE_SIZE,
        "__RequestVerificationToken": token
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://finance.vietstock.vn",
        "Referer": BASE_PAGE_URL,
        "X-Requested-With": "XMLHttpRequest"
    }

    response = session.post(API_URL, data=payload, headers=headers, timeout=20)

    text = response.content.decode("utf-8-sig", errors="replace").strip()

    os.makedirs("debug_response", exist_ok=True)
    with open(f"debug_response/page_{page}.txt", "w", encoding="utf-8") as f:
        f.write("STATUS: " + str(response.status_code) + "\n")
        f.write("CONTENT-TYPE: " + response.headers.get("Content-Type", "") + "\n\n")
        f.write(text)

    if not text:
        raise Exception(f"Page {page}: response rỗng")

    if text.startswith("<"):
        raise Exception(f"Page {page}: API trả HTML, có thể token/session sai")

    return json.loads(text)

def normalize_response(data):
    """
    API có thể trả về dạng:
    - list trực tiếp
    - dict chứa Data / data / rows / Results...
    Hàm này cố tìm list dữ liệu chính.
    """

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        possible_keys = [
            "Data",
            "data",
            "Rows",
            "rows",
            "Results",
            "results",
            "List",
            "list"
        ]

        for key in possible_keys:
            if key in data and isinstance(data[key], list):
                return data[key]

        # Nếu không biết key, tìm list đầu tiên trong dict
        for value in data.values():
            if isinstance(value, list):
                return value

    return []


def main():
    session = requests.Session()

    token = "Cbcph3dawvDk2rwbM8Z-5eUIrPfhNqPkmlszMcvGqjpYJagYZOVU01caQd0Z-pusdSq6_xeIBw-HpFawVHSVyKtNZD9PwtsQ29uKpqf53xg1"
    print("Đã lấy token:", token[:20] + "...")

    all_rows = []

    for page in tqdm(range(1, TOTAL_PAGES + 1), desc="Crawling MWG"):
        try:
            data = fetch_page(session, token, page)
            rows = normalize_response(data)

            if not rows:
                print(f"Page {page}: không có dữ liệu.")
                continue

            for row in rows:
                if isinstance(row, dict):
                    row["page"] = page
                    all_rows.append(row)
                else:
                    all_rows.append({
                        "value": row,
                        "page": page
                    })

            time.sleep(0.3)

        except Exception as e:
            print(f"Lỗi page {page}: {e}")
            time.sleep(2)

            # Thử lấy token mới nếu token/session bị hết hạn
            try:
                token = "Cbcph3dawvDk2rwbM8Z-5eUIrPfhNqPkmlszMcvGqjpYJagYZOVU01caQd0Z-pusdSq6_xeIBw-HpFawVHSVyKtNZD9PwtsQ29uKpqf53xg1"
                data = fetch_page(session, token, page)
                rows = normalize_response(data)

                for row in rows:
                    if isinstance(row, dict):
                        row["page"] = page
                        all_rows.append(row)
                    else:
                        all_rows.append({
                            "value": row,
                            "page": page
                        })

            except Exception as retry_error:
                print(f"Retry page {page} thất bại: {retry_error}")

    df = pd.DataFrame(all_rows)

    print("Tổng số dòng crawl được:", len(df))
    print(df.head())

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    df.to_excel(OUTPUT_EXCEL, index=False)

    print("Đã lưu file:")
    print("-", OUTPUT_CSV)
    print("-", OUTPUT_EXCEL)


if __name__ == "__main__":
    main()