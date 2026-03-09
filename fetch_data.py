import yfinance as yf
import pandas as pd
import json
import os
from datetime import datetime

TICKERS = {
    "MING.OL":  "Sparebank 1 SMN",
    "SRBNK.OL": "Sparebank 1 SR-Bank",
    "SPOL.OL":  "Sparebank 1 Østlandet",
    "NONG.OL":  "Sparebank 1 Nord-Norge",
    "SVEG.OL":  "Sparebanken Vest",
    "MORG.OL":  "Sparebanken Møre",
    "SBVG.OL":  "Sparebank 1 BV",
    "SOR.OL":   "Sparebanken Sør",
    "TOTG.OL":  "Totens Sparebank",
    "JAREN.OL": "Jæren Sparebank",
}
START_DATE = "2010-01-01"
DATA_DIR = "data"


def process_ticker(ticker, name):
    """Fetch and process a single ticker. Returns output dict or None on failure."""
    print(f"Fetching {ticker} ({name})...")

    try:
        t = yf.Ticker(ticker)
        history = t.history(start=START_DATE, auto_adjust=False)
    except Exception as e:
        print(f"  ERROR fetching {ticker}: {e}")
        return None

    if history.empty:
        print(f"  No data returned for {ticker}")
        return None

    # Strip timezone from index
    if history.index.tz is not None:
        history.index = history.index.tz_convert(None)

    prices = history["Close"]
    dividends = history["Dividends"]

    # DRIP calculation: reinvest each dividend at that day's closing price
    shares = 1.0
    records = []

    for date in prices.index:
        close = prices[date]
        div = dividends.get(date, 0.0)

        if pd.isna(close) or close <= 0:
            continue

        if div > 0:
            shares += shares * (div / close)

        records.append(
            {
                "date": date.strftime("%Y-%m-%d"),
                "close": round(float(close), 2),
                "div": round(float(div), 4) if div > 0 else 0,
                "tri": round(shares * float(close), 6),
            }
        )

    if not records:
        print(f"  No valid records for {ticker}")
        return None

    # Normalize TRI to 100 at the first data point
    initial_tri = records[0]["tri"]
    for r in records:
        r["tri"] = round(r["tri"] / initial_tri * 100, 4)

    output = {
        "ticker": ticker,
        "name": name,
        "currency": "NOK",
        "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "start_date": records[0]["date"],
        "end_date": records[-1]["date"],
        "records": records,
    }

    # Print summary
    final_tri = records[-1]["tri"]
    initial_price = records[0]["close"]
    final_price = records[-1]["close"]
    price_return_pct = (final_price / initial_price - 1) * 100
    print(f"  {records[0]['date']} → {records[-1]['date']}  "
          f"Total return: {final_tri - 100:.1f}%  "
          f"Price only: {price_return_pct:.1f}%  "
          f"({len(records)} records)")

    return output


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    stock_index = []

    for ticker, name in TICKERS.items():
        result = process_ticker(ticker, name)
        if result is None:
            print(f"  -> Skipping {ticker}")
            continue

        out_path = os.path.join(DATA_DIR, f"{ticker}.json")
        with open(out_path, "w") as f:
            json.dump(result, f, separators=(",", ":"))

        stock_index.append({"ticker": ticker, "name": name})

    index_path = os.path.join(DATA_DIR, "stocks.json")
    with open(index_path, "w") as f:
        json.dump(stock_index, f, separators=(",", ":"), indent=2)

    print(f"\nDone. {len(stock_index)}/{len(TICKERS)} stocks written to {DATA_DIR}/")
    print(f"Index: {index_path}")


if __name__ == "__main__":
    main()
