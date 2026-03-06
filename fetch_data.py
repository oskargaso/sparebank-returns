import yfinance as yf
import pandas as pd
import json
import os
from datetime import datetime

TICKER = "MING.OL"
START_DATE = "2010-01-01"
OUTPUT_FILE = "data/data.json"


def fetch_and_process():
    print(f"Fetching data for {TICKER} from {START_DATE}...")

    ticker = yf.Ticker(TICKER)

    # auto_adjust=False gives raw (unadjusted) prices + explicit Dividends column
    history = ticker.history(start=START_DATE, auto_adjust=False)

    if history.empty:
        print(f"ERROR: No data returned for {TICKER}")
        return

    # Strip timezone from index so we can format dates simply
    if history.index.tz is not None:
        history.index = history.index.tz_convert(None)

    prices = history["Close"]
    dividends = history["Dividends"]

    # Calculate total return index with dividend reinvestment (DRIP)
    # On each dividend date: reinvest by buying more shares at today's closing price
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
                # tri = unnormalized total return index (shares * price)
                "tri": round(shares * float(close), 6),
            }
        )

    if not records:
        print("ERROR: No valid records after processing")
        return

    # Normalize TRI to 100 at the first data point
    initial_tri = records[0]["tri"]
    for r in records:
        r["tri"] = round(r["tri"] / initial_tri * 100, 4)

    output = {
        "ticker": TICKER,
        "name": "Sparebank 1 SMN",
        "currency": "NOK",
        "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "start_date": records[0]["date"],
        "end_date": records[-1]["date"],
        "records": records,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    print(f"Saved {len(records)} records to {OUTPUT_FILE}")
    print(f"Date range: {records[0]['date']} to {records[-1]['date']}")

    # Quick summary
    final_tri = records[-1]["tri"]
    initial_price = records[0]["close"]
    final_price = records[-1]["close"]
    price_return_pct = (final_price / initial_price - 1) * 100
    print(f"Total return (DRIP):  {final_tri - 100:.1f}%")
    print(f"Price return only:    {price_return_pct:.1f}%")
    print(f"Dividend contribution: {(final_tri - 100) - price_return_pct:.1f}%")


if __name__ == "__main__":
    fetch_and_process()
