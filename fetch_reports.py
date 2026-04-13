"""
Quarterly report downloader + local database for Norwegian savings banks.

Usage:
  python fetch_reports.py fetch          # download new PDFs + extract text
  python fetch_reports.py search <query> # full-text search across all reports
  python fetch_reports.py list           # list all downloaded reports
  python fetch_reports.py stats          # show coverage per bank

Data is stored in:
  reports/<TICKER>/           PDF files
  reports.db                  SQLite database with full-text search

Primary source: Oslo Børs Newsweb (covers all listed companies reliably).
"""

import argparse
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, date
from pathlib import Path

import requests

# ── Optional deps (graceful degradation) ─────────────────────────────────────
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False
    print("[warn] pdfplumber not installed — text extraction disabled. Run: pip install pdfplumber")

# ── Configuration ─────────────────────────────────────────────────────────────

REPORTS_DIR = Path("reports")
DB_PATH     = Path("reports.db")
FETCH_FROM  = "2020-01-01"   # only reports from this date forward
REQUEST_DELAY = 1.2          # seconds between HTTP requests (be polite)

# Newsweb category IDs
CAT_FINANCIAL_REPORTS = 1    # quarterly + annual reports
CAT_INTERIM           = 4    # sometimes used for interim

# Banks to cover — tickers without .OL suffix for Newsweb
BANKS = {
    "MING":   "SpareBank 1 SMN",
    "SPOL":   "SpareBank 1 Østlandet",
    "NONG":   "SpareBank 1 Nord-Norge",
    "SBINO":  "SpareBank 1 Sørøst-Norge",
    "SOAG":   "SpareBank 1 Østfold Akershus",
    "HELG":   "SpareBank 1 Helgeland",
    "RING":   "SpareBank 1 Ringerike Hadeland",
    "SNOR":   "SpareBank 1 Nordmøre",
    "SB68":   "SpareBank 68 Grader Nord",
    "SPOG":   "Sparebanken Øst",
    "AURG":   "Aurskog Sparebank",
    "GRONG":  "Grong Sparebank",
    "HSPG":   "Høland og Setskog Sparebank",
    "FFSB":   "Flekkefjord Sparebank",
    "TRSB":   "Trøndelag Sparebank",
    "MELG":   "Melhus Sparebank",
    "SKUE":   "Skue Sparebank",
    "ROMER":  "Romerike Sparebank",
    "BIEN":   "Bien Sparebank",
    "TINDE":  "Tinde Sparebank",
    "AASB":   "Aasen Sparebank",
    "NISB":   "Nidaros Sparebank",
    "JAREN":  "Jæren Sparebank",
    "VVL":    "Voss Veksel- og Landmandsbank",
    "ROGS":   "Rogaland Sparebank",
    "MORG":   "Sparebanken Møre",
    "SBNOR":  "Sparebanken Norge",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; sparebank-research-bot/1.0)",
    "Accept": "application/json, text/html, */*",
}

# ── Database ──────────────────────────────────────────────────────────────────

def init_db(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS reports (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker        TEXT    NOT NULL,
            bank_name     TEXT    NOT NULL,
            message_id    TEXT    UNIQUE,
            title         TEXT,
            year          INTEGER,
            quarter       INTEGER,  -- 1-4, or 0 for annual/full-year
            period        TEXT,     -- human label: "Q4 2025", "Annual 2024"
            published_at  TEXT,
            source_url    TEXT,
            pdf_url       TEXT,
            filename      TEXT,
            page_count    INTEGER,
            downloaded_at TEXT,
            text_content  TEXT
        )
    """)
    # Full-text search index
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS reports_fts USING fts5(
            ticker,
            bank_name,
            period,
            title,
            text_content,
            content='reports',
            content_rowid='id'
        )
    """)
    # Triggers to keep FTS index in sync
    conn.execute("""
        CREATE TRIGGER IF NOT EXISTS reports_ai AFTER INSERT ON reports BEGIN
            INSERT INTO reports_fts(rowid, ticker, bank_name, period, title, text_content)
            VALUES (new.id, new.ticker, new.bank_name, new.period, new.title, new.text_content);
        END
    """)
    conn.execute("""
        CREATE TRIGGER IF NOT EXISTS reports_au AFTER UPDATE ON reports BEGIN
            INSERT INTO reports_fts(reports_fts, rowid, ticker, bank_name, period, title, text_content)
            VALUES ('delete', old.id, old.ticker, old.bank_name, old.period, old.title, old.text_content);
            INSERT INTO reports_fts(rowid, ticker, bank_name, period, title, text_content)
            VALUES (new.id, new.ticker, new.bank_name, new.period, new.title, new.text_content);
        END
    """)
    conn.execute("""
        CREATE TRIGGER IF NOT EXISTS reports_ad AFTER DELETE ON reports BEGIN
            INSERT INTO reports_fts(reports_fts, rowid, ticker, bank_name, period, title, text_content)
            VALUES ('delete', old.id, old.ticker, old.bank_name, old.period, old.title, old.text_content);
        END
    """)
    conn.commit()


# ── Newsweb scraper ───────────────────────────────────────────────────────────

NEWSWEB_API = "https://newsweb.oslobors.no/message/filteredMessages"
NEWSWEB_PDF = "https://newsweb.oslobors.no/message/attachment/{msg_id}/{att_id}/{filename}"

def fetch_newsweb_messages(ticker: str, from_date: str = FETCH_FROM) -> list[dict]:
    """
    Query Newsweb for financial reports filed by `ticker`.
    Returns list of message dicts from the API.
    """
    params = {
        "category": CAT_FINANCIAL_REPORTS,
        "issuer":   ticker,
        "fromDate": from_date,
        "toDate":   date.today().isoformat(),
        "limit":    200,
    }
    try:
        resp = requests.get(NEWSWEB_API, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        # Newsweb returns {"messages": [...]} or a plain list
        if isinstance(data, dict):
            return data.get("messages", data.get("data", []))
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"    [warn] Newsweb fetch failed for {ticker}: {e}")
        return []


def is_quarterly_report(title: str) -> bool:
    """Filter for quarterly/interim/annual reports only."""
    title_l = title.lower()
    keywords = [
        "kvartalsrapport", "kvartal", "quarterly", "interim",
        "q1", "q2", "q3", "q4", "årsrapport", "annual report",
        "delårsrapport", "results", "regnskap", "halvår", "half year",
    ]
    return any(k in title_l for k in keywords)


def parse_period(title: str, published_at: str) -> tuple[int | None, int | None, str]:
    """
    Extract (year, quarter, period_label) from report title.
    Returns (None, None, title) if not parseable.
    """
    title_l = title.lower()
    year = None
    quarter = None

    # Extract year
    m = re.search(r'(20\d{2})', title)
    if m:
        year = int(m.group(1))
    elif published_at:
        year = int(published_at[:4])

    # Detect quarter
    for q, patterns in {
        1: ["q1", "1. kvartal", "first quarter", "1q"],
        2: ["q2", "2. kvartal", "second quarter", "halvår", "half year", "2q"],
        3: ["q3", "3. kvartal", "third quarter", "3q"],
        4: ["q4", "4. kvartal", "fourth quarter", "fourth-quarter", "annual", "årsrapport", "4q"],
    }.items():
        if any(p in title_l for p in patterns):
            quarter = q
            break

    if year and quarter:
        label = f"Q{quarter} {year}"
    elif year:
        label = f"Annual {year}" if "årsrapport" in title_l or "annual" in title_l else f"{year}"
        quarter = 0
    else:
        label = title[:60]

    return year, quarter, label


def find_pdf_attachment(message: dict) -> tuple[str | None, str | None]:
    """Return (attachment_id, filename) of the first PDF in a message."""
    attachments = message.get("attachments", [])
    for att in attachments:
        fn = att.get("fileName", att.get("filename", ""))
        if fn.lower().endswith(".pdf"):
            att_id = att.get("id") or att.get("attachmentId")
            return str(att_id), fn
    return None, None


def build_pdf_url(msg_id: str, att_id: str, filename: str) -> str:
    return NEWSWEB_PDF.format(msg_id=msg_id, att_id=att_id, filename=filename)


# ── Downloader ────────────────────────────────────────────────────────────────

def download_pdf(url: str, dest: Path) -> bool:
    """Download a PDF to dest. Returns True on success."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30, stream=True)
        resp.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
        return True
    except Exception as e:
        print(f"    [warn] Download failed {url}: {e}")
        return False


# ── Text extraction ───────────────────────────────────────────────────────────

def extract_text(pdf_path: Path) -> tuple[str, int]:
    """
    Extract text from PDF. Returns (text, page_count).
    Falls back to empty string if pdfplumber is unavailable or extraction fails.
    """
    if not HAS_PDFPLUMBER:
        return "", 0
    try:
        pages = []
        with pdfplumber.open(pdf_path) as pdf:
            page_count = len(pdf.pages)
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
        return "\n\n".join(pages), page_count
    except Exception as e:
        print(f"    [warn] Text extraction failed for {pdf_path.name}: {e}")
        return "", 0


# ── Safe filename builder ─────────────────────────────────────────────────────

def safe_filename(ticker: str, period: str, original: str) -> str:
    """Build a clean, consistent filename: TICKER_Q4_2025.pdf"""
    period_clean = re.sub(r'[^A-Za-z0-9]', '_', period)
    ext = Path(original).suffix or ".pdf"
    return f"{ticker}_{period_clean}{ext}"


# ── Main fetch routine ────────────────────────────────────────────────────────

def cmd_fetch(conn: sqlite3.Connection, tickers: list[str] | None = None):
    banks = {k: v for k, v in BANKS.items() if tickers is None or k in tickers}
    total_new = 0

    for ticker, bank_name in banks.items():
        print(f"\n[{ticker}] {bank_name}")
        messages = fetch_newsweb_messages(ticker)
        time.sleep(REQUEST_DELAY)

        if not messages:
            print(f"  No messages found")
            continue

        reports = [m for m in messages if is_quarterly_report(m.get("title", ""))]
        print(f"  {len(messages)} messages → {len(reports)} quarterly/annual reports")

        for msg in reports:
            msg_id  = str(msg.get("messageId") or msg.get("id", ""))
            title   = msg.get("title", "")
            pub     = msg.get("publishedTime", msg.get("published", ""))[:10]

            year, quarter, period = parse_period(title, pub)
            att_id, orig_fn = find_pdf_attachment(msg)

            if not att_id:
                # No PDF attachment — skip
                continue

            # Skip if already in DB
            existing = conn.execute(
                "SELECT id FROM reports WHERE message_id = ?", (msg_id,)
            ).fetchone()
            if existing:
                continue

            # Build paths
            filename  = safe_filename(ticker, period, orig_fn)
            dest_path = REPORTS_DIR / ticker / filename
            pdf_url   = build_pdf_url(msg_id, att_id, orig_fn)

            print(f"  ↓ {period} — {title[:60]}")

            # Download
            ok = download_pdf(pdf_url, dest_path)
            time.sleep(REQUEST_DELAY)
            if not ok:
                continue

            # Extract text
            text, pages = extract_text(dest_path)

            # Insert into DB
            conn.execute("""
                INSERT OR IGNORE INTO reports
                  (ticker, bank_name, message_id, title, year, quarter, period,
                   published_at, pdf_url, filename, page_count, downloaded_at, text_content)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ticker, bank_name, msg_id, title, year, quarter, period,
                pub, pdf_url, str(dest_path), pages,
                datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
                text,
            ))
            conn.commit()
            total_new += 1

    print(f"\n✓ Done — {total_new} new reports added to database")


# ── Search ────────────────────────────────────────────────────────────────────

def cmd_search(conn: sqlite3.Connection, query: str, limit: int = 10):
    """Full-text search across all report contents."""
    rows = conn.execute("""
        SELECT r.ticker, r.bank_name, r.period, r.title,
               snippet(reports_fts, 4, '[', ']', '…', 30) AS snippet
        FROM reports_fts
        JOIN reports r ON r.id = reports_fts.rowid
        WHERE reports_fts MATCH ?
        ORDER BY rank
        LIMIT ?
    """, (query, limit)).fetchall()

    if not rows:
        print(f"No results for: {query}")
        return

    print(f"\n{len(rows)} result(s) for '{query}':\n")
    for ticker, bank, period, title, snip in rows:
        print(f"  [{ticker}] {bank} — {period}")
        print(f"  Title : {title}")
        print(f"  Match : {snip}")
        print()


# ── List ──────────────────────────────────────────────────────────────────────

def cmd_list(conn: sqlite3.Connection, ticker: str | None = None):
    where = "WHERE ticker = ?" if ticker else ""
    args  = (ticker,) if ticker else ()
    rows = conn.execute(f"""
        SELECT ticker, bank_name, period, title, page_count, downloaded_at
        FROM reports {where}
        ORDER BY ticker, year DESC, quarter DESC
    """, args).fetchall()

    if not rows:
        print("No reports in database yet. Run: python fetch_reports.py fetch")
        return

    current = None
    for ticker_r, bank, period, title, pages, downloaded in rows:
        if ticker_r != current:
            print(f"\n── {bank} ({ticker_r}) ──")
            current = ticker_r
        pages_str = f"{pages}p" if pages else "?"
        print(f"  {period:<12} {pages_str:>4}  {title[:55]}")


# ── Stats ─────────────────────────────────────────────────────────────────────

def cmd_stats(conn: sqlite3.Connection):
    rows = conn.execute("""
        SELECT ticker, bank_name,
               COUNT(*) AS n_reports,
               MIN(year) AS oldest,
               MAX(year) AS newest,
               SUM(CASE WHEN text_content != '' AND text_content IS NOT NULL THEN 1 ELSE 0 END) AS n_with_text
        FROM reports
        GROUP BY ticker
        ORDER BY n_reports DESC
    """).fetchall()

    total = conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0]
    print(f"\n{'Ticker':<8} {'Bank':<40} {'Reports':>7}  {'Years':>10}  {'Text':>5}")
    print("─" * 78)
    for ticker, bank, n, oldest, newest, n_text in rows:
        yr = f"{oldest}–{newest}" if oldest != newest else str(oldest)
        print(f"{ticker:<8} {bank:<40} {n:>7}  {yr:>10}  {n_text:>5}")
    print(f"\nTotal: {total} reports across {len(rows)} banks")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Sparebank quarterly report downloader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  fetch               Download new reports for all banks
  fetch MING SPOL     Download only for specific tickers
  search <query>      Full-text search (supports FTS5 syntax)
  list                List all downloaded reports
  list MING           List reports for one bank
  stats               Coverage summary per bank
        """,
    )
    parser.add_argument("command", choices=["fetch", "search", "list", "stats"])
    parser.add_argument("args", nargs="*")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    init_db(conn)

    if args.command == "fetch":
        tickers = [t.upper().replace(".OL", "") for t in args.args] or None
        cmd_fetch(conn, tickers)

    elif args.command == "search":
        if not args.args:
            print("Usage: python fetch_reports.py search <query>")
            sys.exit(1)
        cmd_search(conn, " ".join(args.args))

    elif args.command == "list":
        ticker = args.args[0].upper().replace(".OL", "") if args.args else None
        cmd_list(conn, ticker)

    elif args.command == "stats":
        cmd_stats(conn)

    conn.close()


if __name__ == "__main__":
    main()
