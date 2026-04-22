import yfinance as yf
import pandas as pd
import json
import os
import time
from datetime import datetime, timezone

# All stocks listed on Oslo Børs (source: DN Investor, 2026-03-10)
# Sorted roughly by market cap descending. Tickers use .OL suffix for Yahoo Finance.
TICKERS = {
    "EQNR.OL":  "Equinor",
    "DNB.OL":   "DNB Bank",
    "KOG.OL":   "Kongsberg Gruppen",
    "TEL.OL":   "Telenor",
    "AKRBP.OL": "Aker BP",
    "NHY.OL":   "Norsk Hydro",
    "GJF.OL":   "Gjensidige Forsikring",
    "YAR.OL":   "Yara International",
    "ORK.OL":   "Orkla",
    "MOWI.OL":  "Mowi",
    "AKER.OL":  "Aker",
    "SBINO.OL": "Sparebank 1 Sørøst-Norge",
    "SUBC.OL":  "Subsea 7",
    "FRO.OL":   "Frontline",
    "SALM.OL":  "SalMar",
    "STB.OL":   "Storebrand",
    "VEND.OL":  "Vend Marketplaces",
    "WAWI.OL":  "Wallenius Wilhelmsen",
    "PROT.OL":  "Protector Forsikring",
    "CMBTO.OL": "CMB.TECH",
    "HAFNI.OL": "Hafnia",
    "AUTO.OL":  "AutoStore Holdings",
    "SBNOR.OL": "Sparebanken Norge",
    "TOM.OL":   "Tomra Systems",
    "DOFG.OL":  "DOF Group",
    "MING.OL":  "Sparebank 1 SMN",
    "LSG.OL":   "Lerøy Seafood Group",
    "SPOL.OL":  "Sparebank 1 Østlandet",
    "NOD.OL":   "Nordic Semiconductor",
    "BWLPG.OL": "BW LPG",
    "BAKKA.OL": "Bakkafrost",
    "VEI.OL":   "Veidekke",
    "TIETO.OL": "Tietoevry",
    "TGS.OL":   "TGS",
    "KIT.OL":   "Kitron",
    "AUSS.OL":  "Austevoll Seafood",
    "CADLR.OL": "Cadeler",
    "ENTRA.OL": "Entra",
    "AKSO.OL":  "Aker Solutions",
    "AFG.OL":   "AF Gruppen",
    "SNI.OL":   "Stolt-Nielsen",
    "SWON.OL":  "SoftwareOne Holding",
    "SCATC.OL": "Scatec",
    "OET.OL":   "Okeanis Eco Tankers",
    "ELK.OL":   "Elkem",
    "BRG.OL":   "Borregaard",
    "BORR.OL":  "Borr Drilling",
    "NONG.OL":  "Sparebank 1 Nord-Norge",
    "ATEA.OL":  "Atea",
    "NAS.OL":   "Norwegian Air Shuttle",
    "DNO.OL":   "DNO",
    "EPR.OL":   "Europris",
    "BNOR.OL":  "Bluenord",
    "ELO.OL":   "Elopak",
    "NORCO.OL": "Norconsult",
    "SOMA.OL":  "Solstad Maritime",
    "COSH.OL":  "Constellation Oil Services",
    "BONHR.OL": "Bonheur",
    "MPCC.OL":  "MPC Container Ships",
    "AKBM.OL":  "Aker BioMarine",
    "BWO.OL":   "BW Offshore",
    "B2I.OL":   "B2 Impact",
    "AFK.OL":   "Arendals Fossekompani",
    "SATS.OL":  "SATS",
    "GSF.OL":   "Grieg Seafood",
    "ODF.OL":   "Odfjell Ser. A",
    "PLSV.OL":  "Paratus Energy Services",
    "RING.OL":  "Sparebank 1 Ringerike Hadeland",
    "SNTIA.OL": "Sentia",
    "LINK.OL":  "Link Mobility Group",
    "PEXIP.OL": "Pexip Holding",
    "PARB.OL":  "Pareto Bank",
    "WWIB.OL":  "Wilh. Wilhelmsen Holding B",
    "HSHP.OL":  "Himalaya Shipping",
    "MORG.OL":  "Sparebanken Møre",
    "SOAG.OL":  "Sparebank 1 Østfold Akershus",
    "KCC.OL":   "Klaveness Combination Carriers",
    "BOUV.OL":  "Bouvet",
    "KID.OL":   "Kid",
    "SOFF.OL":  "Solstad Offshore",
    "HELG.OL":  "Sparebank 1 Helgeland",
    "MULTI.OL": "Multiconsult",
    "SEA1.OL":  "Sea1 Offshore",
    "ABG.OL":   "ABG Sundal Collier",
    "CLOUD.OL": "Cloudberry Clean Energy",
    "AKAST.OL": "Akastor",
    "MEDI.OL":  "Medistim",
    "NEL.OL":   "NEL",
    "ROGS.OL":  "Rogaland Sparebank",
    "ELMRA.OL": "Elmera Group",
    "BEWI.OL":  "Bewi",
    "AKVA.OL":  "Akva Group",
    "NAPA.OL":  "Napatech",
    "SMOP.OL":  "Smartoptics Group",
    "ENVIP.OL": "Envipco Holding",
    "PEN.OL":   "Panoro Energy",
    "2020.OL":  "2020 Bulkers",
    "ANDF.OL":  "Andfjord Salmon",
    "POL.OL":   "Polaris Media",
    "MORLD.OL": "Moreld",
    "CAMBI.OL": "Cambi",
    "SMCRT.OL": "SmartCraft",
    "RANA.OL":  "Rana Gruber",
    "MAS.OL":   "Måsøval",
    "OTL.OL":   "Odfjell Technology",
    "NOL.OL":   "Northern Ocean",
    "ZAP.OL":   "Zaptec",
    "NSKOG.OL": "Norske Skog",
    "VDI.OL":   "Vantage Drilling International",
    "APR.OL":   "Appear",
    "VTURA.OL": "Ventura Offshore",
    "ARCH.OL":  "Archer",
    "XPLRA.OL": "Xplora Technologies",
    "IWS.OL":   "Integrated Wind Solutions",
    "DELIA.OL": "Delia Group",
    "ODFB.OL":  "Odfjell Ser. B",
    "REACH.OL": "Reach Subsea",
    "SKUE.OL":  "Skue Sparebank",
    "SALME.OL": "Salmon Evolution",
    "DVD.OL":   "Deep Value Driller",
    "PHO.OL":   "Photocure",
    "KOMPL.OL": "Komplett",
    "KOA.OL":   "Kongsberg Automotive",
    "INSTA.OL": "Instabank",
    "NORAM.OL": "Noram Drilling",
    "JAREN.OL": "Jæren Sparebank",
    "SPOG.OL":  "Sparebanken Øst",
    "SNOR.OL":  "Sparebank 1 Nordmøre",
    "AGLX.OL":  "Agilyx",
    "GIGA.OL":  "Gigante Salmon",
    "ZAL.OL":   "Zalaris",
    "NOAP.OL":  "Nordic Aqua Partners",
    "TRMED.OL": "Thor Medical",
    "PNOR.OL":  "Petronor E&P",
    "NKR.OL":   "Nekkar",
    "NRC.OL":   "NRC Group",
    "NOM.OL":   "Nordic Mining",
    "MGN.OL":   "Magnora",
    "NORDH.OL": "Nordhealth",
    "AURG.OL":  "Aurskog Sparebank",
    "NOHAL.OL": "Nordic Halibut",
    "AFISH.OL": "Arctic Fish Holding",
    "BMA.OL":   "Byggma",
    "LUMI.OL":  "Lumi Gruppen",
    "PRS.OL":   "Prosafe",
    "AZT.OL":   "ArcticZymes Technologies",
    "GYL.OL":   "Gyldendal",
    "EIOF.OL":  "Eidesvik Offshore",
    "NYKD.OL":  "Nykode Therapeutics",
    "SDSD.OL":  "S.D. Standard ETC",
    "NORSE.OL": "Norse Atlantic",
    "DFENS.OL": "Fjord Defence Group",
    "VISTN.OL": "Vistin Pharma",
    "VVL.OL":   "Voss Veksel- og Landmandsbank",
    "HKY.OL":   "Havila Kystruten",
    "NTI.OL":   "Norsk Titanium",
    "SAGA.OL":  "Saga Pure",
    "OMDA.OL":  "OMDA",
    "LYTIX.OL": "Lytix Biopharma",
    "ELIMP.OL": "Elektroimportøren",
    "BIEN.OL":  "Bien Sparebank",
    "VOW.OL":   "VOW",
    "KLDVK.OL": "Kaldvik",
    "GRONG.OL": "Grong Sparebank",
    "CRNA.OL":  "Circio Holding",
    "STECH.OL": "Soiltech",
    "SCANA.OL": "Scana",
    "WEST.OL":  "Western Bulk Chartering",
    "OTOVO.OL": "Otovo",
    "IDEX.OL":  "IDEX Biometrics",
    "NCOD.OL":  "Norcod",
    "ENSU.OL":  "Ensurge Micropower",
    "HUNT.OL":  "Hunter Group",
    "SB68.OL":  "Sparebank 68 Grader Nord",
    "TINDE.OL": "Tinde Sparebank",
    "HDLY.OL":  "Huddly",
    "JIN.OL":   "Jinhui Shipping and Transport",
    "GENT.OL":  "Gentian Diagnostics",
    "ITERA.OL": "Itera",
    "GEOS.OL":  "Golden Energy Offshore",
    "MELG.OL":  "Melhus Sparebank",
    "TRSB.OL":  "Trøndelag Sparebank",
    "HBC.OL":   "Hofseth BioCare",
    "WSTEP.OL": "Webstep",
    "AKOBO.OL": "Akobo Minerals",
    "STST.OL":  "Stainless Tankers",
    "NAVA.OL":  "Navamedic",
    "HPUR.OL":  "Hexagon Purus",
    "CYVIZ.OL": "Cyviz",
    "STRO.OL":  "Strongpoint",
    "ALNG.OL":  "Awilco LNG",
    "PPG.OL":   "Pioneer Property Group",
    "ZLNA.OL":  "Zelluna",
    "KING.OL":  "The Kingfish Company",
    "BALT.OL":  "Baltic Sea Properties",
    "ZENA.OL":  "Zenith Energy",
    "ROMER.OL": "Romerike Sparebank",
    "CAPSL.OL": "Capsol Technologies",
    "NTG.OL":   "Nordic Technology Group",
    "TECH.OL":  "Techstep",
    "LOKO.OL":  "Lokotech Group",
    "GOD.OL":   "Goodtech",
    "ELABS.OL": "Elliptic Laboratories",
    "PROXI.OL": "Proximar Seafood",
    "PSE.OL":   "Petrolia",
    "NORTH.OL": "North Energy",
    "MPCES.OL": "MPC Energy Solutions",
    "HAVI.OL":  "Havila Shipping",
    "PRYME.OL": "Pryme",
    "EQVA.OL":  "Eqva",
    "HERMA.OL": "Hermana Holding",
    "AASB.OL":  "Aasen Sparebank",
    "CONTX.OL": "Contextvision",
    "CAVEN.OL": "Cavendish Hydrogen",
    "MVE.OL":   "Matvareexpressen",
    "OCEAN.OL": "Ocean Geoloop",
    "MVW.OL":   "M Vest Water",
    "ONCIN.OL": "Oncoinvent",
    "FFSB.OL":  "Flekkefjord Sparebank",
    "ASA.OL":   "Atlantic Sapphire",
    "ADS.OL":   "ADS Maritime Holding",
    "HUDL.OL":  "Huddlestock Fintech",
    "RECSI.OL": "REC Silicon",
    "HYPRO.OL": "HydrogenPro",
    "BCS.OL":   "Bergen Carbon Solutions",
    "NISB.OL":  "Nidaros Sparebank",
    "OBSRV.OL": "Observe Medical",
    "NEXT.OL":  "Next Biometrics Group",
    "NBX.OL":   "Norwegian Block Exchange",
    "HSPG.OL":  "Høland og Setskog Sparebank",
    "ABS.OL":   "Arctic Bioscience",
    "IOX.OL":   "Interoil Exploration",
    "EMGS.OL":  "Electromagnetic Geoservices",
    "ABTEC.OL": "Aqua Bio Technology",
    "AKH.OL":   "Aker Horizons",
    "EAM.OL":   "EAM Solar",
    "KMCP.OL":  "KMC Properties",
    "PCIB.OL":  "PCI Biotech Holding",
    "PYRUM.OL": "Pyrum Innovations",
    "REFL.OL":  "Refuels",
    "SOFTX.OL": "Softox Solutions",
    "ROM.OL":   "Romreal",
    "RIVER.OL": "River Tech",
    "NOSN.OL":  "NOS Nova",
    "GEM.OL":   "Green Minerals",
    "BSP.OL":   "Black Sea Property",
    "HYN.OL":   "Hynion",
    "INIFY.OL": "Inify Laboratories",
    "NOFIN.OL": "Nordic Financials",
    "ACED.OL":  "ACE Digital",
    "EXTX.OL":  "Exact Therapeutics",
    "AIX.OL":   "Ayfie International",
    "INDCT.OL": "Induct",
    "BARRA.OL": "Barramundi Group",
    "NORDIC.OL":"Nordic Financials",
    "AURG.OL":  "Aurskog Sparebank",
    "NOHAL.OL": "Nordic Halibut",
    "STST.OL":  "Stainless Tankers",
    "NYKD.OL":  "Nykode Therapeutics",
    "NORDH.OL": "Nordhealth",
}

START_DATE = "2010-01-01"
DATA_DIR = "data"


def _ts_to_date(ts):
    """Convert a Unix timestamp (int or None) to a YYYY-MM-DD string, or None."""
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d")
    except (OSError, OverflowError, ValueError):
        return None


def calc_dividend_recovery(records, years=5):
    """
    For each dividend ex-date in the last `years` years, calculate how many
    trading days it takes for the stock price to recover to the pre-ex-dividend
    closing price. Returns (avg_recovery_days, freq_label, divs_per_year).

    Dividends paid within MERGE_WINDOW trading days of each other are treated
    as a single event (combined amount, single reference price). This handles
    companies that split one payout into two close tranches.
    """
    if not records:
        return None, None, None

    last_date = datetime.strptime(records[-1]["date"], "%Y-%m-%d")
    try:
        cutoff = last_date.replace(year=last_date.year - years).strftime("%Y-%m-%d")
    except ValueError:
        cutoff = last_date.strftime("%Y-%m-%d")

    MAX_RECOVERY = 252  # ~1 trading year cap
    MERGE_WINDOW = 10   # trading days — merges split tranches but not monthly payers

    # Build list of all dividend events (index, date, amount)
    all_divs = [
        (i, r["date"], r["div"])
        for i, r in enumerate(records)
        if r.get("div", 0) > 0
    ]

    # Merge dividends within MERGE_WINDOW trading days into single events
    merged = []  # (first_record_idx, first_date, combined_div)
    k = 0
    while k < len(all_divs):
        first_idx, first_date, total_div = all_divs[k]
        k += 1
        while k < len(all_divs) and all_divs[k][0] - first_idx <= MERGE_WINDOW:
            total_div += all_divs[k][1]
            k += 1
        merged.append((first_idx, first_date, total_div))

    # Only events inside the 5-year window
    events = [(idx, date, div) for idx, date, div in merged if date >= cutoff]
    div_count = len(events)

    recovery_days_list = []

    for first_idx, _date, total_div in events:
        if first_idx == 0:
            continue  # no previous price reference

        pre_ex_price = records[first_idx - 1]["close"]

        # Data-quality filter: require observable drop >= 20% of dividend within
        # 3 days of first ex-date. Use combined div as the expected drop size.
        min_close_window = min(
            records[k]["close"]
            for k in range(first_idx, min(first_idx + 4, len(records)))
        )
        if min_close_window > pre_ex_price - total_div * 0.2:
            continue  # no real drop observed — skip this event

        # Find first day AFTER ex-date when price recovers to pre-ex level.
        recovered_in = MAX_RECOVERY
        for j in range(first_idx + 1, min(first_idx + MAX_RECOVERY + 1, len(records))):
            if records[j]["close"] >= pre_ex_price:
                recovered_in = j - first_idx  # minimum 1
                break

        recovery_days_list.append(recovered_in)

    if not recovery_days_list:
        return None, None, None

    avg_days = round(sum(recovery_days_list) / len(recovery_days_list), 1)
    divs_per_year = div_count / years

    if divs_per_year >= 3.5:
        freq_label = "quarterly"
    elif divs_per_year >= 1.5:
        freq_label = "semi-annual"
    elif divs_per_year >= 0.5:
        freq_label = "annual"
    else:
        freq_label = "irregular"

    return avg_days, freq_label, round(divs_per_year, 1)


def calc_drawdown_from_ath(records):
    """Current % below all-time-high close price."""
    if not records:
        return None
    ath = max(r["close"] for r in records)
    if ath <= 0:
        return None
    return round((records[-1]["close"] / ath - 1) * 100, 1)


def calc_dividend_cagr(records, years):
    """
    CAGR of dividends per share over `years` years.
    Compares trailing-12m dividend sum at the start of the window vs the end.
    """
    if not records:
        return None
    last_dt = datetime.strptime(records[-1]["date"], "%Y-%m-%d")
    try:
        window_start = last_dt.replace(year=last_dt.year - years)
        t12m_start   = window_start.replace(year=window_start.year - 1)
    except ValueError:
        return None

    ws_str  = window_start.strftime("%Y-%m-%d")
    t12_str = t12m_start.strftime("%Y-%m-%d")
    last_str = records[-1]["date"]

    div_old = sum(r["div"] for r in records
                  if r.get("div", 0) > 0 and t12_str <= r["date"] <= ws_str)
    div_new = sum(r["div"] for r in records
                  if r.get("div", 0) > 0 and r["date"] > ws_str and r["date"] <= last_str)

    if div_old <= 0 or div_new <= 0:
        return None
    actual_years = (last_dt - window_start).days / 365.25
    if actual_years < 1:
        return None
    return round((pow(div_new / div_old, 1 / actual_years) - 1) * 100, 1)


def calc_yield_stats(records):
    """
    Returns (current_trailing_12m_yield_pct, avg_trailing_yield_5y_pct).
    Uses a sliding window over the record history.
    """
    from collections import deque
    if not records:
        return None, None

    last_dt = datetime.strptime(records[-1]["date"], "%Y-%m-%d")
    try:
        cutoff_5y = last_dt.replace(year=last_dt.year - 5).strftime("%Y-%m-%d")
    except ValueError:
        cutoff_5y = records[0]["date"]

    dq = deque()   # (date_str, div)
    yields_5y = []
    current_yield = None

    for r in records:
        if r.get("div", 0) > 0:
            dq.append((r["date"], r["div"]))
        cur_dt = datetime.strptime(r["date"], "%Y-%m-%d")
        while dq and (cur_dt - datetime.strptime(dq[0][0], "%Y-%m-%d")).days > 365:
            dq.popleft()
        t12m = sum(d for _, d in dq)
        if r["close"] > 0 and t12m > 0:
            y = t12m / r["close"] * 100
            if r["date"] >= cutoff_5y:
                yields_5y.append(y)
            current_yield = round(y, 2)

    yield_5y_avg = round(sum(yields_5y) / len(yields_5y), 2) if yields_5y else None
    return current_yield, yield_5y_avg


def calc_rolling_returns(records):
    """1Y, 3Y, 5Y, 10Y CAGR of total return index."""
    if not records:
        return {}
    last = records[-1]
    last_dt = datetime.strptime(last["date"], "%Y-%m-%d")
    result = {}
    for yrs in (1, 3, 5, 10):
        try:
            target_str = last_dt.replace(year=last_dt.year - yrs).strftime("%Y-%m-%d")
        except ValueError:
            result[f"{yrs}y"] = None
            continue
        base = next((r for r in records if r["date"] >= target_str), None)
        if base is None or base["date"] == last["date"]:
            result[f"{yrs}y"] = None
            continue
        actual_yrs = (last_dt - datetime.strptime(base["date"], "%Y-%m-%d")).days / 365.25
        if actual_yrs < 0.5:
            result[f"{yrs}y"] = None
            continue
        result[f"{yrs}y"] = round((pow(last["tri"] / base["tri"], 1 / actual_yrs) - 1) * 100, 1)
    return result


def process_ticker(ticker, name):
    """Fetch and process a single ticker. Returns output dict or None on failure."""
    try:
        t = yf.Ticker(ticker)
        history = t.history(period="max", auto_adjust=False)
    except Exception as e:
        print(f"  ERROR fetching {ticker}: {e}")
        return None

    if history.empty:
        print(f"  No data for {ticker}")
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

    # Fetch company metadata from yfinance .info
    info = {}
    try:
        info = t.info or {}
    except Exception:
        pass

    output = {
        "ticker": ticker,
        "name": name,
        "currency": "NOK",
        "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "start_date": records[0]["date"],
        "end_date": records[-1]["date"],
        "marketCap": info.get("marketCap"),
        "industry": info.get("industry"),
        "website": info.get("website"),
        "longBusinessSummary": info.get("longBusinessSummary"),
        "next_ex_date": _ts_to_date(info.get("exDividendDate")),
        "forward_annual_dividend": info.get("dividendRate"),
        "records": records,
    }

    final_tri = records[-1]["tri"]
    initial_price = records[0]["close"]
    final_price = records[-1]["close"]
    price_pct = (final_price / initial_price - 1) * 100

    last_dt = datetime.strptime(records[-1]["date"], "%Y-%m-%d")

    # 1Y return
    one_year_ago = last_dt.replace(year=last_dt.year - 1).strftime("%Y-%m-%d")
    base = next((r for r in records if r["date"] >= one_year_ago), None)
    return_1y = None
    if base and base["date"] != records[-1]["date"]:
        return_1y = round((records[-1]["tri"] / base["tri"] - 1) * 100, 1)
    output["return_1y"] = return_1y

    # 5Y CAGR
    five_years_ago = last_dt.replace(year=last_dt.year - 5).strftime("%Y-%m-%d")
    base_5y = next((r for r in records if r["date"] >= five_years_ago), None)
    cagr_5y = None
    if base_5y and base_5y["date"] != records[-1]["date"]:
        yrs = (last_dt - datetime.strptime(base_5y["date"], "%Y-%m-%d")).days / 365.25
        if yrs >= 1:
            cagr_5y = round((pow(records[-1]["tri"] / base_5y["tri"], 1 / yrs) - 1) * 100, 1)
    output["cagr_5y"] = cagr_5y

    # Dividend recovery analysis (last 5 years)
    avg_recovery_days, div_frequency, divs_per_year = calc_dividend_recovery(records)
    output["avg_recovery_days"] = avg_recovery_days
    output["div_frequency"] = div_frequency
    output["divs_per_year"] = divs_per_year

    # Drawdown from all-time high
    output["drawdown_from_ath"] = calc_drawdown_from_ath(records)

    # Dividend CAGR
    output["div_cagr_3y"] = calc_dividend_cagr(records, 3)
    output["div_cagr_5y_calc"] = calc_dividend_cagr(records, 5)

    # Trailing yield stats
    output["yield_current"], output["yield_5y_avg"] = calc_yield_stats(records)

    # Rolling total returns (1Y, 3Y, 5Y, 10Y)
    output["rolling_returns"] = calc_rolling_returns(records)

    # Valuation metrics from yfinance .info
    output["trailingPE"]             = info.get("trailingPE")
    output["forwardPE"]              = info.get("forwardPE")
    output["priceToBook"]            = info.get("priceToBook")
    output["trailingEps"]            = info.get("trailingEps")
    output["recommendationKey"]      = info.get("recommendationKey")
    output["recommendationMean"]     = info.get("recommendationMean")
    output["numberOfAnalystOpinions"] = info.get("numberOfAnalystOpinions")

    print(f"  {records[0]['date']} -> {records[-1]['date']}  "
          f"DRIP: {final_tri - 100:.1f}%  Price: {price_pct:.1f}%  ({len(records)} days)")

    return output


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # Python dict already deduplicates keys (last definition wins for any dupes)
    stock_index = []
    skipped = []

    total = len(TICKERS)
    for i, (ticker, name) in enumerate(TICKERS.items(), 1):
        print(f"[{i}/{total}] {ticker} — {name}")
        result = process_ticker(ticker, name)

        if result is None:
            skipped.append(ticker)
        else:
            out_path = os.path.join(DATA_DIR, f"{ticker}.json")
            with open(out_path, "w") as f:
                json.dump(result, f, separators=(",", ":"))
            stock_index.append({
                "ticker": ticker,
                "name": name,
                "return_1y": result.get("return_1y"),
                "cagr_5y": result.get("cagr_5y"),
                "marketCap": result.get("marketCap"),
                "industry": result.get("industry"),
                "avg_recovery_days": result.get("avg_recovery_days"),
                "div_frequency": result.get("div_frequency"),
                "website": result.get("website"),
                "next_ex_date": result.get("next_ex_date"),
                "forward_annual_dividend": result.get("forward_annual_dividend"),
                "drawdown_from_ath": result.get("drawdown_from_ath"),
                "div_cagr_5y": result.get("div_cagr_5y_calc"),
                "yield_current": result.get("yield_current"),
                "recommendationKey": result.get("recommendationKey"),
            })

        # Small delay to avoid Yahoo Finance rate limits
        time.sleep(0.4)

    # Write stocks index
    index_path = os.path.join(DATA_DIR, "stocks.json")
    with open(index_path, "w") as f:
        json.dump(stock_index, f, separators=(",", ":"), indent=2)

    print(f"\n{'='*50}")
    print(f"Done. {len(stock_index)}/{total} stocks saved to {DATA_DIR}/")
    print(f"Skipped ({len(skipped)}): {', '.join(skipped)}")


if __name__ == "__main__":
    main()
