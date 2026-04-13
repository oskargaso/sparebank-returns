"""
M&A Target Scoring Pipeline – Norwegian listed banks (Oslo Børs)
Outputs: data/mna-targets.json

Run: python mna_pipeline.py
Scheduled: weekly via GitHub Actions (see .github/workflows/update-data.yml)
"""

import json
import math
import os
import time
from datetime import datetime, timezone
from itertools import combinations

import numpy as np
import pandas as pd
import yfinance as yf

# ── 1. Bank universe ──────────────────────────────────────────────────────────
# DNB excluded (used as peer benchmark only).
# Each entry: ticker, name, alliance, region
BANKS = [
    # SpareBank 1 alliance
    {"ticker": "MING.OL",  "name": "SpareBank 1 SMN",               "alliance": "SpareBank1", "region": "midt"},
    {"ticker": "SPOL.OL",  "name": "SpareBank 1 Østlandet",          "alliance": "SpareBank1", "region": "øst"},
    {"ticker": "NONG.OL",  "name": "SpareBank 1 Nord-Norge",         "alliance": "SpareBank1", "region": "nord"},
    {"ticker": "SBINO.OL", "name": "SpareBank 1 Sørøst-Norge",       "alliance": "SpareBank1", "region": "øst"},
    {"ticker": "SOAG.OL",  "name": "SpareBank 1 Østfold Akershus",   "alliance": "SpareBank1", "region": "øst"},
    {"ticker": "HELG.OL",  "name": "SpareBank 1 Helgeland",          "alliance": "SpareBank1", "region": "nord"},
    {"ticker": "RING.OL",  "name": "SpareBank 1 Ringerike Hadeland", "alliance": "SpareBank1", "region": "øst"},
    {"ticker": "SNOR.OL",  "name": "SpareBank 1 Nordmøre",           "alliance": "SpareBank1", "region": "midt"},
    {"ticker": "SB68.OL",  "name": "SpareBank 68 Grader Nord",       "alliance": "SpareBank1", "region": "nord"},
    # Eika alliance
    {"ticker": "SPOG.OL",  "name": "Sparebanken Øst",                "alliance": "Eika", "region": "øst"},
    {"ticker": "AURG.OL",  "name": "Aurskog Sparebank",              "alliance": "Eika", "region": "øst"},
    {"ticker": "GRONG.OL", "name": "Grong Sparebank",                "alliance": "Eika", "region": "midt"},
    {"ticker": "HSPG.OL",  "name": "Høland og Setskog Sparebank",    "alliance": "Eika", "region": "øst"},
    {"ticker": "FFSB.OL",  "name": "Flekkefjord Sparebank",          "alliance": "Eika", "region": "vest"},
    {"ticker": "TRSB.OL",  "name": "Trøndelag Sparebank",            "alliance": "Eika", "region": "midt"},
    {"ticker": "MELG.OL",  "name": "Melhus Sparebank",               "alliance": "Eika", "region": "midt"},
    {"ticker": "SKUE.OL",  "name": "Skue Sparebank",                 "alliance": "Eika", "region": "øst"},
    {"ticker": "ROMER.OL", "name": "Romerike Sparebank",             "alliance": "Eika", "region": "øst"},
    {"ticker": "BIEN.OL",  "name": "Bien Sparebank",                 "alliance": "Eika", "region": "øst"},
    {"ticker": "TINDE.OL", "name": "Tinde Sparebank",                "alliance": "Eika", "region": "midt"},
    {"ticker": "AASB.OL",  "name": "Aasen Sparebank",                "alliance": "Eika", "region": "midt"},
    {"ticker": "NISB.OL",  "name": "Nidaros Sparebank",              "alliance": "Eika", "region": "midt"},
    {"ticker": "JAREN.OL", "name": "Jæren Sparebank",                "alliance": "Eika", "region": "vest"},
    {"ticker": "VVL.OL",   "name": "Voss Veksel- og Landmandsbank",  "alliance": "Eika", "region": "vest"},
    {"ticker": "ROGS.OL",  "name": "Rogaland Sparebank",             "alliance": "Eika", "region": "vest"},
    # Independent sparebankene
    {"ticker": "MORG.OL",  "name": "Sparebanken Møre",               "alliance": "Independent", "region": "vest"},
    {"ticker": "SBNOR.OL", "name": "Sparebanken Norge",              "alliance": "Independent", "region": "vest"},
]

DNB_TICKER = "DNB.OL"

# Hardcoded CET1 ratios (%) — latest reported, updated manually when annual
# reports are released. Source: each bank's most recent annual report / Pillar 3.
CET1_OVERRIDES = {
    "MING.OL":  18.4,
    "SPOL.OL":  19.1,
    "NONG.OL":  19.5,
    "SBINO.OL": 19.9,
    "SOAG.OL":  20.8,
    "HELG.OL":  19.0,
    "RING.OL":  21.5,
    "SNOR.OL":  22.1,
    "SB68.OL":  21.0,
    "SPOG.OL":  19.3,
    "AURG.OL":  23.5,
    "GRONG.OL": 25.0,
    "HSPG.OL":  24.0,
    "FFSB.OL":  24.5,
    "TRSB.OL":  22.8,
    "MELG.OL":  24.2,
    "SKUE.OL":  23.1,
    "ROMER.OL": 22.0,
    "BIEN.OL":  22.5,
    "TINDE.OL": 23.8,
    "AASB.OL":  25.5,
    "NISB.OL":  24.0,
    "JAREN.OL": 23.0,
    "VVL.OL":   24.8,
    "ROGS.OL":  21.5,
    "MORG.OL":  18.6,
    "SBNOR.OL": 19.2,
}

REQUIRED_CET1 = 16.5  # proxy regulatory minimum (%)

REGION_ADJACENCY = {
    "nord":  ["nord", "midt"],
    "midt":  ["midt", "nord", "vest", "øst"],
    "vest":  ["vest", "midt"],
    "øst":   ["øst",  "midt"],
}


# ── 2. Data fetching ──────────────────────────────────────────────────────────

def fetch_yf_info(ticker: str) -> dict:
    try:
        t = yf.Ticker(ticker)
        info = t.info or {}
        return info
    except Exception as e:
        print(f"  [warn] info fetch failed for {ticker}: {e}")
        return {}


def fetch_financials(ticker: str) -> dict:
    """
    Pull income statement + balance sheet from yfinance.
    Returns dict with keys: roe, cost_income, assets, net_income, opex,
    net_interest_income, pb_ratio — all floats or None.
    """
    result = {
        "roe": None,
        "cost_income": None,
        "assets": None,
        "net_income": None,
        "opex": None,
        "net_interest_income": None,
        "pb_ratio": None,
        "earnings_volatility": None,
    }
    try:
        t = yf.Ticker(ticker)
        info = t.info or {}

        # Price/Book from info
        result["pb_ratio"] = info.get("priceToBook")

        # ROE from info (trailing)
        result["roe"] = info.get("returnOnEquity")  # decimal, e.g. 0.12
        if result["roe"] is not None:
            result["roe"] = result["roe"] * 100  # convert to %

        # Total assets from balance sheet
        try:
            bs = t.balance_sheet
            if bs is not None and not bs.empty:
                asset_row = None
                for label in ["Total Assets", "totalAssets"]:
                    if label in bs.index:
                        asset_row = bs.loc[label]
                        break
                if asset_row is not None:
                    result["assets"] = float(asset_row.iloc[0])
        except Exception:
            pass

        # Net income + opex from income statement
        try:
            inc = t.financials
            if inc is not None and not inc.empty:
                for label in ["Net Income", "netIncome"]:
                    if label in inc.index:
                        ni_row = inc.loc[label].dropna()
                        if not ni_row.empty:
                            result["net_income"] = float(ni_row.iloc[0])
                            # Earnings volatility: std of last 4 annual figures
                            if len(ni_row) >= 2:
                                result["earnings_volatility"] = float(ni_row.values.std())
                        break

                for label in ["Operating Expense", "Total Operating Expenses", "operatingExpenses"]:
                    if label in inc.index:
                        result["opex"] = float(inc.loc[label].iloc[0])
                        break

                # Net interest income proxy: Total Revenue
                for label in ["Total Revenue", "Net Interest Income", "totalRevenue"]:
                    if label in inc.index:
                        result["net_interest_income"] = float(inc.loc[label].iloc[0])
                        break

                # Cost/income proxy: opex / revenue
                if result["opex"] and result["net_interest_income"] and result["net_interest_income"] != 0:
                    result["cost_income"] = abs(result["opex"]) / abs(result["net_interest_income"]) * 100
        except Exception:
            pass

    except Exception as e:
        print(f"  [warn] financials fetch failed for {ticker}: {e}")

    return result


def fetch_price_history(ticker: str, years: int = 5) -> list[float]:
    """Return list of annual close prices for earnings volatility fallback."""
    try:
        t = yf.Ticker(ticker)
        h = t.history(period=f"{years}y", interval="1mo", auto_adjust=True)
        if h.empty:
            return []
        return h["Close"].dropna().tolist()
    except Exception:
        return []


# ── 3. Scoring engine ─────────────────────────────────────────────────────────

def score_financial_pressure(roe, cost_income, cet1, peer_median_roe, peer_median_ci) -> float:
    """0–5 scale. Higher = more pressure = more likely target."""
    s = 0.0
    if roe is not None and peer_median_roe is not None:
        gap = peer_median_roe - roe
        if gap > 4:
            s += 2.0
        elif gap > 0:
            s += 1.0
    if cost_income is not None:
        if cost_income > 65:
            s += 2.0
        elif cost_income > 55:
            s += 1.0
    if cet1 is not None:
        buffer = cet1 - REQUIRED_CET1
        if buffer < 2:
            s += 2.0
        elif buffer < 4:
            s += 1.0
    return min(s, 5.0)


def score_scale(assets) -> float:
    """Mid-sized banks score highest (most attractive targets). 0–5."""
    if assets is None:
        return 2.5  # unknown → neutral
    log_a = math.log10(max(assets, 1))
    # Sweet spot: 10B–100B NOK → log10 ~ 10–11
    # Below 10B (log<10): very small, less attractive → lower score
    # Above 500B (log>11.7): too large → lower score
    if log_a < 9.5:
        return 1.0
    elif log_a < 10.3:
        return 3.5
    elif log_a < 11.0:
        return 5.0
    elif log_a < 11.7:
        return 3.5
    else:
        return 1.5


def score_efficiency_gap(cost_income, peer_median_ci) -> float:
    """How far above peer median cost/income. 0–5."""
    if cost_income is None or peer_median_ci is None:
        return 2.5
    gap = cost_income - peer_median_ci
    if gap > 15:
        return 5.0
    elif gap > 8:
        return 3.5
    elif gap > 3:
        return 2.0
    elif gap > 0:
        return 1.0
    else:
        return 0.0


def score_pb(pb_ratio) -> float:
    """Low P/B = cheap = attractive target. 0–5."""
    if pb_ratio is None:
        return 2.5
    if pb_ratio < 0.7:
        return 5.0
    elif pb_ratio < 1.0:
        return 4.0
    elif pb_ratio < 1.3:
        return 2.5
    elif pb_ratio < 1.7:
        return 1.0
    else:
        return 0.0


def score_earnings_volatility(volatility, peer_median_vol) -> float:
    """Higher earnings volatility → more vulnerable → higher score. 0–5."""
    if volatility is None or peer_median_vol is None or peer_median_vol == 0:
        return 2.5
    ratio = volatility / peer_median_vol
    if ratio > 2.0:
        return 5.0
    elif ratio > 1.5:
        return 3.5
    elif ratio > 1.0:
        return 2.0
    else:
        return 0.5


def score_alliance(alliance: str) -> float:
    """Independent banks are easiest targets. 0–5."""
    return {"Independent": 5.0, "Eika": 3.0, "SpareBank1": 1.5}.get(alliance, 2.0)


# ── 4. Pairwise synergy ───────────────────────────────────────────────────────

def geographic_score(region_a: str, region_b: str) -> float:
    """Adjacent regions = high synergy potential. 0–5."""
    adj = REGION_ADJACENCY.get(region_a, [])
    if region_a == region_b:
        return 2.0   # same region → overlap risk
    elif region_b in adj:
        return 5.0   # adjacent → strong geographic fit
    else:
        return 1.0   # far apart → limited synergy


def pairwise_synergy(bank_a: dict, bank_b: dict, financials: dict) -> float:
    """Composite pairwise synergy score for acquirer A → target B. 0–5."""
    fa = financials.get(bank_a["ticker"], {})
    fb = financials.get(bank_b["ticker"], {})

    # Cost synergy: bigger CI difference = more room to improve
    ci_a = fa.get("cost_income")
    ci_b = fb.get("cost_income")
    cost_synergy = 0.0
    if ci_a is not None and ci_b is not None:
        diff = abs(ci_a - ci_b)
        cost_synergy = min(diff / 20 * 5, 5.0)

    geo = geographic_score(bank_a["region"], bank_b["region"])

    # Same alliance → easier integration (+1.5)
    alliance_bonus = 1.5 if bank_a["alliance"] == bank_b["alliance"] else 0.0

    return min((cost_synergy * 0.4 + geo * 0.4 + alliance_bonus * 0.2), 5.0)


def find_likely_acquirers(target: dict, all_banks: list, financials: dict) -> list[dict]:
    """Return up to 3 plausible acquirers sorted by synergy."""
    candidates = []
    for bank in all_banks:
        if bank["ticker"] == target["ticker"]:
            continue
        fa = financials.get(bank["ticker"], {})
        ft = financials.get(target["ticker"], {})
        # Acquirer must be larger
        assets_a = fa.get("assets") or 0
        assets_t = ft.get("assets") or 0
        if assets_a < assets_t * 0.8:
            continue
        syn = pairwise_synergy(bank, target, financials)
        candidates.append({"ticker": bank["ticker"], "name": bank["name"], "synergy": syn})
    candidates.sort(key=lambda x: x["synergy"], reverse=True)
    return candidates[:3]


# ── 5. Rationale builder ──────────────────────────────────────────────────────

def build_rationale(bank: dict, fin: dict, scores: dict, peer_median_roe, peer_median_ci) -> list[str]:
    reasons = []
    roe = fin.get("roe")
    ci = fin.get("cost_income")
    cet1 = CET1_OVERRIDES.get(bank["ticker"])
    pb = fin.get("pb_ratio")

    if roe is not None and peer_median_roe is not None and roe < peer_median_roe - 2:
        reasons.append(f"ROE {roe:.1f}% — under peer median ({peer_median_roe:.1f}%)")
    if ci is not None and ci > 55:
        reasons.append(f"Cost/income {ci:.0f}% — {'+' if ci > peer_median_ci else ''}{ci - peer_median_ci:.0f}pp vs peers" if peer_median_ci else f"High cost/income ratio ({ci:.0f}%)")
    if cet1 is not None:
        buffer = cet1 - REQUIRED_CET1
        if buffer < 4:
            reasons.append(f"Thin CET1 buffer ({cet1:.1f}% — only {buffer:.1f}pp above regulatory floor)")
        elif cet1 > 23:
            reasons.append(f"High CET1 ({cet1:.1f}%) — excess capital may attract acquirers")
    if pb is not None and pb < 1.1:
        reasons.append(f"Trades at {pb:.2f}x book — attractive acquisition valuation")
    if bank["alliance"] == "Independent":
        reasons.append("No alliance membership — no structural barrier to acquisition")
    if bank["alliance"] == "Eika":
        reasons.append("Eika alliance — consolidation ongoing within group")
    if not reasons:
        reasons.append("Combination of scale, efficiency, and regional factors")
    return reasons[:4]


# ── 6. Main pipeline ──────────────────────────────────────────────────────────

def run():
    print(f"[mna_pipeline] Starting — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")

    financials: dict[str, dict] = {}

    for bank in BANKS:
        ticker = bank["ticker"]
        print(f"  Fetching {ticker} …")
        fin = fetch_financials(ticker)
        # Inject hardcoded CET1
        fin["cet1"] = CET1_OVERRIDES.get(ticker)
        financials[ticker] = fin
        time.sleep(0.5)

    # Peer medians (excluding None)
    roes = [f["roe"] for f in financials.values() if f.get("roe") is not None]
    cis  = [f["cost_income"] for f in financials.values() if f.get("cost_income") is not None]
    vols = [f["earnings_volatility"] for f in financials.values() if f.get("earnings_volatility") is not None]

    peer_median_roe = float(np.median(roes)) if roes else None
    peer_median_ci  = float(np.median(cis))  if cis  else None
    peer_median_vol = float(np.median(vols)) if vols else None

    print(f"  Peer medians — ROE: {peer_median_roe}, CI: {peer_median_ci}, EV: {peer_median_vol}")

    scored = []
    for bank in BANKS:
        ticker = bank["ticker"]
        fin = financials[ticker]

        fp  = score_financial_pressure(fin["roe"], fin["cost_income"], fin["cet1"], peer_median_roe, peer_median_ci)
        sc  = score_scale(fin["assets"])
        eg  = score_efficiency_gap(fin["cost_income"], peer_median_ci)
        pb  = score_pb(fin["pb_ratio"])
        ev  = score_earnings_volatility(fin["earnings_volatility"], peer_median_vol)
        al  = score_alliance(bank["alliance"])

        # Max pairwise synergy this bank can offer any potential acquirer
        max_syn = 0.0
        for other in BANKS:
            if other["ticker"] == ticker:
                continue
            fa = financials.get(other["ticker"], {})
            ft = fin
            assets_a = fa.get("assets") or 0
            assets_t = ft.get("assets") or 0
            if assets_a >= assets_t * 0.8:
                syn = pairwise_synergy(other, bank, financials)
                max_syn = max(max_syn, syn)

        raw = (
            0.28 * fp +
            0.18 * sc +
            0.13 * eg +
            0.12 * pb +
            0.10 * ev +
            0.09 * al +
            0.10 * max_syn
        )

        scored.append({
            "bank": bank,
            "fin": fin,
            "raw": raw,
            "components": {
                "financial_pressure": round(fp, 2),
                "scale": round(sc, 2),
                "efficiency_gap": round(eg, 2),
                "pb_ratio": round(pb, 2),
                "earnings_volatility": round(ev, 2),
                "alliance": round(al, 2),
                "max_synergy": round(max_syn, 2),
            }
        })

    # Normalize to 0–100
    raws = [s["raw"] for s in scored]
    min_r, max_r = min(raws), max(raws)
    span = max_r - min_r if max_r != min_r else 1.0
    for s in scored:
        s["score"] = round((s["raw"] - min_r) / span * 100, 1)

    scored.sort(key=lambda x: x["score"], reverse=True)

    # Build output
    top_targets = []
    for s in scored[:7]:
        bank = s["bank"]
        fin = s["fin"]
        acquirers = find_likely_acquirers(bank, BANKS, financials)
        rationale = build_rationale(bank, fin, s["components"], peer_median_roe, peer_median_ci)

        top_targets.append({
            "ticker": bank["ticker"],
            "name": bank["name"],
            "alliance": bank["alliance"],
            "region": bank["region"],
            "score": s["score"],
            "components": s["components"],
            "financials": {
                "roe": round(fin["roe"], 1) if fin.get("roe") is not None else None,
                "cost_income": round(fin["cost_income"], 1) if fin.get("cost_income") is not None else None,
                "cet1": fin.get("cet1"),
                "pb_ratio": round(fin["pb_ratio"], 2) if fin.get("pb_ratio") is not None else None,
                "assets_bnok": round(fin["assets"] / 1e9, 1) if fin.get("assets") is not None else None,
            },
            "likely_acquirers": [{"ticker": a["ticker"], "name": a["name"]} for a in acquirers],
            "rationale": rationale,
        })

    output = {
        "generated": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "peer_medians": {
            "roe_pct": round(peer_median_roe, 1) if peer_median_roe is not None else None,
            "cost_income_pct": round(peer_median_ci, 1) if peer_median_ci is not None else None,
        },
        "top_targets": top_targets,
    }

    os.makedirs("data", exist_ok=True)
    out_path = os.path.join("data", "mna-targets.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"[mna_pipeline] Done — wrote {out_path}")
    for t in top_targets:
        print(f"  #{top_targets.index(t)+1} {t['name']:40s}  score={t['score']}")


if __name__ == "__main__":
    run()
