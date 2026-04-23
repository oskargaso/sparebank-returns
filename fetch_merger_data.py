"""
fetch_merger_data.py - Real price data for Norwegian sparebank mergers (Fusjoner section).

Yahoo Finance availability notes:
  SRBNK.OL (SR-Bank acquirer) -- delisted Oct 2024, no history on YF.
    SOON.OL (acquired) was renamed SB1NO.OL; retains full historical data.
  TOTG.OL (Totens Sparebank) -- not available on Yahoo Finance.
  SADG.OL (Sandnes) -- not available; renamed ROGS.OL from Jul 2024 (post-completion).
  SVEG.OL + SOR.OL -- not available; SBNOR.OL starts Apr 2025 (pre-completion).
"""
import os, json, warnings
from datetime import datetime
import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

BENCHMARK_TICKER = "OSEBX.OL"
WINDOW_PRE_DAYS  = 90
WINDOW_POST_DAYS = 730  # up to 2 years post-announcement

MERGERS = [
    # 1. SMN acquires Sore Sunnmore (announced 2022-06-20)
    # Acquirer MING.OL -- full data.
    # Sosun not listed; implied from exchange ratio (1 SOSUN -> 1.4079 MING, +33% premium).
    {
        "id": "smn",
        "title_no": "SMN + S\u00f8re Sunnm\u00f8re",
        "year": "2022/23",
        "announce_date": "2022-06-20",
        "completion_date": "2023-05-02",
        "acquirer": {
            "ticker": "MING.OL",
            "label": "SpareBank 1 SMN (MING) \u2014 kj\u00f8per",
            "available": True,
            "data_from": None,
        },
        "acquired": {
            "ticker": None,
            "label": "SpareBank 1 S\u00f8re Sunnm\u00f8re (S\u00d8SUN) \u2014 oppkj\u00f8pt, ikke notert",
            "available": False,
            "implied_from": "acquirer",
            "implied_premium_pct": 33.0,
            "tegningskurs": None,
        },
        "premium_text": "+33\u00a0% (tegningskurs NOK\u00a0103.36 vs MING markedskurs NOK\u00a0137.10)",
        "verdict_text": "Oppkj\u00f8pt vant klart",
        "verdict_class": "win",
        "note": (
            "S\u00f8re Sunnm\u00f8re Sparebank var ikke b\u00f8rsnotert. "
            "Implisitt verdi er beregnet fra bytteforholdet 1 S\u00d8SUN \u2192 1.4079 MING. "
            "MING handlet til ~NOK\u00a0137.10 p\u00e5 kunngjøringsdagen, mens tegningskursen tilsvarte "
            "NOK\u00a0103.36 per EKB \u2014 en umiddelbar implisitt gevinst p\u00e5 ~33\u00a0%. "
            "S\u00d8SUN-linjen starter ved kunngjøringsdatoen; f\u00f8r dette fantes ingen markedskurs. "
            "Etter kunngjøring f\u00f8lger implisitt S\u00d8SUN-verdi MING-kursutviklingen."
        ),
    },
    # 2. SR-Bank acquires Sorost-Norge (announced 2023-10-26)
    # SRBNK.OL delisted -- no YF history. SOON renamed SB1NO.OL -- continuous history.
    # Real price data shows SOON fell ~6% in days before announcement (111 -> 104 NOK),
    # suggesting the deal terms were seen as less favourable than expected for SOON holders.
    {
        "id": "srbnk",
        "title_no": "SR-Bank + S\u00f8r\u00f8st-Norge",
        "year": "2023/24",
        "announce_date": "2023-10-26",
        "completion_date": "2024-10-01",
        "acquirer": {
            "ticker": None,
            "label": "SpareBank 1 SR-Bank (SRBNK) \u2014 kj\u00f8per, data ikke tilgjengelig p\u00e5 YF",
            "available": False,
            "data_from": None,
        },
        "acquired": {
            "ticker": "SB1NO.OL",
            "label": "SpareBank 1 S\u00f8r\u00f8st-Norge (SOON \u2192 SB1NO) \u2014 oppkj\u00f8pt",
            "available": True,
            "implied_from": None,
            "implied_premium_pct": None,
            "tegningskurs": None,
        },
        "premium_text": "~8\u201312\u00a0% over bokverdi (0.4817 SRBNK + NOK\u00a04.33 kontant per SOON EKB)",
        "verdict_text": "Blandet \u2014 se note",
        "verdict_class": "draw",
        "note": (
            "SRBNK.OL er ikke tilgjengelig p\u00e5 Yahoo Finance (delisert okt\u00a02024). "
            "SB1NO.OL er den historiske SOON-kursen (oppkj\u00f8pt), og etter fullf\u00f8ring "
            "ogs\u00e5 den fusjonerte bankens kurs. "
            "Reelle kursdata viser at SOON falt ~6\u00a0% i dagene f\u00f8r kunngjøringen "
            "(fra ~111 til ~104\u00a0NOK), noe som tyder p\u00e5 at markedet vurderte "
            "byttevilk\u00e5rene som noe ugunstige for SOON-innehaverne relativt til markedspris. "
            "Premien p\u00e5 8\u201312\u00a0% gjelder over bokverdi, ikke over markedspris. "
            "Den fusjonerte enheten handlet videre som SpareBank 1 S\u00f8r-Norge (SB1NO.OL)."
        ),
    },
    # 3. Ostlandet acquires Totens (announced 2024-01-03)
    # SPOL.OL -- full data. TOTG.OL not on YF.
    # Tegningskurs 117.88 vs SPOL market ~107.93 at announcement (-8.4% vs tegningskurs).
    # Premium is stated over Totens book value, not over SPOL market price.
    {
        "id": "spol",
        "title_no": "\u00d8stlandet + Totens",
        "year": "2024",
        "announce_date": "2024-01-03",
        "completion_date": "2024-11-01",
        "acquirer": {
            "ticker": "SPOL.OL",
            "label": "SpareBank 1 \u00d8stlandet (SPOL) \u2014 kj\u00f8per",
            "available": True,
            "data_from": None,
        },
        "acquired": {
            "ticker": None,
            "label": "Totens Sparebank (TOTG) \u2014 oppkj\u00f8pt, ikke tilgjengelig p\u00e5 YF",
            "available": False,
            "implied_from": "acquirer",
            "implied_premium_pct": None,
            "tegningskurs": 117.88,
        },
        "premium_text": (
            "Tegningskurs NOK\u00a0117.88 per ny SPOL EKB "
            "(SPOL handlet ~107.93 ved kunngjøring \u2014 8\u00a0% under tegningskurs)"
        ),
        "verdict_text": "Komplekst \u2014 se note",
        "verdict_class": "draw",
        "note": (
            "Totens Sparebank er ikke tilgjengelig p\u00e5 Yahoo Finance. "
            "SPOL handlet til ~NOK\u00a0107.93 p\u00e5 kunngjøringsdatoen (3.\u00a0jan\u00a02024), "
            "mens tegningskursen for nye SPOL-andeler var NOK\u00a0117.88 \u2014 "
            "tegningskursen er satt ~8\u00a0% OVER SPOL sin markedskurs p\u00e5 kunngjøringen. "
            "Dette er uvanlig og betyr at Totens-innehaverne fikk SPOL-andeler papirverdsatt til 117.88, "
            "men som markedet sa var verdt 107.93. "
            "Premien er oppgitt over Totens sin bokverdi (ikke over SPOL-markedspris). "
            "TOTG-linjen er beregnet fra kunngjøringsdatoen og f\u00f8lger SPOL fra dette punktet."
        ),
    },
    # 4. Sandnes acquires Hjelmeland (announced 2023-12-07)
    # SADG.OL not on YF; renamed ROGS.OL from Jul 2024 (post-completion Aug 2024).
    # Hjelmeland not listed. Pre-announcement window unavailable.
    {
        "id": "sadg",
        "title_no": "Sandnes + Hjelmeland",
        "year": "2023/24",
        "announce_date": "2023-12-07",
        "completion_date": "2024-08-01",
        "acquirer": {
            "ticker": "ROGS.OL",
            "label": "Rogaland Sparebank (ROGS, tidl. Sandnes) \u2014 kj\u00f8per",
            "available": True,
            "data_from": "2024-07-24",
        },
        "acquired": {
            "ticker": None,
            "label": "Hjelmeland Sparebank \u2014 oppkj\u00f8pt, ikke notert",
            "available": False,
            "implied_from": "acquirer",
            "implied_premium_pct": 10.0,
            "tegningskurs": None,
        },
        "premium_text": "1 Hjelmeland \u2192 1.80 ROGS (~10\u00a0% av samlet EK til Hjelmeland)",
        "verdict_text": "Oppkj\u00f8pt vant",
        "verdict_class": "win",
        "note": (
            "SADG.OL (Sandnes Sparebank) er ikke tilgjengelig p\u00e5 Yahoo Finance. "
            "ROGS.OL (Rogaland Sparebank, omd\u00f8pt etter fusjonen) finnes kun fra juli\u00a02024 "
            "(etter fullf\u00f8ring aug\u00a02024). "
            "Kursreaksjonen ved kunngjøringen (7.\u00a0des\u00a02023) kan ikke vises fra b\u00f8rsdata. "
            "Hjelmeland var ikke b\u00f8rsnotert; implisitt premie ~10\u00a0% over bokverdi "
            "basert p\u00e5 bytteforhold 1:1.80. "
            "Grafen viser post-fullf\u00f8ring-utvikling for den fusjonerte banken."
        ),
    },
    # 5. Vest acquires Sparebanken Sor (announced 2024-08-28)
    # SVEG.OL + SOR.OL not on YF. SBNOR.OL starts 2025-04-23 (before completion 2025-05-02).
    {
        "id": "vest",
        "title_no": "Vest + Sparebanken S\u00f8r",
        "year": "2024/25",
        "announce_date": "2024-08-28",
        "completion_date": "2025-05-02",
        "acquirer": {
            "ticker": "SBNOR.OL",
            "label": "Sparebanken Norge (SBNOR, tidl. Vest) \u2014 kj\u00f8per",
            "available": True,
            "data_from": "2025-04-23",
        },
        "acquired": {
            "ticker": None,
            "label": "Sparebanken S\u00f8r (SOR) \u2014 oppkj\u00f8pt, data ikke tilgjengelig p\u00e5 YF",
            "available": False,
            "implied_from": None,
            "implied_premium_pct": None,
            "tegningskurs": None,
        },
        "premium_text": "Tegningskurs NOK\u00a089.10 = estimert bokverdi (minimal direkte premie)",
        "verdict_text": "Uavgjort",
        "verdict_class": "draw",
        "note": (
            "Verken SVEG.OL (Sparebanken Vest) eller SOR.OL (Sparebanken S\u00f8r) "
            "er tilgjengelig p\u00e5 Yahoo Finance. "
            "SBNOR.OL (Sparebanken Norge) starter 23.\u00a0april\u00a02025, like f\u00f8r fullf\u00f8ring "
            "2.\u00a0mai\u00a02025. Kursreaksjonen ved kunngjøringen (28.\u00a0aug\u00a02024) kan ikke vises. "
            "Tegningskursen (NOK\u00a089.10) ble satt lik estimert bokverdi \u2014 ingen direkte "
            "markedspremie ved kunngjøringen. "
            "Grafen viser SBNOR fra noteringstidspunktet."
        ),
    },
]


# ---------------------------------------------------------------------------
def fetch_weekly(ticker, start, end):
    """Fetch weekly Friday close prices. Returns pd.Series or None."""
    try:
        h = yf.Ticker(ticker).history(start=start, end=end, auto_adjust=True)
        if h.empty:
            print("  EMPTY  " + ticker)
            return None
        if h.index.tz:
            h.index = h.index.tz_localize(None)
        w = h["Close"].resample("W-FRI").last().dropna()
        print(f"  OK     {ticker}: {len(w)} weeks  "
              f"{w.index[0].date()} - {w.index[-1].date()}")
        return w
    except Exception as exc:
        print(f"  FAIL   {ticker}: {exc}")
        return None


def nearest(series, ts):
    return int(series.index.get_indexer([pd.Timestamp(ts)], method="nearest")[0])


def norm100(series, base_ts):
    i = nearest(series, base_ts)
    v = series.iloc[i]
    return (series / v * 100).round(2) if v > 0 else None


def react(series, ann_i):
    if ann_i < 1 or ann_i >= len(series):
        return None
    b, a = float(series.iloc[ann_i - 1]), float(series.iloc[ann_i])
    return round((a / b - 1) * 100, 1) if (not pd.isna(b)) and b > 0 else None


def tol(col):
    return [None if pd.isna(v) else round(float(v), 2) for v in col]


# ---------------------------------------------------------------------------
def process_merger(m, bench_raw):
    ann_ts    = pd.Timestamp(m["announce_date"])
    data_from = m["acquirer"].get("data_from")
    start_ts  = (pd.Timestamp(data_from) if data_from
                 else ann_ts - pd.Timedelta(days=WINDOW_PRE_DAYS))
    end_ts    = min(ann_ts + pd.Timedelta(days=WINDOW_POST_DAYS),
                   pd.Timestamp.today().normalize())
    s, e      = start_ts.strftime("%Y-%m-%d"), end_ts.strftime("%Y-%m-%d")

    print(f"\n[{m['id']}]  Announce: {m['announce_date']}  Window: {s} to {e}")

    # -- acquirer --
    acq_raw  = None
    acq_norm = None
    acq_cfg  = m["acquirer"]
    if acq_cfg["available"] and acq_cfg["ticker"]:
        acq_raw  = fetch_weekly(acq_cfg["ticker"], s, e)
        acq_norm = norm100(acq_raw, start_ts) if acq_raw is not None else None

    # -- benchmark --
    bench_norm = None
    if bench_raw is not None:
        bsl = bench_raw[s:e]
        if not bsl.empty:
            bench_norm = norm100(bsl, start_ts)

    # master index: prefer acquirer, fall back to benchmark
    ref = acq_norm if acq_norm is not None else bench_norm
    if ref is None:
        print("  SKIP: no master series")
        return None
    master = ref.index

    # -- acquired --
    acqd_cfg  = m["acquired"]
    acqd_norm = None
    acqd_imp  = True

    if acqd_cfg["available"] and acqd_cfg["ticker"]:
        acqd_raw = fetch_weekly(acqd_cfg["ticker"], s, e)
        if acqd_raw is not None:
            acqd_norm = norm100(acqd_raw, start_ts)
            acqd_imp  = False

    # build implied series for unlisted / unavailable acquired banks
    if acqd_norm is None and acqd_cfg.get("implied_from") == "acquirer" and acq_norm is not None:
        prem = acqd_cfg.get("implied_premium_pct")

        # Derive premium from tegningskurs vs acquirer price at announcement
        if prem is None and acqd_cfg.get("tegningskurs") and acq_raw is not None:
            ai   = nearest(acq_raw, ann_ts)
            prem = round((float(acq_raw.iloc[ai]) / acqd_cfg["tegningskurs"] - 1) * 100, 1)
            m["computed_premium_pct"] = prem
            print(f"  Computed implied premium: {prem:+.1f}%")

        if prem is not None:
            ai_n  = nearest(acq_norm, ann_ts)
            base_v = float(acq_norm.iloc[ai_n])
            impl   = pd.Series(index=acq_norm.index, dtype=float)
            for i, v in enumerate(acq_norm):
                impl.iloc[i] = (float("nan") if i < ai_n
                                else round(100.0 * (1 + prem / 100.0) * float(v) / base_v, 2))
            acqd_norm = impl

    # -- DataFrame --
    df = pd.DataFrame(index=master)
    if acq_norm  is not None: df["acquirer"]  = acq_norm.reindex(master,  method="nearest")
    if acqd_norm is not None: df["acquired"]  = acqd_norm.reindex(master, method="nearest")
    if bench_norm is not None: df["benchmark"] = bench_norm.reindex(master, method="nearest")
    df = df.ffill()

    ann_i  = int(master.get_indexer([ann_ts], method="nearest")[0])
    acq_r  = react(df["acquirer"], ann_i) if "acquirer" in df else None
    acqd_r = react(df["acquired"], ann_i) if "acquired" in df else None
    if acq_r  is not None: print(f"  Acquirer reaction: {acq_r:+.1f}%")
    if acqd_r is not None: print(f"  Acquired reaction: {acqd_r:+.1f}%")

    return {
        "id":                m["id"],
        "title":             m["title_no"],
        "year":              m["year"],
        "announce_date":     m["announce_date"],
        "completion_date":   m["completion_date"],
        "announce_idx":      ann_i,
        "window_start":      s,
        "pre_announce_available": not bool(data_from),
        "labels":            [d.strftime("%d.%m.%y") for d in df.index],
        "acquirer": {
            "label":     acq_cfg["label"],
            "ticker":    acq_cfg["ticker"],
            "available": acq_cfg["available"],
            "data":      tol(df["acquirer"])  if "acquirer"  in df else [],
        },
        "acquired": {
            "label":     acqd_cfg["label"],
            "ticker":    acqd_cfg.get("ticker"),
            "available": acqd_cfg["available"],
            "implied":   acqd_imp,
            "data":      tol(df["acquired"])  if "acquired"  in df else [],
        },
        "benchmark": {
            "label": "OSEBX (referanse)",
            "data":  tol(df["benchmark"]) if "benchmark" in df else [],
        },
        "metrics": {
            "acquirer_react": acq_r,
            "acquired_react": acqd_r,
            "computed_premium_pct": m.get("computed_premium_pct"),
        },
        "premium_text":  m["premium_text"],
        "verdict_text":  m["verdict_text"],
        "verdict_class": m["verdict_class"],
        "note":          m["note"],
    }


# ---------------------------------------------------------------------------
def main():
    os.makedirs("data", exist_ok=True)
    print("=" * 60)
    print("fetch_merger_data.py - Norwegian sparebank mergers")
    print("=" * 60)
    print("\nFetching OSEBX benchmark (2021-today)...")
    bench_raw = fetch_weekly(BENCHMARK_TICKER, "2021-01-01",
                             datetime.today().strftime("%Y-%m-%d"))

    results = [r for m in MERGERS if (r := process_merger(m, bench_raw)) is not None]

    out = {
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M UTC"),
        "mergers":   results,
    }
    out_path = "data/mergers.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, separators=(",", ":"), ensure_ascii=False)
    print(f"\nDone. Wrote {out_path} with {len(results)}/{len(MERGERS)} mergers.")


if __name__ == "__main__":
    main()
