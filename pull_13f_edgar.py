#!/usr/bin/env python3
"""
Run:
  python pull_13f_edgar_v18.py --user-agent "Name email" --out-dir "/Users/admin"
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin

import requests
import xml.etree.ElementTree as ET

SEC_DATA_BASE = "https://data.sec.gov/"
SEC_ARCHIVES_BASE = "https://www.sec.gov/Archives/"
SEC_COMPANY_TICKERS = "https://www.sec.gov/files/company_tickers.json"
SEC_COMPANY_FACTS = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

DEFAULT_START_DATE = "2010-01-01"
DEFAULT_MIN_REPORT_DATE = "2009-12-31"
FORMS = {"13F-HR", "13F-HR/A"}

GROUPS: Dict[str, Dict[str, Any]] = {
    "BlackRock": {"seed_ciks": ["0001364742", "0000913414"], "keywords": ["blackrock"]},
    "Vanguard": {"seed_ciks": ["0000102909", "0000862084"], "keywords": ["vanguard"]},
    "StateStreet": {
        "seed_ciks": ["0000093751", "0001257442", "0001324601", "0001324602"],
        "keywords": ["state street", "ssga", "state street global advisors"],
    },
}

RAW_COLUMNS = [
    "group","ticker","mapped_cusip","filer_cik","form","filing_date","report_date","accession","info_table_url",
    "issuer_name","class_title","cusip","value_usd_thousands","shares_or_principal","shares_or_principal_type",
    "put_call","investment_discretion","other_manager","voting_sole","voting_shared","voting_none","shares_outstanding",
]

PANEL_COLUMNS = [
    "group","ticker","mapped_cusip","report_date",
    "shares_or_principal_consolidated","value_usd_thousands_consolidated",
    "consolidation_method","selected_filer_cik","selected_accession","selected_filing_date","shares_outstanding",
]

BVS_COLUMNS = [
    "group","ticker","mapped_cusip","report_date","shares_held_total","value_usd_thousands_total","shares_outstanding",
]

README_TEXT = """EDGAR 13F Holdings Output — README (v18)

Mode:
- raw   -> ONLY per-group RAW files: <Group>_13f_holdings_raw.csv
- panel -> ONLY per-group PANEL files: <Group>_13f_holdings_panel.csv
          plus BVS_13f_holdings_panel.csv

Ordering:
- ticker ASC, report_date ASC (oldest -> newest)

Report period filter:
- report_date >= min_report_date (default: 2009-12-31)
  Use --min-report-date 2010-03-31 for the first 2010 report quarter only.

shares_outstanding:
- From SEC companyfacts (XBRL) dei/us-gaap shares outstanding.
- If holdings > shares_outstanding, shares_outstanding is blanked.

De-dup / consolidation:
- RAW: latest filing per (filer_cik, ticker, report_date, cusip)
- PANEL: MAX across filer CIKs per (ticker, report_date) inside each manager to avoid overlap double counting.
"""

@dataclass
class Filing:
    group: str
    cik: str
    cik_int: str
    form: str
    accession: str
    filing_date: str
    report_date: Optional[str]
    primary_doc: Optional[str]

def pad_cik(cik: str) -> str:
    return re.sub(r"\D", "", cik).zfill(10)

def cik_int_str(cik: str) -> str:
    return str(int(re.sub(r"\D", "", cik)))

def accession_nodash(acc: str) -> str:
    return acc.replace("-", "")

def parse_iso_date_or_none(s: Any) -> Optional[dt.date]:
    if s is None:
        return None
    x = str(s).strip()
    if not x:
        return None
    if re.fullmatch(r"\d{8}", x):
        try:
            return dt.date(int(x[0:4]), int(x[4:6]), int(x[6:8]))
        except Exception:
            return None
    try:
        return dt.date.fromisoformat(x)
    except Exception:
        return None

def normalize_report_date_str(s: Any) -> str:
    d = parse_iso_date_or_none(s)
    return d.isoformat() if d else ""

def safe_int(s: Any) -> int:
    if s is None:
        return 0
    if isinstance(s, int):
        return s
    x = str(s).strip()
    if not x:
        return 0
    try:
        return int(float(x))
    except Exception:
        return 0

def sec_get(session: requests.Session, url: str, retries: int = 12, timeout: int = 60) -> requests.Response:
    backoff = 1.0
    last = None
    for _ in range(retries):
        r = session.get(url, timeout=timeout)
        last = r
        if r.status_code == 200:
            return r
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 90.0)
            continue
        raise RuntimeError(f"GET {url} failed: {r.status_code} {r.text[:200]}")
    raise RuntimeError(f"GET {url} failed after retries: {getattr(last,'status_code',None)}")

def fetch_submissions(session: requests.Session, cik: str) -> Dict[str, Any]:
    url = urljoin(SEC_DATA_BASE, f"submissions/CIK{pad_cik(cik)}.json")
    return sec_get(session, url).json()

def fetch_submissions_file(session: requests.Session, name: str) -> Dict[str, Any]:
    url = urljoin(SEC_DATA_BASE, f"submissions/{name}")
    return sec_get(session, url).json()

def rows_from_columnar(col: Dict[str, Any]) -> List[Dict[str, Any]]:
    forms = col.get("form", [])
    if not isinstance(forms, list):
        return []
    n = len(forms)
    out = []
    for i in range(n):
        out.append({
            "form": col.get("form", [None])[i],
            "accessionNumber": col.get("accessionNumber", [None])[i],
            "filingDate": col.get("filingDate", [None])[i],
            "reportDate": col.get("reportDate", [None])[i],
            "primaryDocument": col.get("primaryDocument", [None])[i],
        })
    return out

def normalize_submission_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    filings = payload.get("filings")
    if isinstance(filings, dict):
        recent = filings.get("recent")
        if isinstance(recent, dict) and isinstance(recent.get("form"), list):
            return rows_from_columnar(recent)
        if isinstance(filings.get("form"), list):
            return rows_from_columnar(filings)
    if isinstance(filings, list):
        return filings
    recent = payload.get("recent")
    if isinstance(recent, dict) and isinstance(recent.get("form"), list):
        return rows_from_columnar(recent)
    if isinstance(payload.get("form"), list):
        return rows_from_columnar(payload)
    return []

def iter_filing_rows(sub: Dict[str, Any], session: requests.Session) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    rows.extend(normalize_submission_payload(sub))
    files = (sub.get("filings", {}).get("files", []) or [])
    for f in files:
        try:
            older = fetch_submissions_file(session, f["name"])
            rows.extend(normalize_submission_payload(older))
        except Exception:
            pass
        time.sleep(0.15)
    return rows

def has_any_13f_in_recent(sub: Dict[str, Any]) -> bool:
    recent = sub.get("filings", {}).get("recent", {}) or {}
    forms = recent.get("form", []) if isinstance(recent.get("form"), list) else []
    return any((f or "").strip() in FORMS for f in forms)

def filing_base_dir(f: Filing) -> str:
    return urljoin(SEC_ARCHIVES_BASE, f"edgar/data/{f.cik_int}/{accession_nodash(f.accession)}/")

def infer_report_date_from_primary(session: requests.Session, filing: Filing) -> str:
    base = filing_base_dir(filing)
    candidates: List[str] = []
    if filing.primary_doc:
        candidates.append(filing.primary_doc)
    candidates.extend(["primary_doc.xml", "submission.txt"])
    seen = set()
    for name in candidates:
        if not name or name in seen:
            continue
        seen.add(name)
        try:
            content = sec_get(session, urljoin(base, name), timeout=45).content
        except Exception:
            continue
        text = content.decode("utf-8", errors="ignore")
        m = re.search(r"<reportCalendarOrQuarter>\s*([0-9]{4}-[0-9]{2}-[0-9]{2})\s*</reportCalendarOrQuarter>", text, flags=re.I)
        if m:
            return m.group(1)
        m = re.search(r"CONFORMED\s+PERIOD\s+OF\s+REPORT:\s*([0-9]{8})", text, flags=re.I)
        if m:
            d = parse_iso_date_or_none(m.group(1))
            return d.isoformat() if d else ""
    return ""

def get_13f_filings_for_cik(session: requests.Session, group: str, cik: str, start_filing_date: dt.date) -> List[Filing]:
    sub = fetch_submissions(session, cik)
    rows = iter_filing_rows(sub, session)
    out: List[Filing] = []
    for r in rows:
        form = (r.get("form") or "").strip()
        if form not in FORMS:
            continue
        filing_date = r.get("filingDate")
        accession = r.get("accessionNumber")
        if not filing_date or not accession:
            continue
        fd = parse_iso_date_or_none(filing_date)
        if fd is None or fd < start_filing_date:
            continue
        out.append(Filing(
            group=group,
            cik=pad_cik(cik),
            cik_int=cik_int_str(cik),
            form=form,
            accession=accession,
            filing_date=str(filing_date),
            report_date=r.get("reportDate"),
            primary_doc=r.get("primaryDocument"),
        ))
    out.sort(key=lambda x: (parse_iso_date_or_none(x.filing_date) or dt.date(9999,12,31), x.accession, x.cik))
    return out

def list_filing_directory_items(session: requests.Session, filing: Filing) -> Tuple[str, List[Dict[str, Any]]]:
    base = filing_base_dir(filing)
    idx = sec_get(session, urljoin(base, "index.json")).json()
    items = idx.get("directory", {}).get("item", []) or []
    return base, items

def find_info_table_url(session: requests.Session, filing: Filing) -> Optional[str]:
    base, items = list_filing_directory_items(session, filing)
    xmls: List[Tuple[str, str, int]] = []
    for it in items:
        name = (it.get("name") or "")
        lname = name.lower()
        if not lname.endswith(".xml"):
            continue
        size = int(it.get("size") or 0)
        xmls.append((name, lname, size))
    if not xmls:
        return None
    candidates = [(n, ln, sz) for (n, ln, sz) in xmls if "primary" not in ln and "primary_doc" not in ln]
    if not candidates:
        candidates = xmls
    for kw in ("infotable", "informationtable", "information_table", "form13f", "13f_"):
        preferred = [c for c in candidates if kw in c[1]]
        if preferred:
            preferred.sort(key=lambda x: (-x[2], len(x[0]), x[0]))
            return urljoin(base, preferred[0][0])
    candidates.sort(key=lambda x: (-x[2], len(x[0]), x[0]))
    return urljoin(base, candidates[0][0])

def strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag

def parse_info_table_xml(xml_bytes: bytes) -> List[Dict[str, Any]]:
    root = ET.fromstring(xml_bytes)
    out: List[Dict[str, Any]] = []
    for el in root.iter():
        if strip_ns(el.tag) != "infoTable":
            continue

        def child_text(parent, name: str) -> Optional[str]:
            for ch in parent:
                if strip_ns(ch.tag) == name:
                    return (ch.text or "").strip()
            return None

        row: Dict[str, Any] = {
            "issuer_name": child_text(el, "nameOfIssuer") or "",
            "class_title": child_text(el, "titleOfClass") or "",
            "cusip": child_text(el, "cusip") or "",
            "value_usd_thousands": child_text(el, "value") or "",
            "put_call": child_text(el, "putCall") or "",
            "investment_discretion": child_text(el, "investmentDiscretion") or "",
            "other_manager": child_text(el, "otherManager") or "",
            "shares_or_principal": "",
            "shares_or_principal_type": "",
            "voting_sole": "",
            "voting_shared": "",
            "voting_none": "",
        }
        for ch in el:
            if strip_ns(ch.tag) == "shrsOrPrnAmt":
                row["shares_or_principal"] = child_text(ch, "sshPrnamt") or ""
                row["shares_or_principal_type"] = child_text(ch, "sshPrnamtType") or ""
        for ch in el:
            if strip_ns(ch.tag) == "votingAuthority":
                row["voting_sole"] = child_text(ch, "Sole") or ""
                row["voting_shared"] = child_text(ch, "Shared") or ""
                row["voting_none"] = child_text(ch, "None") or ""
        out.append(row)
    return out

def load_ticker_to_cusip_map(path: str) -> Dict[str, str]:
    m: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        if not r.fieldnames:
            return m
        lower = [c.lower() for c in r.fieldnames]
        if "ticker" not in lower or "cusip" not in lower:
            raise ValueError("ticker_cusip.csv must have headers: ticker,cusip")
        tcol = r.fieldnames[lower.index("ticker")]
        ccol = r.fieldnames[lower.index("cusip")]
        for row in r:
            t = (row.get(tcol) or "").strip().upper()
            c = (row.get(ccol) or "").strip()
            if t and c:
                m[t] = c
    return m

def build_ticker_to_issuer_cik(session: requests.Session, verbose: bool) -> Dict[str, str]:
    if verbose:
        print("Downloading SEC company_tickers.json (this can take ~5-20s)…", flush=True)
    data = sec_get(session, SEC_COMPANY_TICKERS).json()
    if verbose:
        print("Loaded company_tickers.json.", flush=True)
    mapping: Dict[str, str] = {}
    for _, row in data.items():
        t = (row.get("ticker") or "").strip().upper()
        c = str(row.get("cik_str") or "").strip()
        if t and c:
            mapping[t] = pad_cik(c)
    return mapping

def _extract_shares_outstanding_units(facts_block: Dict[str, Any]) -> List[Dict[str, Any]]:
    item = facts_block.get("EntityCommonStockSharesOutstanding") or {}
    units = item.get("units") or {}
    vals = units.get("shares") or []
    return vals if isinstance(vals, list) else []

def fetch_shares_outstanding_series(session: requests.Session, issuer_cik_padded: str) -> List[Tuple[dt.date, int]]:
    url = SEC_COMPANY_FACTS.format(cik=issuer_cik_padded)
    data = sec_get(session, url).json()
    facts = data.get("facts") or {}
    dei = facts.get("dei") or {}
    usg = facts.get("us-gaap") or {}
    vals = _extract_shares_outstanding_units(dei)
    if not vals:
        vals = _extract_shares_outstanding_units(usg)
    out: List[Tuple[dt.date, int]] = []
    for v in vals:
        end = parse_iso_date_or_none(v.get("end"))
        val = v.get("val")
        if end and val is not None:
            try:
                out.append((end, int(float(val))))
            except Exception:
                continue
    out.sort(key=lambda x: x[0])
    return out

def pick_shares_outstanding(series: List[Tuple[dt.date, int]], report_date: dt.date) -> Optional[int]:
    if not series:
        return None
    for end, val in reversed(series):
        if end == report_date:
            return val
    cutoff = report_date - dt.timedelta(days=120)
    candidates = [(end, val) for (end, val) in series if cutoff <= end <= report_date]
    if candidates:
        candidates.sort(key=lambda x: x[0])
        return candidates[-1][1]
    return None

def write_csv(path: str, cols: List[str], rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})

def raw_sort_key(r: Dict[str, Any]) -> Tuple[str, dt.date, dt.date, str]:
    t = (r.get("ticker") or "")
    rep = parse_iso_date_or_none(r.get("report_date")) or dt.date(9999, 12, 31)
    fil = parse_iso_date_or_none(r.get("filing_date")) or dt.date(9999, 12, 31)
    acc = r.get("accession") or ""
    return (t, rep, fil, acc)

def panel_sort_key(r: Dict[str, Any]) -> Tuple[str, dt.date]:
    t = (r.get("ticker") or "")
    rep = parse_iso_date_or_none(r.get("report_date")) or dt.date(9999, 12, 31)
    return (t, rep)

def prompt_mode() -> str:
    while True:
        m = input('Output mode? Type "raw" or "panel": ').strip().lower()
        if m in ("raw", "panel"):
            return m
        print("Please type exactly: raw or panel", flush=True)

def delete_if_exists(path: str) -> None:
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass

def _latest_rank(form: str, filing_date: str, accession: str) -> Tuple[int, dt.date, str]:
    is_amend = 1 if (form or "") == "13F-HR/A" else 0
    fd = parse_iso_date_or_none(filing_date) or dt.date(1900,1,1)
    return (is_amend, fd, accession or "")

def dedupe_raw_keep_latest(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best: Dict[Tuple[str,str,str,str], Dict[str, Any]] = {}
    for r in rows:
        key = (r.get("filer_cik") or "", r.get("ticker") or "", r.get("report_date") or "", r.get("cusip") or "")
        cur = best.get(key)
        if cur is None or _latest_rank(r.get("form",""), r.get("filing_date",""), r.get("accession","")) > _latest_rank(cur.get("form",""), cur.get("filing_date",""), cur.get("accession","")):
            best[key] = r
    out = list(best.values())
    out.sort(key=raw_sort_key)
    return out

def panel_build_max_across_filers(group: str, raw_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    within: Dict[Tuple[str,str,str,str,str], Dict[str, Any]] = {}
    for r in raw_rows:
        key = (r.get("accession") or "", r.get("filer_cik") or "", r.get("report_date") or "", r.get("ticker") or "", r.get("cusip") or "")
        if key not in within:
            within[key] = dict(r)
        else:
            within[key]["shares_or_principal"] = safe_int(within[key].get("shares_or_principal")) + safe_int(r.get("shares_or_principal"))
            within[key]["value_usd_thousands"] = safe_int(within[key].get("value_usd_thousands")) + safe_int(r.get("value_usd_thousands"))
            if not within[key].get("shares_outstanding") and r.get("shares_outstanding"):
                within[key]["shares_outstanding"] = r.get("shares_outstanding")
    collapsed = list(within.values())

    best_version: Dict[Tuple[str,str,str,str], Dict[str, Any]] = {}
    for r in collapsed:
        key = (r.get("filer_cik") or "", r.get("report_date") or "", r.get("ticker") or "", r.get("cusip") or "")
        cur = best_version.get(key)
        if cur is None or _latest_rank(r.get("form",""), r.get("filing_date",""), r.get("accession","")) > _latest_rank(cur.get("form",""), cur.get("filing_date",""), cur.get("accession","")):
            best_version[key] = r
    latest = list(best_version.values())

    chosen: Dict[Tuple[str,str], Dict[str, Any]] = {}
    for r in latest:
        t = r.get("ticker") or ""
        rep = r.get("report_date") or ""
        k = (t, rep)
        if k not in chosen or safe_int(r.get("shares_or_principal")) > safe_int(chosen[k].get("shares_or_principal")):
            chosen[k] = r

    out: List[Dict[str, Any]] = []
    for (t, rep), r in chosen.items():
        shares = safe_int(r.get("shares_or_principal"))
        valk = safe_int(r.get("value_usd_thousands"))
        so = r.get("shares_outstanding") or ""
        try:
            so_i = int(so) if str(so).strip() else None
        except Exception:
            so_i = None
        if so_i is not None and so_i > 0 and shares > so_i:
            so = ""
        out.append({
            "group": group,
            "ticker": t,
            "mapped_cusip": r.get("mapped_cusip") or "",
            "report_date": rep,
            "shares_or_principal_consolidated": shares,
            "value_usd_thousands_consolidated": valk,
            "consolidation_method": "MAX_ACROSS_FILER_CIKS",
            "selected_filer_cik": r.get("filer_cik") or "",
            "selected_accession": r.get("accession") or "",
            "selected_filing_date": r.get("filing_date") or "",
            "shares_outstanding": so,
        })
    out.sort(key=panel_sort_key)
    return out

def build_bvs(panel_by_group: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    agg: Dict[Tuple[str,str], Dict[str, Any]] = {}
    for rows in panel_by_group.values():
        for r in rows:
            t = r.get("ticker") or ""
            rep = r.get("report_date") or ""
            k = (t, rep)
            if k not in agg:
                agg[k] = {
                    "group": "BVS",
                    "ticker": t,
                    "mapped_cusip": r.get("mapped_cusip") or "",
                    "report_date": rep,
                    "shares_held_total": 0,
                    "value_usd_thousands_total": 0,
                    "shares_outstanding": r.get("shares_outstanding") or "",
                }
            agg[k]["shares_held_total"] += safe_int(r.get("shares_or_principal_consolidated"))
            agg[k]["value_usd_thousands_total"] += safe_int(r.get("value_usd_thousands_consolidated"))
            if not agg[k]["shares_outstanding"] and r.get("shares_outstanding"):
                agg[k]["shares_outstanding"] = r.get("shares_outstanding") or ""

    out = list(agg.values())
    for r in out:
        so = r.get("shares_outstanding") or ""
        try:
            so_i = int(so) if str(so).strip() else None
        except Exception:
            so_i = None
        if so_i is not None and so_i > 0 and safe_int(r.get("shares_held_total")) > so_i:
            r["shares_outstanding"] = ""
    out.sort(key=panel_sort_key)
    return out

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--user-agent", required=True)
    ap.add_argument("--start-date", default=DEFAULT_START_DATE)
    ap.add_argument("--min-report-date", default=DEFAULT_MIN_REPORT_DATE)
    ap.add_argument("--out-dir", default=os.path.expanduser("~"))
    ap.add_argument("--ticker-cusip-csv", default=os.path.join(os.path.expanduser("~"), "ticker_cusip.csv"))
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--no-auto-cik", action="store_true")
    ap.add_argument("--auto-cik-max", type=int, default=250)
    ap.add_argument("--mode", choices=["raw", "panel"])
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    verbose = not args.quiet
    mode = args.mode or prompt_mode()
    want_panel = (mode == "panel")
    if verbose:
        print(f"Mode selected: {mode}", flush=True)

    out_dir = os.path.abspath(os.path.expanduser(args.out_dir))
    os.makedirs(out_dir, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": args.user_agent, "Accept-Encoding": "gzip, deflate"})

    start_filing = dt.date.fromisoformat(args.start_date)
    min_report = dt.date.fromisoformat(args.min_report_date)

    if verbose:
        print(f"Loading ticker map: {args.ticker_cusip_csv}", flush=True)
    ticker_to_cusip = load_ticker_to_cusip_map(args.ticker_cusip_csv)
    if not ticker_to_cusip:
        raise RuntimeError(f"No rows found in ticker map: {args.ticker_cusip_csv}")
    if verbose:
        print(f"Tickers loaded ({len(ticker_to_cusip)}): {', '.join(sorted(ticker_to_cusip.keys()))}", flush=True)

    cusip_to_tickers: Dict[str, List[str]] = {}
    for t, c in ticker_to_cusip.items():
        cusip_to_tickers.setdefault(c, []).append(t)
    allowed_cusips: Set[str] = set(cusip_to_tickers.keys())

    ticker_to_issuer_cik = build_ticker_to_issuer_cik(session, verbose=verbose)
    shares_series_cache: Dict[str, List[Tuple[dt.date, int]]] = {}

    def get_shares_outstanding_for(ticker: str, report_date_str: str) -> Optional[int]:
        d = parse_iso_date_or_none(report_date_str)
        if d is None:
            return None
        issuer_cik = ticker_to_issuer_cik.get(ticker.upper())
        if not issuer_cik:
            return None
        if issuer_cik not in shares_series_cache:
            try:
                shares_series_cache[issuer_cik] = fetch_shares_outstanding_series(session, issuer_cik)
            except Exception:
                shares_series_cache[issuer_cik] = []
        return pick_shares_outstanding(shares_series_cache[issuer_cik], d)

    with open(os.path.join(out_dir, "13F_OUTPUT_README.txt"), "w", encoding="utf-8") as f:
        f.write(README_TEXT)
    if verbose:
        print(f"Wrote README: {os.path.join(out_dir,'13F_OUTPUT_README.txt')}", flush=True)

    # Build group CIKs
    group_ciks: Dict[str, List[str]] = {}
    if args.no_auto_cik:
        for group, cfg in GROUPS.items():
            group_ciks[group] = sorted(set(pad_cik(x) for x in cfg["seed_ciks"]))
    else:
        if verbose:
            print("Building candidate CIK sets (auto-expanding)…", flush=True)
        companies = sec_get(session, SEC_COMPANY_TICKERS).json()
        companies_list = [v for _, v in companies.items()]
        for group, cfg in GROUPS.items():
            kws = [k.lower().strip() for k in cfg["keywords"] if k.strip()]
            candidates = [c for c in companies_list if any(kw in (c.get("title") or "").lower() for kw in kws)]
            def score(c):
                title = (c.get("title") or "").lower()
                hits = sum(1 for kw in kws if kw in title)
                return (-hits, len(title))
            candidates.sort(key=score)
            candidates = candidates[: args.auto_cik_max]
            verified: Set[str] = set(pad_cik(x) for x in cfg["seed_ciks"])

            if verbose:
                print(f"  {group}: scanning up to {len(candidates)} candidates…", flush=True)

            for i, c in enumerate(candidates, start=1):
                cik = str(c.get("cik_str") or "").strip()
                if not cik:
                    continue
                cik_p = pad_cik(cik)
                if cik_p in verified:
                    continue
                try:
                    sub = fetch_submissions(session, cik_p)
                    if has_any_13f_in_recent(sub):
                        verified.add(cik_p)
                except Exception:
                    pass
                if verbose and i % 50 == 0:
                    print(f"    {group}: scanned {i}/{len(candidates)}… (verified={len(verified)})", flush=True)
                time.sleep(0.02)
            group_ciks[group] = sorted(verified)
            if verbose:
                print(f"  {group}: using {len(group_ciks[group])} filer CIK(s).", flush=True)

    # Clean stale outputs from other mode
    if want_panel:
        for g in GROUPS.keys():
            delete_if_exists(os.path.join(out_dir, f"{g}_13f_holdings_raw.csv"))
    else:
        for g in GROUPS.keys():
            delete_if_exists(os.path.join(out_dir, f"{g}_13f_holdings_panel.csv"))
        delete_if_exists(os.path.join(out_dir, "BVS_13f_holdings_panel.csv"))

    panel_by_group: Dict[str, List[Dict[str, Any]]] = {}

    for group, ciks in group_ciks.items():
        if verbose:
            print(f"\n== {group} ==", flush=True)
            print(f"CIKs: {', '.join(ciks)}", flush=True)
            print(f"Fetching filings since {args.start_date} (min report_date {args.min_report_date})…", flush=True)

        filings: List[Filing] = []
        for idx, cik in enumerate(ciks, start=1):
            try:
                f = get_13f_filings_for_cik(session, group, cik, start_filing)
                filings.extend(f)
                if verbose:
                    print(f"  [{idx}/{len(ciks)}] CIK {cik}: {len(f)} filings", flush=True)
            except Exception as e:
                if verbose:
                    print(f"  [{idx}/{len(ciks)}] CIK {cik}: ERROR {e}", flush=True)
                continue

        seen = set()
        uniq: List[Filing] = []
        for fobj in filings:
            key = (fobj.cik, fobj.accession)
            if key in seen:
                continue
            seen.add(key)
            uniq.append(fobj)

        uniq.sort(key=lambda x: (parse_iso_date_or_none(x.filing_date) or dt.date(9999,12,31), x.accession, x.cik))

        if verbose:
            print(f"Total filings (deduped): {len(uniq)}", flush=True)
            if len(uniq) == 0:
                print("No filings found — check CIKs or SEC throttling.", flush=True)

        raw_rows: List[Dict[str, Any]] = []
        for i, fobj in enumerate(uniq, start=1):
            rep_norm = normalize_report_date_str(fobj.report_date) or infer_report_date_from_primary(session, fobj)
            rep_date = parse_iso_date_or_none(rep_norm)
            if rep_date is not None and rep_date < min_report:
                continue

            if verbose and (i == 1 or i % 25 == 0 or i == len(uniq)):
                print(f"  Filing {i}/{len(uniq)}: {fobj.form} filed {fobj.filing_date} report {rep_norm} CIK {fobj.cik}", flush=True)

            try:
                info_url = find_info_table_url(session, fobj)
                if not info_url:
                    time.sleep(args.sleep)
                    continue
                xml = sec_get(session, info_url).content
                parsed = parse_info_table_xml(xml)

                so_cache_local: Dict[Tuple[str, str], Optional[int]] = {}

                for r in parsed:
                    cusip = (r.get("cusip") or "").strip()
                    if not cusip or cusip not in allowed_cusips:
                        continue
                    for ticker in cusip_to_tickers.get(cusip, []):
                        so_key = (ticker, rep_norm)
                        if so_key not in so_cache_local:
                            so_cache_local[so_key] = get_shares_outstanding_for(ticker, rep_norm) if rep_norm else None
                        so = so_cache_local[so_key]

                        out_row = {k: "" for k in RAW_COLUMNS}
                        out_row.update(r)
                        out_row["group"] = group
                        out_row["ticker"] = ticker
                        out_row["mapped_cusip"] = ticker_to_cusip.get(ticker, "")
                        out_row["filer_cik"] = fobj.cik
                        out_row["form"] = fobj.form
                        out_row["filing_date"] = str(fobj.filing_date).strip()
                        out_row["report_date"] = rep_norm
                        out_row["accession"] = fobj.accession
                        out_row["info_table_url"] = info_url
                        out_row["shares_outstanding"] = str(so) if so is not None else ""
                        raw_rows.append(out_row)

            except Exception as e:
                if verbose:
                    print(f"    Parse error: {e}", flush=True)

            time.sleep(args.sleep)

        raw_rows_sorted = sorted(raw_rows, key=raw_sort_key)

        # de-dupe latest filing per filer/ticker/report/cusip
        best: Dict[Tuple[str,str,str,str], Dict[str, Any]] = {}
        for r in raw_rows_sorted:
            key = (r.get("filer_cik") or "", r.get("ticker") or "", r.get("report_date") or "", r.get("cusip") or "")
            cur = best.get(key)
            def rank(x):
                return _latest_rank(x.get("form",""), x.get("filing_date",""), x.get("accession","")) if x else (-1, dt.date(1900,1,1), "")
            if cur is None or rank(r) > rank(cur):
                best[key] = r
        raw_rows_deduped = list(best.values())
        raw_rows_deduped.sort(key=raw_sort_key)

        if not want_panel:
            out_path = os.path.join(out_dir, f"{group}_13f_holdings_raw.csv")
            write_csv(out_path, RAW_COLUMNS, raw_rows_deduped)
            if verbose:
                print(f"Wrote RAW: {out_path} (rows={len(raw_rows_deduped)})", flush=True)
        else:
            panel_rows = panel_build_max_across_filers(group, raw_rows_deduped)
            out_path = os.path.join(out_dir, f"{group}_13f_holdings_panel.csv")
            write_csv(out_path, PANEL_COLUMNS, panel_rows)
            panel_by_group[group] = panel_rows
            if verbose:
                print(f"Wrote PANEL: {out_path} (rows={len(panel_rows)})", flush=True)

    if want_panel:
        bvs_rows = build_bvs(panel_by_group)
        out_path = os.path.join(out_dir, "BVS_13f_holdings_panel.csv")
        write_csv(out_path, BVS_COLUMNS, bvs_rows)
        if verbose:
            print(f"Wrote BVS PANEL: {out_path} (rows={len(bvs_rows)})", flush=True)

if __name__ == "__main__":
    main()
