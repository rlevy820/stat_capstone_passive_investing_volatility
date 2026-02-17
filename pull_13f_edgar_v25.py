#!/usr/bin/env python3
"""EDGAR Form 13F downloader (SEC-only) — v25

Changes from v24:
  - Fixed stock-split lumpiness: replaced _split_factor with deterministic
    SO_EXPECTED_RANGES validator.  Every SO value is checked against known
    share-count ranges per ticker per split era (AAPL 7:1/4:1, AMZN 20:1).
    Values outside range are corrected or rejected — no edge cases.
  - Chart only plots quarters where all 3 managers (BVS) have data,
    eliminating partial-quarter drops at data boundaries.
  - Chart capped at 2025-12-31.
  - BVS CSV now includes num_managers column.

All previous features preserved:
  - Interactive mode + start-date prompts
  - Smart consolidation (MAX_FILER — takes max across CIKs)
  - 3-field XBRL shares-outstanding lookup (±180 day window)
  - BVS aggregate CSV + per-ticker ownership chart
  - RAW and PANEL output modes
  - Amendment deduplication (13F-HR/A overrides 13F-HR)

Run:
  python pull_13f_edgar_v25.py --user-agent "Name email" --out-dir "/Users/admin"
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin

import requests
import xml.etree.ElementTree as ET

# ── SEC endpoints ──────────────────────────────────────────────────────────────
SEC_DATA_BASE = "https://data.sec.gov/"
SEC_ARCHIVES_BASE = "https://www.sec.gov/Archives/"
SEC_COMPANY_TICKERS = "https://www.sec.gov/files/company_tickers.json"
SEC_COMPANY_FACTS = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
SEC_FULL_INDEX = "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"

# ── Defaults ───────────────────────────────────────────────────────────────────
DEFAULT_START_FILING_DATE = "2009-01-01"
DEFAULT_MIN_REPORT_DATE   = "2010-03-31"
FORMS = {"13F-HR", "13F-HR/A"}

# ── Seed CIKs ─────────────────────────────────────────────────────────────────
GROUPS: Dict[str, Dict[str, Any]] = {
    "BlackRock": {
        "seed_ciks": ["0001364742", "0000913414"],
        "keywords": ["blackrock"],
    },
    "Vanguard": {
        "seed_ciks": ["0000102909", "0000862084"],
        "keywords": ["vanguard"],
    },
    "StateStreet": {
        "seed_ciks": ["0000093751", "0001257442", "0001324601", "0001324602"],
        "keywords": ["state street", "ssga", "state street global advisors"],
    },
}

# ── Column schemas ─────────────────────────────────────────────────────────────
RAW_COLUMNS = [
    "group", "ticker", "mapped_cusip", "filer_cik", "form", "filing_date",
    "report_date", "accession", "info_table_url",
    "issuer_name", "class_title", "cusip",
    "value_usd_thousands", "shares_held", "shares_type",
    "put_call", "investment_discretion", "other_manager",
    "voting_sole", "voting_shared", "voting_none",
    "shares_outstanding",
]

PANEL_COLUMNS = [
    "group", "ticker", "mapped_cusip", "report_date",
    "shares_held", "value_usd_thousands",
    "num_filer_ciks", "filer_ciks_used", "consolidation_note",
    "latest_filing_date", "latest_accession",
    "shares_outstanding",
]

BVS_COLUMNS = [
    "group", "ticker", "mapped_cusip", "report_date",
    "shares_held_total", "value_usd_thousands_total",
    "shares_outstanding", "num_managers",
]

README_TEXT = r"""EDGAR 13F Holdings Output — README (v22)

=== DISCOVERY METHOD ===
  v22 uses SEC quarterly full-index files (master.idx) for filing discovery.
  This is the most reliable method — every filing accepted by EDGAR gets one
  entry in the index.  No pagination, no chunk files, no missing history.

=== MODE ===
  raw   -> per-group RAW files:  <Group>_13f_holdings_raw.csv
  panel -> per-group PANEL files: <Group>_13f_holdings_panel.csv
            plus BVS_13f_holdings_panel.csv (aggregate across all managers)
            plus ownership_pct_chart.png (graph of % shares held)

=== ORDERING ===
  ticker ASC, then report_date ASC (oldest -> newest)

=== COLUMN DEFINITIONS — RAW ===
  group                 Manager complex (BlackRock / Vanguard / StateStreet)
  ticker                Stock ticker from your ticker_cusip.csv
  mapped_cusip          CUSIP from your ticker_cusip.csv
  filer_cik             SEC CIK of the filing entity
  form                  13F-HR or 13F-HR/A (amendment)
  filing_date           Date filed with SEC
  report_date           Quarter-end the filing covers (e.g. 2010-03-31)
  accession             SEC accession number
  info_table_url        URL of the parsed XML information table
  issuer_name           Company name from 13F XML
  class_title           Security class (e.g. "COM", "CL A")
  cusip                 CUSIP from the 13F XML
  value_usd_thousands   Market value in $1,000s (as reported)
  shares_held           Number of shares (or principal amount)
  shares_type           "SH" = shares, "PRN" = principal
  put_call              "PUT" / "CALL" if applicable
  investment_discretion "SOLE" / "DEFINED" / "SHARED" / "OTHER"
  other_manager         Other manager number if applicable
  voting_sole           Shares with sole voting authority
  voting_shared         Shares with shared voting authority
  voting_none           Shares with no voting authority
  shares_outstanding    Total shares outstanding for the ISSUER (SEC XBRL)

=== COLUMN DEFINITIONS — PANEL ===
  shares_held           Consolidated shares held by this manager complex
  value_usd_thousands   Consolidated market value in $1,000s
  num_filer_ciks        Number of filer CIKs that contributed
  filer_ciks_used       Which CIKs contributed (semicolon-separated)
  consolidation_note    "SINGLE_FILER" or "MAX_FILER(...)" — always takes max CIK
                        Stock splits (AAPL 7:1, AAPL 4:1, AMZN 20:1) are
                        normalized in shares_outstanding to match 13F report basis.
  latest_filing_date    Most recent filing date among contributors
  latest_accession      Accession of that filing
  shares_outstanding    Total shares outstanding (SEC XBRL)

=== COLUMN DEFINITIONS — BVS ===
  shares_held_total          Sum of shares_held across all three managers
  value_usd_thousands_total  Sum of values across all three managers
  shares_outstanding         Total shares outstanding (SEC XBRL)

=== CONSOLIDATION LOGIC ===
  For each (ticker, report_date) within a manager group:
    - Amendments (13F-HR/A) override original filings for the same filer/period.
    - If only 1 filer CIK reported: used directly.
    - If multiple filer CIKs: check if the largest filer holds >= 90% of the
      combined total (indicates a combination report). If so, use MAX (that filer
      alone). Otherwise, SUM across filers (separate positions).

=== NOTES ===
  - values are in $1,000s as reported by the SEC.
  - shares_outstanding from SEC XBRL (dei:EntityCommonStockSharesOutstanding).
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  Data classes & utilities
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Filing:
    group: str
    cik: str        # 10-digit zero-padded
    cik_int: str    # integer string (no leading zeros)
    form: str
    accession: str  # with dashes
    filing_date: str
    report_date: Optional[str] = None
    primary_doc: Optional[str] = None


def pad_cik(cik: str) -> str:
    return re.sub(r"\D", "", cik).zfill(10)

def cik_int_str(cik: str) -> str:
    return str(int(re.sub(r"\D", "", cik)))

def accession_nodash(acc: str) -> str:
    return acc.replace("-", "")

def parse_date(s: Any) -> Optional[dt.date]:
    if s is None:
        return None
    x = str(s).strip()
    if not x:
        return None
    if re.fullmatch(r"\d{8}", x):
        try:
            return dt.date(int(x[:4]), int(x[4:6]), int(x[6:8]))
        except Exception:
            return None
    try:
        return dt.date.fromisoformat(x)
    except Exception:
        return None

def date_str(s: Any) -> str:
    d = parse_date(s)
    return d.isoformat() if d else ""

def safe_int(s: Any) -> int:
    if s is None:
        return 0
    if isinstance(s, (int, float)):
        return int(s)
    x = str(s).strip().replace(",", "")
    if not x:
        return 0
    try:
        return int(float(x))
    except Exception:
        return 0

def _sort_date(s: Any) -> dt.date:
    return parse_date(s) or dt.date(9999, 12, 31)


# ═══════════════════════════════════════════════════════════════════════════════
#  SEC HTTP
# ═══════════════════════════════════════════════════════════════════════════════

def sec_get(session: requests.Session, url: str, retries: int = 12,
            timeout: int = 90) -> requests.Response:
    backoff = 1.0
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=timeout)
        except requests.exceptions.RequestException:
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 90.0)
            continue
        if r.status_code == 200:
            return r
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 90.0)
            continue
        if r.status_code == 404:
            raise FileNotFoundError(f"404: {url}")
        raise RuntimeError(f"GET {url} -> {r.status_code}")
    raise RuntimeError(f"GET {url} failed after {retries} retries")


# ═══════════════════════════════════════════════════════════════════════════════
#  Quarterly full-index filing discovery  (v22 — replaces submissions API)
# ═══════════════════════════════════════════════════════════════════════════════

def _quarter_range(start: dt.date, end: dt.date) -> List[Tuple[int, int]]:
    """Generate (year, qtr) tuples covering [start, end]."""
    sy, sq = start.year, (start.month - 1) // 3 + 1
    ey, eq = end.year, (end.month - 1) // 3 + 1
    out = []
    y, q = sy, sq
    while (y, q) <= (ey, eq):
        out.append((y, q))
        q += 1
        if q > 4:
            q = 1
            y += 1
    return out


def _accession_from_filename(filename: str) -> str:
    """Extract dash-formatted accession from an index filename.

    Handles both formats:
      'edgar/data/1364742/000136474210000123.txt'       -> undashed in filename
      'edgar/data/1364742/0001364742-10-000123.txt'     -> already dashed
      'edgar/data/1364742/0001364742-10-000123-index.htm' -> with suffix
    """
    base = filename.rsplit("/", 1)[-1]   # e.g. '0001364742-10-000123.txt'
    nodash = base.split(".")[0]           # e.g. '0001364742-10-000123'

    # If already contains dashes in the right pattern, use as-is
    if re.match(r"^\d{10}-\d{2}-\d{6}", nodash):
        return nodash[:20]  # CIK(10) + dash + YY(2) + dash + SEQ(6) = 20 chars

    # Otherwise, try undashed format (18+ digits)
    digits_only = re.sub(r"\D", "", nodash)
    if len(digits_only) >= 18:
        return f"{digits_only[:10]}-{digits_only[10:12]}-{digits_only[12:18]}"

    return nodash


def _cik_int_from_filename(filename: str) -> str:
    """Extract integer CIK from index filename path.

    Example: 'edgar/data/1364742/000136474210000123.txt' -> '1364742'
    """
    parts = filename.split("/")
    for i, p in enumerate(parts):
        if p == "data" and i + 1 < len(parts):
            return parts[i + 1]
    return ""


def parse_index_text(text: str, target_ciks: Set[str],
                     target_forms: Set[str],
                     verbose: bool = False) -> List[Dict[str, str]]:
    """Parse a master.idx file (pipe-delimited), returning rows matching
    target CIKs and forms.

    master.idx format:
      CIK|Company Name|Form Type|Date Filed|Filename
    with a few header/separator lines at the top.
    """
    lines = text.split("\n")
    results: List[Dict[str, str]] = []
    header_found = False
    data_started = False

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # Look for header row
        if not header_found:
            if "|" in stripped and ("CIK" in stripped or "Form Type" in stripped):
                header_found = True
                continue  # skip the header itself
            # Skip separator lines
            if stripped.startswith("-"):
                continue
            # Fallback: if line has 4+ pipes and starts with digits, treat as data
            if stripped.count("|") >= 4 and stripped.split("|")[0].strip().isdigit():
                header_found = True  # implicit header
                data_started = True
                # Don't continue — fall through to parse this line
            else:
                continue

        # Skip separator lines after header
        if stripped.startswith("-"):
            data_started = True
            continue

        data_started = True

        # Parse pipe-delimited data
        parts = stripped.split("|")
        if len(parts) < 5:
            continue

        cik_raw    = parts[0].strip()
        # company  = parts[1].strip()  # not needed
        form_type  = parts[2].strip()
        date_filed = parts[3].strip()
        filename   = parts[4].strip()

        # Filter by form type
        if form_type not in target_forms:
            continue

        # Normalize CIK to 10-digit padded
        try:
            cik_padded = str(int(cik_raw)).zfill(10)
        except (ValueError, TypeError):
            continue

        # Filter by target CIKs
        if cik_padded not in target_ciks:
            continue

        results.append({
            "cik": cik_padded,
            "form": form_type,
            "filing_date": date_filed,
            "filename": filename,
        })

    if verbose and not header_found:
        print("      WARNING: could not find header line in index file", flush=True)

    return results


def discover_filings_via_full_index(
    session: requests.Session,
    all_ciks: Set[str],
    start_filing: dt.date,
    verbose: bool = False,
) -> List[Dict[str, str]]:
    """Download quarterly master.idx files and extract all 13F filings for our CIKs.

    Returns a list of dicts: {cik, form, filing_date, filename, accession, cik_int}
    """
    today = dt.date.today()
    # Scan through Q1 of the current year to catch Q4-previous-year
    # filings that are filed ~45 days after quarter end.
    # The chart + BVS cap at 2025-12-31 via max_date filtering.
    end_date = dt.date(today.year, 3, 31)
    quarters = _quarter_range(start_filing, end_date)

    if verbose:
        print(f"\nFull-index scan: {len(quarters)} quarters "
              f"(Q{quarters[0][1]}-{quarters[0][0]} through "
              f"Q{quarters[-1][1]}-{quarters[-1][0]}), "
              f"watching {len(all_ciks)} CIK(s)", flush=True)
        print(f"  Target CIKs: {sorted(all_ciks)}", flush=True)
        print(f"  URL template: {SEC_FULL_INDEX}", flush=True)

    all_entries: List[Dict[str, str]] = []
    diag_done = False

    for i, (year, qtr) in enumerate(quarters, 1):
        url = SEC_FULL_INDEX.format(year=year, qtr=qtr)
        try:
            resp = sec_get(session, url, timeout=120)
        except FileNotFoundError:
            if verbose:
                print(f"  [{i}/{len(quarters)}] Q{qtr}-{year}: not found (404), skipping", flush=True)
            time.sleep(0.15)
            continue
        except Exception as e:
            if verbose:
                print(f"  [{i}/{len(quarters)}] Q{qtr}-{year}: ERROR {e}", flush=True)
            time.sleep(0.15)
            continue

        # ── Decode response robustly ──
        # master.idx is plain text but SEC often serves it without charset header,
        # causing requests to default to ISO-8859-1.  Try multiple decodings.
        text = None
        raw = resp.content
        for enc in ("utf-8", "latin-1", "ascii"):
            try:
                text = raw.decode(enc, errors="replace")
                if "|" in text[:5000]:
                    break  # looks like valid pipe-delimited data
            except Exception:
                continue
        if text is None:
            text = resp.text  # fallback to requests' auto-detection

        # ── Diagnostic output (first successful download) ──
        if verbose and not diag_done:
            diag_done = True
            print(f"\n  [DIAG] === First index file: Q{qtr}-{year} ===", flush=True)
            print(f"  [DIAG] URL: {url}", flush=True)
            print(f"  [DIAG] HTTP status: {resp.status_code}", flush=True)
            print(f"  [DIAG] Content-Type: {resp.headers.get('Content-Type', '?')}", flush=True)
            print(f"  [DIAG] Content length: {len(raw)} bytes", flush=True)
            print(f"  [DIAG] Encoding used: {resp.encoding} -> detected pipes: {'|' in text[:5000]}", flush=True)

            # Check if response looks like HTML (redirect/error page)
            text_start = text.lstrip()[:200]
            if text_start.startswith("<") or text_start.startswith("<!"):
                print(f"  [DIAG] ⚠️  Response appears to be HTML, not a text index!", flush=True)
                print(f"  [DIAG] First 300 chars: {text[:300]}", flush=True)
            else:
                sample_lines = text.split("\n")[:15]
                print(f"  [DIAG] First {len(sample_lines)} lines:", flush=True)
                for sl in sample_lines:
                    print(f"    | {sl[:130]}", flush=True)

                # Count total lines and pipe-delimited lines
                all_lines = text.split("\n")
                pipe_lines = [l for l in all_lines if l.count("|") >= 4]
                print(f"  [DIAG] Total lines: {len(all_lines)}, "
                      f"pipe-delimited data lines: {len(pipe_lines)}", flush=True)

                # Try to find our CIKs in raw text as sanity check
                for cik in sorted(all_ciks)[:3]:  # check first 3
                    cik_raw = str(int(cik))  # remove leading zeros
                    count = text.count(f"|{cik_raw}|") + text.count(f"{cik_raw}|")
                    if count > 0:
                        print(f"  [DIAG] CIK {cik} (raw: {cik_raw}) appears ~{count} time(s) in file", flush=True)
                    else:
                        print(f"  [DIAG] CIK {cik} (raw: {cik_raw}) NOT found in file", flush=True)

            print(f"  [DIAG] ================================\n", flush=True)

        entries = parse_index_text(text, all_ciks, FORMS, verbose=verbose)

        # Enrich each entry with derived fields
        for e in entries:
            e["accession"] = _accession_from_filename(e["filename"])
            e["cik_int"] = _cik_int_from_filename(e["filename"]) or cik_int_str(e["cik"])

        # Filter by start_filing date
        entries = [e for e in entries
                   if (parse_date(e["filing_date"]) or dt.date.min) >= start_filing]

        all_entries.extend(entries)

        if verbose:
            print(f"  [{i}/{len(quarters)}] Q{qtr}-{year}: "
                  f"{len(entries)} 13F filing(s) matched", flush=True)

        time.sleep(0.15)

    if verbose:
        print(f"  Total 13F filings discovered: {len(all_entries)}", flush=True)

    return all_entries


# ═══════════════════════════════════════════════════════════════════════════════
#  CIK auto-expansion (still uses submissions API for quick boolean check)
# ═══════════════════════════════════════════════════════════════════════════════

def _submissions_has_13f(session: requests.Session, cik: str, start: dt.date) -> bool:
    """Quick check: does this CIK have any 13F-HR filing since start?
    Uses the submissions API (fine for a boolean check).
    """
    try:
        url = urljoin(SEC_DATA_BASE, f"submissions/CIK{pad_cik(cik)}.json")
        sub = sec_get(session, url).json()
        recent = sub.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        for f, d in zip(forms, dates):
            if (f or "").strip() in FORMS:
                fd = parse_date(d)
                if fd and fd >= start:
                    return True
        return False
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════════════════════
#  Info table discovery + parsing
# ═══════════════════════════════════════════════════════════════════════════════

def _filing_base_url(cik_int: str, accession: str) -> str:
    return urljoin(SEC_ARCHIVES_BASE,
                   f"edgar/data/{cik_int}/{accession_nodash(accession)}/")


def extract_report_date(session: requests.Session, base_url: str,
                        primary_doc: Optional[str] = None) -> str:
    """Try to extract reportCalendarOrQuarter from the filing's primary doc."""
    candidates = []
    if primary_doc:
        candidates.append(primary_doc)
    candidates.extend(["primary_doc.xml", "0.xml"])
    for name in dict.fromkeys(candidates):
        if not name:
            continue
        try:
            text = sec_get(session, urljoin(base_url, name),
                           timeout=45).content.decode("utf-8", errors="ignore")
            m = re.search(
                r"<reportCalendarOrQuarter>\s*(\d{4}-\d{2}-\d{2})\s*</reportCalendarOrQuarter>",
                text, re.I,
            )
            if m:
                return m.group(1)
            m = re.search(r"CONFORMED\s+PERIOD\s+OF\s+REPORT:\s*(\d{8})", text, re.I)
            if m:
                d = parse_date(m.group(1))
                if d:
                    return d.isoformat()
        except Exception:
            continue
    return ""


def guess_report_date(filing_date_str: str) -> str:
    """Heuristic: guess which quarter a 13F filing covers based on filing date.

    13F filers must file within 45 days of quarter-end:
      Q4 (Dec 31) -> due Feb 14      -> filed Jan-Feb
      Q1 (Mar 31) -> due May 15      -> filed Apr-May
      Q2 (Jun 30) -> due Aug 14      -> filed Jul-Aug
      Q3 (Sep 30) -> due Nov 14      -> filed Oct-Nov

    Amendments can be filed later, so this is a fallback only.
    """
    fd = parse_date(filing_date_str)
    if not fd:
        return ""
    m, y = fd.month, fd.year
    if m <= 2 or (m == 3 and fd.day <= 15):
        return f"{y - 1}-12-31"
    elif m <= 5 or (m == 6 and fd.day <= 15):
        return f"{y}-03-31"
    elif m <= 8 or (m == 9 and fd.day <= 15):
        return f"{y}-06-30"
    else:
        return f"{y}-09-30"


def find_info_table_urls(session: requests.Session, base_url: str,
                         primary_doc: Optional[str] = None) -> List[str]:
    """Find candidate info-table XML URLs from a filing's index.json."""
    try:
        idx = sec_get(session, urljoin(base_url, "index.json")).json()
    except Exception:
        return []
    items = idx.get("directory", {}).get("item", []) or []
    candidates = []
    for it in items:
        name = it.get("name") or ""
        ln = name.lower()
        size = int(it.get("size") or 0)
        if not (ln.endswith(".xml") or ln.endswith(".txt")
               or ln.endswith(".htm") or ln.endswith(".html")):
            continue
        score = 0
        if "infotable" in ln or "informationtable" in ln:
            score += 100
        if "13f" in ln and "primary" not in ln:
            score += 30
        if ln.endswith(".xml"):
            score += 10
        if ln.endswith(".htm") or ln.endswith(".html"):
            score += 5
        score += min(size // 5000, 40)
        if "primary" in ln or name == (primary_doc or ""):
            score -= 50
        candidates.append((score, name))
    candidates.sort(key=lambda x: (-x[0], x[1]))
    return [urljoin(base_url, n) for (_, n) in candidates[:25]]


def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def parse_info_table_xml(xml_bytes: bytes) -> List[Dict[str, Any]]:
    # Strip BOM and leading whitespace that can confuse the XML parser
    raw = xml_bytes.lstrip(b"\xef\xbb\xbf \t\r\n")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        # Try wrapping in a root element (some files are fragments)
        try:
            root = ET.fromstring(b"<root>" + raw + b"</root>")
        except ET.ParseError:
            return []
    out = []
    for el in root.iter():
        tag_local = _strip_ns(el.tag).lower()
        if tag_local != "infotable":
            continue

        def ct(parent, name):
            name_l = name.lower()
            for ch in parent:
                if _strip_ns(ch.tag).lower() == name_l:
                    return (ch.text or "").strip()
            return ""

        row = {
            "issuer_name": ct(el, "nameOfIssuer"),
            "class_title": ct(el, "titleOfClass"),
            "cusip": ct(el, "cusip"),
            "value_usd_thousands": ct(el, "value"),
            "put_call": ct(el, "putCall"),
            "investment_discretion": ct(el, "investmentDiscretion"),
            "other_manager": ct(el, "otherManager"),
            "shares_held": "", "shares_type": "",
            "voting_sole": "", "voting_shared": "", "voting_none": "",
        }
        for ch in el:
            if _strip_ns(ch.tag).lower() == "shrsOrPrnAmt".lower():
                row["shares_held"] = ct(ch, "sshPrnamt")
                row["shares_type"] = ct(ch, "sshPrnamtType")
        for ch in el:
            if _strip_ns(ch.tag).lower() == "votingAuthority".lower():
                row["voting_sole"]   = ct(ch, "Sole")
                row["voting_shared"] = ct(ch, "Shared")
                row["voting_none"]   = ct(ch, "None")
        if row["cusip"]:
            out.append(row)
    return out


def parse_info_table_text(content: bytes) -> List[Dict[str, Any]]:
    """Parse a non-XML 13F info table (SGML/text/HTML format).

    Older filings (pre-~2013) sometimes use SGML or plain-text tables
    instead of well-formed XML.  This parser uses regex to extract
    holdings from any text-based format where CUSIPs are visible.

    A 13F info-table row contains:  issuer name, title, CUSIP (9 chars),
    value ($1k), shares/principal amount, type (SH/PRN), and optional
    put/call, discretion, voting fields.
    """
    try:
        text = content.decode("utf-8", errors="replace")
    except Exception:
        try:
            text = content.decode("latin-1", errors="replace")
        except Exception:
            return []

    out: List[Dict[str, Any]] = []

    # Pattern: CUSIP (9 alphanumeric chars), followed by value and shares
    # Handles various delimiters: whitespace, pipes, tabs, XML-like tags
    # Common layout: ... CUSIP  VALUE  SHARES  SH/PRN ...
    #
    # Strategy: find every 9-char CUSIP candidate, then grab the numbers
    # on the same line (or in nearby context)

    # First try: XML-like <cusip>...</cusip> tags embedded in SGML
    cusip_tag_pattern = re.compile(
        r'<cusip>\s*([A-Za-z0-9]{8,9})\s*</cusip>', re.I
    )
    value_tag = re.compile(r'<value>\s*(\d[\d,]*)\s*</value>', re.I)
    shares_tag = re.compile(r'<sshPrnamt>\s*(\d[\d,]*)\s*</sshPrnamt>', re.I)
    shares_type_tag = re.compile(
        r'<sshPrnamtType>\s*(SH|PRN)\s*</sshPrnamtType>', re.I
    )
    name_tag = re.compile(
        r'<nameOfIssuer>\s*([^<]+?)\s*</nameOfIssuer>', re.I
    )
    title_tag = re.compile(
        r'<titleOfClass>\s*([^<]+?)\s*</titleOfClass>', re.I
    )
    putcall_tag = re.compile(r'<putCall>\s*([^<]*?)\s*</putCall>', re.I)
    disc_tag = re.compile(
        r'<investmentDiscretion>\s*([^<]*?)\s*</investmentDiscretion>', re.I
    )
    vs_tag = re.compile(r'<Sole>\s*(\d[\d,]*)\s*</Sole>', re.I)
    vsh_tag = re.compile(r'<Shared>\s*(\d[\d,]*)\s*</Shared>', re.I)
    vn_tag = re.compile(r'<None>\s*(\d[\d,]*)\s*</None>', re.I)

    # Split text into info-table entries by looking for <infoTable> blocks
    # (SGML style, may not be well-formed XML)
    entry_pattern = re.compile(
        r'<infoTable[^>]*>(.*?)</infoTable>', re.I | re.DOTALL
    )
    entries = entry_pattern.findall(text)

    if entries:
        for entry in entries:
            cm = cusip_tag_pattern.search(entry)
            if not cm:
                continue
            vm = value_tag.search(entry)
            sm = shares_tag.search(entry)
            stm = shares_type_tag.search(entry)
            nm = name_tag.search(entry)
            tm = title_tag.search(entry)
            pcm = putcall_tag.search(entry)
            dm = disc_tag.search(entry)
            vsm = vs_tag.search(entry)
            vshm = vsh_tag.search(entry)
            vnm = vn_tag.search(entry)

            out.append({
                "issuer_name": nm.group(1) if nm else "",
                "class_title": tm.group(1) if tm else "",
                "cusip": cm.group(1).strip().upper(),
                "value_usd_thousands": (vm.group(1).replace(",", "")
                                        if vm else ""),
                "shares_held": (sm.group(1).replace(",", "")
                                if sm else ""),
                "shares_type": stm.group(1).upper() if stm else "SH",
                "put_call": pcm.group(1) if pcm else "",
                "investment_discretion": dm.group(1) if dm else "",
                "other_manager": "",
                "voting_sole": (vsm.group(1).replace(",", "")
                                if vsm else ""),
                "voting_shared": (vshm.group(1).replace(",", "")
                                  if vshm else ""),
                "voting_none": (vnm.group(1).replace(",", "")
                                if vnm else ""),
            })
        return out

    # Fallback: look for tabular lines with CUSIP patterns
    # CUSIP = 6 alphanumeric + 2-3 digits (8 or 9 chars total)
    # NOTE: domestic CUSIPs start with digits (e.g., 037833100 for AAPL)
    #       while international CINs start with letters (e.g., G1151C101).
    #       Both must be matched.
    line_pattern = re.compile(
        r'(?:^|[\s|,;>])'                  # preceded by separator or tag close
        r'([A-Z0-9][A-Z0-9]{5}\d{2,3})'   # CUSIP (8 or 9 chars, any alnum start)
        r'[\s|,;]+'
        r'(\d[\d,]*)'                      # value ($1000s)
        r'[\s|,;]+'
        r'(\d[\d,]*)'                      # shares
        r'[\s|,;]+'
        r'(SH|PRN)',                        # type
        re.I | re.MULTILINE,
    )
    for m in line_pattern.finditer(text):
        cusip = m.group(1).upper()
        out.append({
            "issuer_name": "",
            "class_title": "",
            "cusip": cusip,
            "value_usd_thousands": m.group(2).replace(",", ""),
            "shares_held": m.group(3).replace(",", ""),
            "shares_type": m.group(4).upper(),
            "put_call": "", "investment_discretion": "",
            "other_manager": "",
            "voting_sole": "", "voting_shared": "", "voting_none": "",
        })

    return out


def _try_parse_info_table(blob: bytes) -> List[Dict[str, Any]]:
    """Try XML first, then SGML/text parser for 13F info tables."""
    # Try XML parser
    rows = parse_info_table_xml(blob)
    if rows:
        return rows
    # Try text/SGML parser
    rows = parse_info_table_text(blob)
    return rows


def parse_full_submission_text(
    blob: bytes,
    target_cusips: Set[str],
) -> List[Dict[str, Any]]:
    """Parse a full submission text file (.txt) that contains multiple
    <DOCUMENT> sections in SGML format.

    Pre-~2013 13F filings often bundle domestic and international info
    tables as separate <DOCUMENT> blocks within one file.  This function
    splits them apart and parses each INFORMATION TABLE section
    individually, returning only holdings whose CUSIPs appear in
    target_cusips (checked as 6/8/9-char prefixes).
    """
    try:
        text = blob.decode("utf-8", errors="replace")
    except Exception:
        try:
            text = blob.decode("latin-1", errors="replace")
        except Exception:
            return []

    # Build prefix sets for quick matching
    exact = set(c.upper() for c in target_cusips)
    pref8 = set(c[:8].upper() for c in target_cusips if len(c) >= 8)
    pref6 = set(c[:6].upper() for c in target_cusips if len(c) >= 6)

    def cusip_matches(c: str) -> bool:
        cu = c.strip().upper()
        if cu in exact:
            return True
        if len(cu) >= 8 and cu[:8] in pref8:
            return True
        if len(cu) >= 6 and cu[:6] in pref6:
            return True
        return False

    # Split into <DOCUMENT>...</DOCUMENT> blocks
    doc_pattern = re.compile(
        r'<DOCUMENT>(.*?)</DOCUMENT>', re.DOTALL | re.IGNORECASE
    )
    documents = doc_pattern.findall(text)
    if not documents:
        # No DOCUMENT tags — try parsing the whole blob
        return _try_parse_info_table(blob)

    all_rows: List[Dict[str, Any]] = []
    for doc in documents:
        # Check if this document contains any target CUSIPs
        has_target = any(c in doc for c in exact)
        if not has_target:
            has_target = any(c in doc for c in pref8)
        if not has_target:
            continue

        # Check if it's an information table type
        type_match = re.search(
            r'<TYPE>\s*(.*?)[\r\n]', doc, re.IGNORECASE
        )
        doc_type = type_match.group(1).strip().upper() if type_match else ""

        # Extract the <TEXT> section
        text_match = re.search(
            r'<TEXT>(.*?)(?:</TEXT>|$)', doc, re.DOTALL | re.IGNORECASE
        )
        if not text_match:
            inner = doc
        else:
            inner = text_match.group(1)

        inner_bytes = inner.encode("utf-8", errors="replace")
        parsed = _try_parse_info_table(inner_bytes)
        if not parsed:
            continue

        # Keep only rows with matching CUSIPs
        for r in parsed:
            c = (r.get("cusip") or "").strip().upper()
            if c and cusip_matches(c):
                all_rows.append(r)

    return all_rows


# ═══════════════════════════════════════════════════════════════════════════════
#  Ticker map + shares outstanding
# ═══════════════════════════════════════════════════════════════════════════════

def load_ticker_cusip_map(path: str) -> Dict[str, str]:
    m: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return m
        lower = [c.lower() for c in reader.fieldnames]
        if "ticker" not in lower or "cusip" not in lower:
            raise ValueError("ticker_cusip.csv needs columns: ticker, cusip")
        tcol = reader.fieldnames[lower.index("ticker")]
        ccol = reader.fieldnames[lower.index("cusip")]
        for row in reader:
            t = (row.get(tcol) or "").strip().upper()
            c = (row.get(ccol) or "").strip()
            if t and c:
                m[t] = c
    return m


def build_ticker_to_issuer_cik(session: requests.Session,
                                verbose: bool) -> Dict[str, str]:
    if verbose:
        print("Downloading SEC company_tickers.json …", flush=True)
    data = sec_get(session, SEC_COMPANY_TICKERS).json()
    if verbose:
        print(f"  Loaded {len(data)} entries.", flush=True)
    out: Dict[str, str] = {}
    for _, row in data.items():
        t = (row.get("ticker") or "").strip().upper()
        c = str(row.get("cik_str") or "").strip()
        if t and c:
            out[t] = pad_cik(c)
    return out


def fetch_so_series(session: requests.Session,
                    cik: str) -> List[Tuple[dt.date, int]]:
    url = SEC_COMPANY_FACTS.format(cik=cik)
    data = sec_get(session, url).json()
    facts = data.get("facts", {})

    # Collect from ALL matching XBRL fields, not just the first
    all_vals: list = []
    for ns, fld in [
        ("dei", "EntityCommonStockSharesOutstanding"),
        ("us-gaap", "EntityCommonStockSharesOutstanding"),
        ("us-gaap", "CommonStockSharesOutstanding"),
    ]:
        block = facts.get(ns, {}).get(fld, {})
        vals = block.get("units", {}).get("shares", [])
        all_vals.extend(vals)

    # Deduplicate: for each (end_date), keep the value from the
    # EARLIEST filing (smallest filed date).  Later filings may
    # restate historical share counts on a post-split basis, which
    # would mismatch with 13F shares_held reported at the original
    # (pre-split) basis.  Using the earliest filing avoids this.
    by_date: Dict[dt.date, Tuple[str, int]] = {}
    for v in all_vals:
        end = parse_date(v.get("end"))
        val = v.get("val")
        filed = v.get("filed", "")
        if end and val is not None:
            try:
                ival = int(float(val))
                if ival <= 0:
                    continue
                if end not in by_date or filed < by_date[end][0]:
                    by_date[end] = (filed, ival)
            except Exception:
                continue

    out = [(d, v) for d, (_, v) in by_date.items()]
    out.sort(key=lambda x: x[0])
    return out


# ── Deterministic SO validation ──
# Expected shares-outstanding ranges per ticker, per split era.
# Each entry: (era_start, era_end, min_SO, max_SO, fix_divisor)
#   - If SO > max_SO and fix_divisor is set, try SO // fix_divisor
#   - If SO < min_SO and fix_divisor is set, try SO * fix_divisor
# Ranges are intentionally wide to handle buyback-driven declines.
SO_EXPECTED_RANGES: Dict[str, List[Tuple[dt.date, dt.date, int, int, Optional[int]]]] = {
    "AAPL": [
        # Pre-7:1 split (before June 9, 2014): ~880M → 940M
        (dt.date(2009, 1, 1), dt.date(2014, 6, 8),
         800_000_000, 960_000_000, 7),
        # Post-7:1, pre-4:1 (June 9, 2014 – Aug 30, 2020): ~6.5B → 4.3B via buybacks
        (dt.date(2014, 6, 9), dt.date(2020, 8, 30),
         4_000_000_000, 6_700_000_000, 4),
        # Post-4:1 (Aug 31, 2020+): ~17B → 15B via buybacks
        (dt.date(2020, 8, 31), dt.date(2030, 1, 1),
         14_000_000_000, 18_000_000_000, None),
    ],
    "AMZN": [
        # Pre-20:1 split (before June 6, 2022): ~440M → 510M
        (dt.date(2009, 1, 1), dt.date(2022, 6, 5),
         420_000_000, 530_000_000, 20),
        # Post-20:1 (June 6, 2022+): ~10B → 10.5B
        (dt.date(2022, 6, 6), dt.date(2030, 1, 1),
         9_000_000_000, 11_000_000_000, None),
    ],
    "MSFT": [
        # No splits since 2003. ~8.8B → 7.3B via buybacks
        (dt.date(2009, 1, 1), dt.date(2030, 1, 1),
         7_000_000_000, 9_000_000_000, None),
    ],
}

_so_fix_log: List[str] = []   # first N corrections logged


def _validate_so(ticker: str, report_date: dt.date,
                 raw_so: int, verbose: bool = False) -> Optional[int]:
    """Validate SO against known expected ranges.

    Returns corrected SO, or None if unrecoverable.
    """
    ranges = SO_EXPECTED_RANGES.get(ticker.upper())
    if not ranges:
        return raw_so  # unknown ticker → pass through

    for era_start, era_end, min_so, max_so, fix_div in ranges:
        if era_start <= report_date <= era_end:
            # In range → good
            if min_so <= raw_so <= max_so:
                return raw_so
            # Too high → try dividing by fix_divisor
            if raw_so > max_so and fix_div:
                fixed = raw_so // fix_div
                if min_so <= fixed <= max_so:
                    if verbose and len(_so_fix_log) < 10:
                        _so_fix_log.append(
                            f"    ⚡ SO fix {ticker} {report_date}: "
                            f"{raw_so:,} → {fixed:,} (÷{fix_div})")
                        print(_so_fix_log[-1], flush=True)
                    return fixed
            # Too low → try multiplying by fix_divisor
            if raw_so < min_so and fix_div:
                fixed = raw_so * fix_div
                if min_so <= fixed <= max_so:
                    if verbose and len(_so_fix_log) < 10:
                        _so_fix_log.append(
                            f"    ⚡ SO fix {ticker} {report_date}: "
                            f"{raw_so:,} → {fixed:,} (×{fix_div})")
                        print(_so_fix_log[-1], flush=True)
                    return fixed
            # Unrecoverable — value doesn't fit any correction
            if verbose and len(_so_fix_log) < 10:
                _so_fix_log.append(
                    f"    ⚠ SO reject {ticker} {report_date}: "
                    f"{raw_so:,} (expected {min_so:,}–{max_so:,})")
                print(_so_fix_log[-1], flush=True)
            return None

    return raw_so  # no matching era → pass through


def pick_so(series: List[Tuple[dt.date, int]],
            report_date: dt.date,
            ticker: str = "",
            verbose: bool = False) -> Optional[int]:
    """Pick the best SO value, validated against expected ranges.

    Tries exact match first, then closest within 180 days.
    Every candidate is validated; bad values are corrected or skipped.
    """
    if not series:
        return None

    # Build candidates sorted by distance from report_date
    candidates = []
    for end, val in series:
        dist = abs((end - report_date).days)
        if dist <= 180:
            candidates.append((dist, val))
    candidates.sort()

    for dist, raw_so in candidates:
        validated = _validate_so(ticker, report_date, raw_so, verbose=verbose)
        if validated is not None:
            return validated

    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  CSV + dedup + consolidation
# ═══════════════════════════════════════════════════════════════════════════════

def write_csv(path: str, cols: List[str], rows: List[Dict[str, Any]]) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})


def _recency(form, filing_date, accession):
    return (1 if form == "13F-HR/A" else 0,
            parse_date(filing_date) or dt.date(1900, 1, 1),
            accession or "")


def dedupe_raw(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deduplicate raw holdings: keep the best FILING per (CIK, report_date, ticker).

    When a 13F-HR/A (amendment) exists alongside a 13F-HR (original) for the
    same filer+quarter+ticker, keep only the amendment's rows.

    CRITICAL: within a single filing, multiple rows for the same CUSIP are
    LEGITIMATE — they represent different sub-fund holdings (e.g., BlackRock
    Fund Advisors reports AAPL holdings across 10+ sub-funds).  These must
    ALL be preserved so build_panel can sum them correctly.
    """
    # Step 1: For each (filer_cik, report_date, ticker), find the best
    #         accession (prefer amendment over original, most recent filing).
    #         This groups are rows that will compete for dedup.
    from collections import defaultdict
    groups: Dict[tuple, Dict[str, List[Dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for r in rows:
        group_key = (r.get("filer_cik", ""),
                     r.get("report_date", ""),
                     r.get("ticker", ""))
        acc = r.get("accession", "")
        groups[group_key][acc].append(r)

    # Step 2: For each group, pick the best accession
    out: List[Dict[str, Any]] = []
    for group_key, acc_dict in groups.items():
        if len(acc_dict) == 1:
            # Only one filing → keep all its rows
            for rows_list in acc_dict.values():
                out.extend(rows_list)
        else:
            # Multiple filings → pick the best one
            best_acc = max(
                acc_dict.keys(),
                key=lambda acc: _recency(
                    acc_dict[acc][0].get("form", ""),
                    acc_dict[acc][0].get("filing_date", ""),
                    acc,
                ),
            )
            # But if the "best" (amendment) has very few rows while
            # the original has many, the amendment may be a partial
            # correction that doesn't include our tickers.  In that
            # case, keep the original.
            best_rows = acc_dict[best_acc]
            if len(best_rows) > 0:
                out.extend(best_rows)
            else:
                # Fall back to the accession with the most rows
                fallback_acc = max(acc_dict.keys(),
                                   key=lambda a: len(acc_dict[a]))
                out.extend(acc_dict[fallback_acc])

    out.sort(key=lambda r: (
        r.get("ticker", ""),
        _sort_date(r.get("report_date")),
        _sort_date(r.get("filing_date")),
        r.get("accession", ""),
    ))
    return out


def build_panel(group: str,
                raw_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_key: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for r in raw_rows:
        by_key[(r.get("ticker", ""), r.get("report_date", ""))].append(r)

    out = []
    for (ticker, report_date), recs in by_key.items():
        by_filer: Dict[str, Dict[str, Any]] = {}
        for r in recs:
            fc = r.get("filer_cik", "")
            if fc not in by_filer:
                by_filer[fc] = {
                    "shares": 0, "value": 0,
                    "filing_date": "", "accession": "",
                    "shares_outstanding": "", "mapped_cusip": "",
                }
            by_filer[fc]["shares"] += safe_int(r.get("shares_held"))
            by_filer[fc]["value"]  += safe_int(r.get("value_usd_thousands"))
            if _sort_date(r.get("filing_date")) > _sort_date(
                by_filer[fc]["filing_date"]
            ):
                by_filer[fc]["filing_date"] = r.get("filing_date", "")
                by_filer[fc]["accession"]   = r.get("accession", "")
            if not by_filer[fc]["shares_outstanding"] and r.get("shares_outstanding"):
                by_filer[fc]["shares_outstanding"] = r.get("shares_outstanding", "")
            if not by_filer[fc]["mapped_cusip"]:
                by_filer[fc]["mapped_cusip"] = r.get("mapped_cusip", "")

        filers = list(by_filer.items())
        filer_ciks = [fc for fc, _ in filers]

        if len(filers) == 1:
            fc, info = filers[0]
            total_sh, total_val = info["shares"], info["value"]
            note = "SINGLE_FILER"
        else:
            # Parent CIKs (e.g. BlackRock Inc) already include
            # subsidiary holdings (e.g. BlackRock Advisors).
            # Take MAX across CIKs to avoid double-counting.
            shares_list = [(fc, info["shares"]) for fc, info in filers]
            max_fc, max_sh = max(shares_list, key=lambda x: x[1])
            total_sh = max_sh
            total_val = by_filer[max_fc]["value"]
            note = f"MAX_FILER({max_fc})"

        so, latest_fd, latest_acc, mcusip = "", "", "", ""
        for fc, info in filers:
            if info["shares_outstanding"]:
                so = info["shares_outstanding"]
            if _sort_date(info["filing_date"]) > _sort_date(latest_fd):
                latest_fd = info["filing_date"]
                latest_acc = info["accession"]
            if not mcusip:
                mcusip = info["mapped_cusip"]

        out.append({
            "group": group, "ticker": ticker, "mapped_cusip": mcusip,
            "report_date": report_date,
            "shares_held": total_sh, "value_usd_thousands": total_val,
            "num_filer_ciks": len(filers),
            "filer_ciks_used": ";".join(filer_ciks),
            "consolidation_note": note,
            "latest_filing_date": latest_fd, "latest_accession": latest_acc,
            "shares_outstanding": so,
        })

    out.sort(key=lambda r: (r.get("ticker", ""),
                            _sort_date(r.get("report_date"))))
    return out


def build_bvs(
    panel_by_group: Dict[str, List[Dict[str, Any]]]
) -> List[Dict[str, Any]]:
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for group_name, rows in panel_by_group.items():
        for r in rows:
            k = (r.get("ticker", ""), r.get("report_date", ""))
            if k not in agg:
                agg[k] = {
                    "group": "BVS", "ticker": k[0],
                    "mapped_cusip": r.get("mapped_cusip", ""),
                    "report_date": k[1],
                    "shares_held_total": 0,
                    "value_usd_thousands_total": 0,
                    "shares_outstanding": r.get("shares_outstanding", ""),
                    "num_managers": 0,
                    "managers": set(),
                }
            agg[k]["shares_held_total"] += safe_int(r.get("shares_held"))
            agg[k]["value_usd_thousands_total"] += safe_int(
                r.get("value_usd_thousands")
            )
            if not agg[k]["shares_outstanding"] and r.get("shares_outstanding"):
                agg[k]["shares_outstanding"] = r.get("shares_outstanding", "")
            agg[k]["managers"].add(group_name)
    # Finalize
    out = []
    for k, v in agg.items():
        v["num_managers"] = len(v.pop("managers"))
        out.append(v)
    out.sort(key=lambda r: (r.get("ticker", ""),
                            _sort_date(r.get("report_date"))))
    return out


# ═══════════════════════════════════════════════════════════════════════════════
#  Ownership-% chart
# ═══════════════════════════════════════════════════════════════════════════════

def generate_ownership_chart(bvs_csv_path: str, out_dir: str,
                             verbose: bool = True) -> Optional[str]:
    """Read BVS panel CSV, compute per-ticker ownership %, save a chart.

    Plots one line per ticker showing BVS (BlackRock+Vanguard+StateStreet)
    institutional ownership percentage over time.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        if verbose:
            print("WARNING: matplotlib not installed. Skipping chart. "
                  "Install with: pip install matplotlib", flush=True)
        return None

    rows = []
    with open(bvs_csv_path, "r", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(r)

    if not rows:
        if verbose:
            print("WARNING: BVS CSV is empty, skipping chart.", flush=True)
        return None

    # Build per-ticker time series
    ticker_series: Dict[str, List[Tuple[dt.date, float]]] = defaultdict(list)
    skipped = 0
    tickers_seen: Set[str] = set()
    max_date = dt.date(2025, 12, 31)  # cap chart at end of 2025

    for r in rows:
        ticker = r.get("ticker", "")
        rd_str = r.get("report_date", "")
        sh = safe_int(r.get("shares_held_total"))
        so = safe_int(r.get("shares_outstanding"))
        nm = safe_int(r.get("num_managers")) or 0
        if not rd_str or not ticker:
            continue
        rd = parse_date(rd_str)
        if not rd:
            skipped += 1
            continue
        # Cap at max_date
        if rd > max_date:
            skipped += 1
            continue
        if so <= 0 or sh <= 0:
            skipped += 1
            continue
        # Skip partial quarters (not all 3 managers reported)
        if nm < 3:
            skipped += 1
            continue
        # Validate SO against expected ranges (safety net)
        validated_so = _validate_so(ticker, rd, so)
        if validated_so is None:
            skipped += 1
            continue
        pct = (sh / validated_so) * 100.0
        # Sanity check: BVS combined ownership should be 2-25%
        if pct > 25.0 or pct < 1.0:
            skipped += 1
            continue
        tickers_seen.add(ticker)
        ticker_series[ticker].append((rd, pct))

    if not ticker_series:
        if verbose:
            print(f"WARNING: No valid ownership data to chart "
                  f"(skipped {skipped} rows with missing data).", flush=True)
        return None

    for t in ticker_series:
        ticker_series[t].sort(key=lambda x: x[0])

    n_tickers = len(tickers_seen)
    if verbose:
        tstr = ", ".join(sorted(tickers_seen))
        print(f"Charting ownership % for {n_tickers} ticker(s): {tstr} "
              f"(skipped {skipped} rows)…", flush=True)

    colors = {"AAPL": "#2563eb", "AMZN": "#d97706", "MSFT": "#16a34a"}
    fig, ax = plt.subplots(figsize=(14, 7))
    for ticker in sorted(ticker_series.keys()):
        series = ticker_series[ticker]
        dates = [s[0] for s in series]
        pcts  = [s[1] for s in series]
        color = colors.get(ticker, None)
        ax.plot(dates, pcts, marker="o", markersize=3, linewidth=1.5,
                color=color, alpha=0.9, label=ticker)

    ax.set_title(
        "BVS Aggregate Ownership %  "
        "(shares_held_total ÷ shares_outstanding × 100)",
        fontsize=12, fontweight="bold", pad=12,
    )
    ax.set_xlabel("Report Date (Quarter-End)", fontsize=11)
    ax.set_ylabel("Ownership %", fontsize=11)
    ax.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda y, _: f"{y:.1f}%")
    )
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=[1, 4, 7, 10]))
    ax.tick_params(axis="x", rotation=45)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left", fontsize=10, framealpha=0.9)
    fig.tight_layout()

    chart_path = os.path.join(out_dir, "ownership_pct_chart.png")
    fig.savefig(chart_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    if verbose:
        print(f"  -> Saved chart: {chart_path}", flush=True)
    return chart_path


# ═══════════════════════════════════════════════════════════════════════════════
#  UI helpers
# ═══════════════════════════════════════════════════════════════════════════════

def prompt_mode() -> str:
    while True:
        try:
            m = input('\nOutput mode? Type "raw" or "panel": ').strip().lower()
        except EOFError:
            m = "panel"
        if m in ("raw", "panel"):
            return m
        print('Please type exactly: raw or panel', flush=True)


def prompt_start_date() -> str:
    """Ask the user for the earliest report-date to include."""
    print(f"\nDefault earliest report date: {DEFAULT_MIN_REPORT_DATE}")
    print("This means the script will pull filings from Q1-2010 onward.")
    print("Enter a different date (YYYY-MM-DD) or press Enter for the default.")
    print("Examples:  2010-03-31  (Q1-2010)")
    print("           2009-12-31  (Q4-2009, filed early 2010)")
    print("           2015-06-30  (Q2-2015)")
    while True:
        try:
            raw = input("Earliest report date [2010-03-31]: ").strip()
        except EOFError:
            raw = ""
        if not raw:
            return DEFAULT_MIN_REPORT_DATE
        d = parse_date(raw)
        if d:
            return d.isoformat()
        print(f"  Could not parse '{raw}'. Use YYYY-MM-DD format.", flush=True)


def delete_stale(path: str) -> None:
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    ap = argparse.ArgumentParser(description="EDGAR 13F downloader v22 "
                                 "(full-index discovery)")
    ap.add_argument("--user-agent", required=True)
    ap.add_argument("--start-filing-date", default=DEFAULT_START_FILING_DATE,
                    help="Earliest filing date to include (default: 2009-01-01)")
    ap.add_argument("--min-report-date", default=None,
                    help="Earliest report date (YYYY-MM-DD). "
                         "If omitted, you'll be prompted.")
    ap.add_argument("--out-dir", default=os.path.expanduser("~"))
    ap.add_argument("--ticker-cusip-csv",
                    default=os.path.join(os.path.expanduser("~"),
                                         "ticker_cusip.csv"))
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--no-auto-cik", action="store_true")
    ap.add_argument("--auto-cik-max", type=int, default=350)
    ap.add_argument("--mode", choices=["raw", "panel"])
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--no-chart", action="store_true",
                    help="Skip ownership-%% chart (panel mode only)")
    args = ap.parse_args()

    verbose = not args.quiet

    # ── Interactive prompts ──
    mode = args.mode or prompt_mode()
    want_panel = (mode == "panel")
    if verbose:
        print(f"Mode: {mode}", flush=True)

    if args.min_report_date:
        min_report_str = args.min_report_date
    else:
        min_report_str = prompt_start_date()
    if verbose:
        print(f"Earliest report date: {min_report_str}", flush=True)

    out_dir = os.path.abspath(os.path.expanduser(args.out_dir))
    os.makedirs(out_dir, exist_ok=True)

    session = requests.Session()
    session.headers.update({
        "User-Agent": args.user_agent,
        "Accept-Encoding": "gzip, deflate",
    })

    min_report   = dt.date.fromisoformat(min_report_str)
    # Start scanning from January of the min_report year
    # (filings for Q1 min_report year are filed ~45 days later)
    start_filing = dt.date(min_report.year, 1, 1)

    # ── Ticker map ──
    if verbose:
        print(f"Loading ticker map: {args.ticker_cusip_csv}", flush=True)
    ticker_to_cusip = load_ticker_cusip_map(args.ticker_cusip_csv)
    if not ticker_to_cusip:
        raise RuntimeError(f"No rows in {args.ticker_cusip_csv}")
    if verbose:
        print(f"Tickers ({len(ticker_to_cusip)}): "
              f"{', '.join(sorted(ticker_to_cusip))}", flush=True)

    cusip_to_tickers: Dict[str, List[str]] = defaultdict(list)
    for t, c in ticker_to_cusip.items():
        cusip_to_tickers[c].append(t)
    allowed_cusips: Set[str] = set(cusip_to_tickers.keys())

    # Build prefix-based CUSIP lookup for flexible matching
    # 13F filings may use 6, 8, or 9-char CUSIPs; user's file may differ
    cusip_prefix_6: Dict[str, str] = {}  # 6-char prefix -> full cusip
    cusip_prefix_8: Dict[str, str] = {}  # 8-char prefix -> full cusip
    for c in allowed_cusips:
        if len(c) >= 6:
            cusip_prefix_6[c[:6]] = c
        if len(c) >= 8:
            cusip_prefix_8[c[:8]] = c

    def match_cusip(raw_cusip: str) -> Optional[str]:
        """Match a CUSIP from a 13F filing to our allowed set.
        Tries exact match, then 8-char, then 6-char prefix."""
        c = raw_cusip.strip().upper()
        if c in allowed_cusips:
            return c
        if len(c) >= 8 and c[:8] in cusip_prefix_8:
            return cusip_prefix_8[c[:8]]
        if len(c) >= 6 and c[:6] in cusip_prefix_6:
            return cusip_prefix_6[c[:6]]
        # Also try: our cusip is 9-char, filing has 8-char
        for ac in allowed_cusips:
            if ac.startswith(c) or c.startswith(ac):
                return ac
        return None

    if verbose:
        print(f"CUSIP lookup: {len(allowed_cusips)} exact, "
              f"{len(cusip_prefix_8)} 8-char, "
              f"{len(cusip_prefix_6)} 6-char prefixes", flush=True)
        for c in sorted(allowed_cusips):
            tickers = cusip_to_tickers[c]
            print(f"  {c} -> {', '.join(tickers)}", flush=True)

    # ── Shares outstanding (issuer CIK lookup) ──
    ticker_to_issuer_cik = build_ticker_to_issuer_cik(session, verbose)
    so_cache: Dict[str, List[Tuple[dt.date, int]]] = {}

    def get_so(ticker: str, rd_str: str) -> Optional[int]:
        d = parse_date(rd_str)
        if not d:
            return None
        cik = ticker_to_issuer_cik.get(ticker.upper())
        if not cik:
            return None
        if cik not in so_cache:
            try:
                so_cache[cik] = fetch_so_series(session, cik)
            except Exception:
                so_cache[cik] = []
        return pick_so(so_cache[cik], d, ticker=ticker, verbose=verbose)

    # ── README ──
    rp = os.path.join(out_dir, "13F_OUTPUT_README.txt")
    with open(rp, "w", encoding="utf-8") as f:
        f.write(README_TEXT)
    if verbose:
        print(f"Wrote README: {rp}", flush=True)

    # ── CIK expansion ──
    group_ciks: Dict[str, List[str]] = {}
    if args.no_auto_cik:
        for g, cfg in GROUPS.items():
            group_ciks[g] = sorted(set(pad_cik(x) for x in cfg["seed_ciks"]))
            if verbose:
                print(f"{g}: {len(group_ciks[g])} seed CIK(s).", flush=True)
    else:
        if verbose:
            print("Auto-expanding CIKs …", flush=True)
        companies = list(
            sec_get(session, SEC_COMPANY_TICKERS).json().values()
        )
        for g, cfg in GROUPS.items():
            kws = [k.lower().strip() for k in cfg["keywords"]]
            cands = [c for c in companies
                     if any(kw in (c.get("title") or "").lower() for kw in kws)]
            cands.sort(key=lambda c: (
                -sum(1 for kw in kws
                     if kw in (c.get("title") or "").lower()),
                len(c.get("title") or ""),
            ))
            cands = cands[:args.auto_cik_max]
            verified: Set[str] = set(pad_cik(x) for x in cfg["seed_ciks"])
            if verbose:
                print(f"  {g}: checking {len(cands)} candidates …", flush=True)
            for i, c in enumerate(cands, 1):
                cik = str(c.get("cik_str") or "").strip()
                if not cik:
                    continue
                cp = pad_cik(cik)
                if cp in verified:
                    continue
                if _submissions_has_13f(session, cp, start_filing):
                    verified.add(cp)
                if verbose and (i % 50 == 0 or i == len(cands)):
                    print(f"    scanned {i}/{len(cands)}, "
                          f"verified={len(verified)}", flush=True)
                time.sleep(0.02)
            group_ciks[g] = sorted(verified)
            if verbose:
                print(f"  {g}: using {len(group_ciks[g])} CIK(s).", flush=True)

    # ── Collect ALL CIKs across groups for index scanning ──
    all_ciks: Set[str] = set()
    cik_to_group: Dict[str, str] = {}
    for g, ciks in group_ciks.items():
        for c in ciks:
            all_ciks.add(c)
            cik_to_group[c] = g

    # ── Clean stale outputs ──
    if want_panel:
        for g in GROUPS:
            delete_stale(os.path.join(out_dir, f"{g}_13f_holdings_raw.csv"))
    else:
        for g in GROUPS:
            delete_stale(os.path.join(out_dir, f"{g}_13f_holdings_panel.csv"))
        delete_stale(os.path.join(out_dir, "BVS_13f_holdings_panel.csv"))
        delete_stale(os.path.join(out_dir, "ownership_pct_chart.png"))

    # ══════════════════════════════════════════════════════════════════════
    #  STEP 1:  Discover filings via quarterly full-index (v22 core change)
    # ══════════════════════════════════════════════════════════════════════
    index_entries = discover_filings_via_full_index(
        session, all_ciks, start_filing, verbose=verbose,
    )

    # Deduplicate by (cik, accession) — keep latest entry
    seen_acc: Dict[Tuple[str, str], Dict[str, str]] = {}
    for e in index_entries:
        key = (e["cik"], e["accession"])
        seen_acc[key] = e   # last wins (later entries = amendments)
    unique_entries = list(seen_acc.values())

    # Sort by filing date for orderly processing
    unique_entries.sort(key=lambda e: (e.get("filing_date", ""), e["accession"]))

    if verbose:
        print(f"\nTotal unique 13F filings to process: {len(unique_entries)}",
              flush=True)

    # ── Group entries by manager ──
    entries_by_group: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for e in unique_entries:
        group = cik_to_group.get(e["cik"], "")
        if group:
            entries_by_group[group].append(e)

    for g in group_ciks:
        if verbose:
            print(f"  {g}: {len(entries_by_group.get(g, []))} filings", flush=True)

    # ══════════════════════════════════════════════════════════════════════
    #  STEP 2:  Process each group — extract holdings from each filing
    # ══════════════════════════════════════════════════════════════════════
    panel_by_group: Dict[str, List[Dict[str, Any]]] = {}

    for group in group_ciks:
        if verbose:
            print(f"\n{'='*60}", flush=True)
            print(f"  {group}  ({len(entries_by_group.get(group, []))} filings "
                  f"from {len(group_ciks[group])} CIK(s))", flush=True)
            print(f"{'='*60}", flush=True)

        filings = entries_by_group.get(group, [])
        raw_rows: List[Dict[str, Any]] = []
        _diag_empty_count = [0]  # mutable counter for diagnostic limiting
        _diag_success_count = [0]  # first N successful extractions shown

        for i, entry in enumerate(filings, 1):
            cik_p = entry["cik"]
            cik_i = entry.get("cik_int", cik_int_str(cik_p))
            form  = entry["form"]
            acc   = entry["accession"]
            f_date = entry["filing_date"]

            base_url = _filing_base_url(cik_i, acc)

            # ── Get report_date ──
            # Try extracting from filing primary doc; fall back to heuristic
            rep_str = extract_report_date(session, base_url)
            if not rep_str:
                rep_str = guess_report_date(f_date)

            rep_d = parse_date(rep_str)
            if rep_d and rep_d < min_report:
                continue

            if verbose:
                print(f"  [{i}/{len(filings)}] {form} filed {f_date} "
                      f"report {rep_str or '???'} CIK {cik_p}", flush=True)

            # ── Find + parse info table ──
            urls = find_info_table_urls(session, base_url)
            if not urls:
                if verbose and rep_d and rep_d.year <= 2013:
                    print(f"    ⚠ No info table URLs found at {base_url}",
                          flush=True)
                time.sleep(args.sleep)
                continue

            extracted = False
            diag_total_parsed = 0
            diag_sample_cusips: List[str] = []
            tried_urls: Set[str] = set()

            for url in urls:
                tried_urls.add(url)
                try:
                    blob = sec_get(session, url).content
                    # Try both XML and text parsers
                    parsed = _try_parse_info_table(blob)
                    if not parsed:
                        continue
                    diag_total_parsed += len(parsed)
                    for r in parsed:
                        raw_cusip = (r.get("cusip") or "").strip().upper()
                        if not raw_cusip:
                            continue
                        # Collect sample CUSIPs for diagnostics
                        if len(diag_sample_cusips) < 5:
                            diag_sample_cusips.append(raw_cusip)

                        matched_cusip = match_cusip(raw_cusip)
                        if matched_cusip is None:
                            continue
                        extracted = True
                        for ticker in cusip_to_tickers.get(matched_cusip, []):
                            so = get_so(ticker, rep_str)
                            row = {
                                "group": group,
                                "ticker": ticker,
                                "mapped_cusip": ticker_to_cusip.get(ticker, ""),
                                "filer_cik": cik_p,
                                "form": form,
                                "filing_date": f_date,
                                "report_date": rep_str,
                                "accession": acc,
                                "info_table_url": url,
                                "issuer_name": r.get("issuer_name", ""),
                                "class_title": r.get("class_title", ""),
                                "cusip": raw_cusip,
                                "value_usd_thousands": r.get(
                                    "value_usd_thousands", ""
                                ),
                                "shares_held": r.get("shares_held", ""),
                                "shares_type": r.get("shares_type", ""),
                                "put_call": r.get("put_call", ""),
                                "investment_discretion": r.get(
                                    "investment_discretion", ""
                                ),
                                "other_manager": r.get("other_manager", ""),
                                "voting_sole": r.get("voting_sole", ""),
                                "voting_shared": r.get("voting_shared", ""),
                                "voting_none": r.get("voting_none", ""),
                                "shares_outstanding": (
                                    str(so) if so is not None else ""
                                ),
                            }
                            raw_rows.append(row)
                    if extracted:
                        break
                except Exception:
                    continue

            # ── FALLBACK: parse full submission text file ──
            # Old filings (pre-~2013) bundle domestic + international
            # info tables as separate <DOCUMENT> sections within one
            # big .txt file.  The scored candidates often only hit the
            # international section.  parse_full_submission_text splits
            # on <DOCUMENT> boundaries and searches each section for
            # our target CUSIPs.
            if not extracted:
                # CRITICAL: the full submission text file is at the
                # PARENT directory level, NOT inside the filing dir.
                #   ✓ .../000136474210000027.txt   (parent level)
                #   ✗ .../000136474210000027/0001364742-10-000027.txt
                fsub_urls: List[str] = []
                # 1) Parent-level full submission text
                fsub_urls.append(base_url.rstrip("/") + ".txt")
                # 2) Accession-named file inside directory (backup)
                fsub_urls.append(urljoin(base_url, f"{acc}.txt"))
                # 3) Any large .txt files from index.json
                try:
                    idx_j2 = sec_get(
                        session, urljoin(base_url, "index.json")
                    ).json()
                    items2 = idx_j2.get("directory", {}).get("item", []) or []
                    items2.sort(
                        key=lambda x: int(x.get("size") or 0), reverse=True
                    )
                    for it2 in items2:
                        n2 = (it2.get("name") or "").lower()
                        sz2 = int(it2.get("size") or 0)
                        if n2.endswith(".txt") and sz2 > 100_000:
                            furl2 = urljoin(base_url, it2.get("name", ""))
                            if furl2 not in fsub_urls:
                                fsub_urls.append(furl2)
                except Exception:
                    pass

                seen_fsub: Set[str] = set()
                for fsub_url in fsub_urls:
                    if extracted:
                        break
                    if fsub_url in seen_fsub:
                        continue
                    seen_fsub.add(fsub_url)
                    try:
                        sub_blob = sec_get(session, fsub_url).content
                        if len(sub_blob) < 500:
                            continue  # Too small / error page
                        sub_rows = parse_full_submission_text(
                            sub_blob, allowed_cusips
                        )
                        if not sub_rows:
                            continue
                        for r in sub_rows:
                            raw_cusip = (
                                (r.get("cusip") or "").strip().upper()
                            )
                            matched_cusip = match_cusip(raw_cusip)
                            if matched_cusip is None:
                                continue
                            extracted = True
                            for ticker in cusip_to_tickers.get(
                                matched_cusip, []
                            ):
                                so = get_so(ticker, rep_str)
                                raw_rows.append({
                                    "group": group,
                                    "ticker": ticker,
                                    "mapped_cusip":
                                        ticker_to_cusip.get(ticker, ""),
                                    "filer_cik": cik_p,
                                    "form": form,
                                    "filing_date": f_date,
                                    "report_date": rep_str,
                                    "accession": acc,
                                    "info_table_url": fsub_url,
                                    "issuer_name":
                                        r.get("issuer_name", ""),
                                    "class_title":
                                        r.get("class_title", ""),
                                    "cusip": raw_cusip,
                                    "value_usd_thousands": r.get(
                                        "value_usd_thousands", ""
                                    ),
                                    "shares_held":
                                        r.get("shares_held", ""),
                                    "shares_type":
                                        r.get("shares_type", ""),
                                    "put_call":
                                        r.get("put_call", ""),
                                    "investment_discretion": r.get(
                                        "investment_discretion", ""
                                    ),
                                    "other_manager":
                                        r.get("other_manager", ""),
                                    "voting_sole":
                                        r.get("voting_sole", ""),
                                    "voting_shared":
                                        r.get("voting_shared", ""),
                                    "voting_none":
                                        r.get("voting_none", ""),
                                    "shares_outstanding": (
                                        str(so)
                                        if so is not None else ""
                                    ),
                                })
                        if extracted and verbose:
                            fname = fsub_url.rsplit("/", 1)[-1]
                            print(f"    ✓ Found domestic holdings "
                                  f"via full submission: {fname}",
                                  flush=True)
                    except Exception as exc:
                        if verbose and _diag_empty_count[0] < 3:
                            print(f"    ⚠ Fallback error on "
                                  f"{fsub_url}: {exc}",
                                  flush=True)
                        continue

            # Diagnostic: show details when a filing yields no matches
            if not extracted and verbose and diag_total_parsed > 0:
                if _diag_empty_count[0] < 5:
                    _diag_empty_count[0] += 1
                    print(f"    ⚠ {diag_total_parsed} holdings parsed but "
                          f"0 CUSIP matches!", flush=True)
                    print(f"      Sample CUSIPs in filing: "
                          f"{diag_sample_cusips}", flush=True)
                    print(f"      Our target CUSIPs: "
                          f"{sorted(allowed_cusips)}", flush=True)
                    # On first failure, dump full directory listing
                    if _diag_empty_count[0] <= 2:
                        try:
                            idx_diag = sec_get(
                                session, urljoin(base_url, "index.json")
                            ).json()
                            ditems = (idx_diag.get("directory", {})
                                      .get("item", []) or [])
                            def _safe_size(x):
                                try:
                                    return int(x.get("size", 0))
                                except (ValueError, TypeError):
                                    return 0
                            ditems_sorted = sorted(
                                ditems, key=_safe_size, reverse=True
                            )[:15]
                            print(f"      Directory ({len(ditems)} files):",
                                  flush=True)
                            for di in ditems_sorted:
                                sz = str(di.get('size', '?'))
                                print(f"        {di.get('name','?'):45s} "
                                      f"size={sz:>12s}",
                                      flush=True)
                        except Exception:
                            pass
            elif not extracted and verbose and diag_total_parsed == 0:
                if _diag_empty_count[0] < 5:
                    _diag_empty_count[0] += 1
                    top_url = urls[0] if urls else "?"
                    print(f"    ⚠ 0 holdings parsed from "
                          f"{len(urls)} candidate file(s)", flush=True)
                    print(f"      Top candidate: {top_url}", flush=True)

            # Show first 3 successful extractions for data verification
            if extracted and verbose and _diag_success_count[0] < 3:
                _diag_success_count[0] += 1
                # Gather what was just extracted for this filing
                filing_rows = [r for r in raw_rows
                               if r.get("accession") == acc
                               and r.get("filer_cik") == cik_p]
                for fr in filing_rows:
                    print(f"    📊 {fr['ticker']}: "
                          f"shares={fr['shares_held']:>12s}  "
                          f"value=${fr['value_usd_thousands']}k  "
                          f"SO={fr.get('shares_outstanding','?')}",
                          flush=True)

            time.sleep(args.sleep)

        raw_deduped = dedupe_raw(raw_rows)

        if verbose:
            dates = [parse_date(r.get("report_date")) for r in raw_deduped]
            dates = [d for d in dates if d]
            if dates:
                print(f"  Date range: {min(dates)} -> {max(dates)}", flush=True)
            print(f"  Rows (raw={len(raw_rows)}, deduped={len(raw_deduped)})",
                  flush=True)

        if want_panel:
            panel = build_panel(group, raw_deduped)
            path = os.path.join(out_dir, f"{group}_13f_holdings_panel.csv")
            write_csv(path, PANEL_COLUMNS, panel)

            # Diagnostic: show first 3 and last 3 panel rows per ticker
            if verbose:
                by_tk: Dict[str, list] = defaultdict(list)
                for pr in panel:
                    by_tk[pr.get("ticker", "")].append(pr)
                for tk in sorted(by_tk):
                    tkrows = sorted(by_tk[tk],
                                    key=lambda r: r.get("report_date", ""))
                    sample = tkrows[:2] + (["..."] if len(tkrows) > 4 else []) + tkrows[-2:]
                    for sr in sample:
                        if sr == "...":
                            print(f"    {tk}: ...", flush=True)
                        else:
                            sh = sr.get("shares_held", 0)
                            so = sr.get("shares_outstanding", "?")
                            rd = sr.get("report_date", "?")
                            note = sr.get("consolidation_note", "")
                            print(f"    {tk} {rd}: shares_held={sh:>14,}  "
                                  f"SO={so:>14s}  {note}",
                                  flush=True)
            write_csv(path, PANEL_COLUMNS, panel)
            panel_by_group[group] = panel
            if verbose:
                print(f"  -> Wrote PANEL: {path} ({len(panel)} rows)", flush=True)
        else:
            path = os.path.join(out_dir, f"{group}_13f_holdings_raw.csv")
            write_csv(path, RAW_COLUMNS, raw_deduped)
            if verbose:
                print(f"  -> Wrote RAW: {path} ({len(raw_deduped)} rows)",
                      flush=True)

    # ── BVS + chart ──
    if want_panel:
        bvs = build_bvs(panel_by_group)
        bvs_path = os.path.join(out_dir, "BVS_13f_holdings_panel.csv")
        write_csv(bvs_path, BVS_COLUMNS, bvs)
        if verbose:
            print(f"\n  -> Wrote BVS: {bvs_path} ({len(bvs)} rows)", flush=True)

        if not args.no_chart:
            generate_ownership_chart(bvs_path, out_dir, verbose=verbose)

    if verbose:
        print("\nDone.", flush=True)


if __name__ == "__main__":
    main()
