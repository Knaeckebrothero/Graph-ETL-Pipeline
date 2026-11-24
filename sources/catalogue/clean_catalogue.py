#!/usr/bin/env python3
"""Rebuild sources_catalog_clean.csv from the unverified candidate catalogue.

Every value this script writes is a PRE-FILL for a human check. Nothing here is
verified; the column `verified_by_author` is left empty on purpose.

Inputs (read only)
    sources_catalog.csv                              324 LLM-generated candidate rows
    quellenliste/research_link_sources_unique.csv    older list with stable ids
    ../manifest.csv                                  the files that are on disk
    prefill_judgements.csv                           judgement values: one row per catalogue
                                                     row (key orig_row) or per added source
                                                     (key source_id)
Output
    sources_catalog_clean.csv

Division of labour
    Rules live in this file. Whatever needed a person's or a model's judgement lives in
    prefill_judgements.csv and is labelled `judgement` in the column prefill_basis.
    A blank cell in that file means "let the rules decide".

Rules (the letters appear in the columns `repaired` and `prefill_basis`)
    U   URL normalisation: stray quotes stripped, scheme and host lower-cased, bare host
        gets a slash, tracking parameters and empty fragments dropped, illegal characters
        percent-encoded. http is not upgraded to https, deeper trailing slashes are kept.
    I1  source_id = quellenliste id when the normalised URL is the same
    I2  ... or the same once hyphens are ignored (the quellenliste has lost hyphens
        inside URLs); otherwise cat-<host>-<nn>, counted per host in catalogue order.
        Sources without a URL get cat-local-<nn>.
    P1  description is parser residue ("URL", "curl ...", "#")       -> dropped
    P2  name is the host                                             -> replaced (judgement file)
    P3  name is a section label ("Data Source Categories")           -> replaced (judgement file)
    P4  name is markup residue ("# ...", "URL: ...", "@prefix")      -> stripped or replaced
    P5  Format cell holds prose or a URL fragment                    -> moved to notes
    P6  License cell holds prose                                     -> moved to notes
    P7  description only repeats the name                            -> dropped
    P8  URL split across columns                                     -> rejoined (judgement file)
    P9  URL is a template with a placeholder                         -> flagged
    L   licence is copied, never inferred: a licence name in the catalogue's License or
        Description column, the manifest, or a statement quoted from the source's own
        page (judgement file); access terms such as "Public" or "Free" are not licences.
    F1  format from the URL extension          F2  from the catalogue Format cell
    F3  from the observed content type         F4  API-like URL
    F5  the URL serves HTML                    (judgement and manifest come first)
    H   host rules: statute portals, community platforms, the Open Food Facts family
    M1  manifest link when the manifest itself names the URL
    D1  duplicate when the normalised URL is the same; D2 same source under another URL
        (judgement file). Duplicates keep their row and inherit the classes of the
        canonical row unless the judgement file says otherwise.

Usage
    python3 clean_catalogue.py                  rebuild; the access columns are carried
                                                over from the existing output (no network)
    python3 clean_catalogue.py --check-access   also re-check every URL: HEAD, then a small
                                                GET if the HEAD fails, 15 s, 4 in parallel,
                                                one retry, generic User-Agent
    python3 clean_catalogue.py --stats          also print the tables of CLEANING_REPORT.md

Standard library only.
"""
from __future__ import annotations

import argparse
import csv
import datetime
import re
import ssl
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit, urlunsplit

HERE = Path(__file__).resolve().parent

OUT_COLS = [
    "source_id", "manifest_id", "orig_row", "added", "source_name", "publisher", "url",
    "url_original", "host", "format", "licence", "licence_basis", "modality_stratum",
    "authority_class", "jurisdiction", "population_screen", "screen_reason",
    "access_status", "checked_on", "duplicate_of", "repaired", "prefill_basis",
    "verified_by_author", "notes",
]
JUDGE_COLS = [
    "orig_row", "source_id", "added_reason", "manifest_id", "source_name", "name_basis",
    "url", "publisher", "format", "modality_stratum", "authority_class", "jurisdiction",
    "population_screen", "screen_reason", "duplicate_of", "licence", "licence_quote", "note",
]

FORMATS = ["HTML", "PDF", "CSV", "XLSX", "JSON", "XML", "API", "database (web query)",
           "dataset download", "other"]
MODALITIES = ["numeric tabular", "tabular-categorical", "catalogue", "free text", "unclear"]
AUTHORITIES = ["A", "B", "C", "D", "unclear"]
JURISDICTIONS = ["Frankfurt", "campus", "Hessen", "federal", "EU", "Munich", "Berlin",
                 "other municipality or state", "none", "unclear"]
SCREENS = ["in", "contrast", "out", "unclear"]

USER_AGENT = "Mozilla/5.0 (compatible; catalogue-check)"
TIMEOUT_S = 15
MAX_PARALLEL = 4

# --------------------------------------------------------------------------- URLs

TRACKING = re.compile(r"^(utm_\w+|fbclid|gclid|dclid|msclkid|yclid|mc_cid|mc_eid|_ga|igshid)$", re.I)


def _clean_query(query: str) -> str:
    parts = [p for p in query.split("&") if p and not TRACKING.match(p.split("=")[0])]
    return "&".join(parts)


def normalise_url(raw: str) -> tuple[str, list[str]]:
    """Rule U1-U5. Returns the normalised URL and the list of changes made.

    U1 stray quotes and whitespace stripped        U2 scheme and host lower-cased,
    U3 a bare host gets a trailing slash; deeper      scheme added if missing (http is
       paths are kept as published, because           NOT silently upgraded to https)
       removing a slash can break a URL            U4 tracking parameters dropped
    U5 empty fragment dropped, characters that are not legal in a URL percent-encoded
    """
    changes: list[str] = []
    u = raw.strip().strip("\"'").strip()
    if u != raw:
        changes.append("stray characters stripped")
    if not u:
        return "", changes
    if not re.match(r"^[A-Za-z][A-Za-z0-9+.\-]*://", u):
        u = "https://" + u
        changes.append("scheme added")
    s = urlsplit(u)
    scheme, host = s.scheme.lower(), s.netloc.lower()
    if scheme != s.scheme or host != s.netloc:
        changes.append("scheme/host lower-cased")
    path = s.path
    if path == "":
        path = "/"
        changes.append("trailing slash added to bare host")
    query = _clean_query(s.query)
    if query != s.query:
        changes.append("tracking parameters dropped")
    if "#" in u and not s.fragment:
        changes.append("empty fragment dropped")
    enc_path = quote(path, safe="/%:@!$&'()*+,;=~{}")
    enc_query = quote(query, safe="/%:@!$&'()*+,;=~?{}")
    if enc_path != path or enc_query != query:
        changes.append("illegal characters percent-encoded")
    return urlunsplit((scheme, host, enc_path, enc_query, s.fragment)), changes


def host_of(url: str) -> str:
    h = re.sub(r":(443|80)$", "", urlsplit(url).netloc.lower())
    return h[4:] if h.startswith("www.") else h


def match_key(url: str) -> str:
    """Comparison key: scheme, www., trailing slash, fragment, percent-encoding,
    tracking parameters and parameter order are ignored."""
    url = url.strip().strip("\"'")
    s = urlsplit(url)
    q = "&".join(sorted(unquote(p) for p in _clean_query(s.query).split("&") if p))
    return host_of(url) + unquote(s.path).rstrip("/") + ("?" + q if q else "")


def loose_key(url: str) -> str:
    """Second-tier key: additionally ignores hyphens and case. The quellenliste has
    lost hyphens inside URLs ('gesetze-iminternet.de', it looks like line-break
    damage), so an exact comparison misses those rows."""
    return match_key(url).replace("-", "").lower()


def slug_host(url: str) -> str:
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", host_of(url))).strip("-") or "no-host"


# --------------------------------------------------------------------------- artefacts

GENERIC_NAMES = {"data source categories", "categories", "urls", "wiki", "download",
                 "download url", "raw data page", "base url", "store links", "web beta"}
DESC_RESIDUE = re.compile(r'^(URL|URLs?:.*|#|""|curl\b.*|grep\b.*)$', re.S)
LICENCE_NAME = re.compile(
    r"\b(ODbL|MIT|CC0|CC[ -]BY(?:[ -](?:SA|NC|ND))*(?: \d\.\d)?|GFDL|L?GPL(?:v\d)?|"
    r"Apache(?: License)?(?: 2\.0)?|BSD|Public domain)\b")
ACCESS_TERM = re.compile(r"\b(public|free|open|commercial|paid|undocumented|registration)\b", re.I)

FORMAT_TOKENS = {
    "pdf": "PDF", "html": "HTML", "web": "HTML", "csv": "CSV", "excel": "XLSX", "xls": "XLSX",
    "xlsx": "XLSX", "json": "JSON", "geojson": "JSON", "xml": "XML", "api": "API",
    "rest": "API", "openapi": "API", "wfs": "API", "overpass": "API", "rdf": "other",
    "skos": "other", "sql": "other", "images": "other",
}
FORMAT_NOISE = {"bulk", "tables", "paid", "catalog", "ld", "iuclid", "ilcd"}


def map_catalogue_format(cell: str) -> tuple[str, bool]:
    """Rule F2. Returns (normalised format, is_residue). A cell that is longer than 30
    characters or contains words outside the format vocabulary is not a format but
    displaced prose (rule P5)."""
    cell = cell.strip()
    if not cell:
        return "", False
    text = re.sub(r"\(no api\)", "", cell, flags=re.I)
    tokens = [t for t in re.split(r"[^A-Za-z0-9]+", text.lower()) if t]
    unknown = [t for t in tokens if t not in FORMAT_TOKENS and t not in FORMAT_NOISE]
    if len(cell) > 30 or unknown:
        return "", True
    found = {FORMAT_TOKENS[t] for t in tokens if t in FORMAT_TOKENS}
    if "API" in found:
        found -= {"JSON", "XML"}          # payload formats of the API itself
    return "; ".join(f for f in FORMATS if f in found), False


EXT_FORMAT = {".pdf": "PDF", ".csv": "CSV", ".xlsx": "XLSX", ".xls": "XLSX", ".json": "JSON",
              ".ndjson": "JSON", ".xml": "XML", ".gz": "dataset download",
              ".zip": "dataset download", ".parquet": "dataset download"}
CTYPE_FORMAT = {"application/pdf": "PDF", "text/csv": "CSV", "application/json": "JSON",
                "application/geo+json": "JSON", "application/xml": "XML", "text/xml": "XML",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "XLSX",
                "application/vnd.ms-excel": "XLSX", "application/zip": "dataset download",
                "application/gzip": "dataset download", "application/x-gzip": "dataset download",
                "text/html": "HTML", "application/xhtml+xml": "HTML"}
API_URL = re.compile(r"(^api\.|/api(/|$|\?)|/rest(/|$)|/sparql|/ows\?|/interpreter$|/pug$)", re.I)
MANIFEST_FORMAT = {"CSV": "CSV", "JSON": "JSON", "PDF": "PDF", "XLSX": "XLSX", "DOCX": "other",
                   "Parquet": "dataset download", "JSONL (gzip)": "dataset download",
                   "JSONL": "JSON", "JSON API": "API", "WordNet": "other"}
MANIFEST_STRATUM = {"tabular text": "tabular-categorical", "product catalogue": "catalogue",
                    "free text": "free text"}

# Host rules (rule H). Only hosts whose class follows from what the host is.
_STATUTE_DE = dict(publisher="Bundesministerium der Justiz / Bundesamt für Justiz (gesetze-im-internet.de)",
                   authority_class="A", jurisdiction="federal", modality_stratum="free text")
_OFF = dict(publisher="Open Food Facts (association)", authority_class="D", jurisdiction="none")
_COMMUNITY = dict(authority_class="D", jurisdiction="none")
HOST_RULES = {
    "gesetze-im-internet.de": _STATUTE_DE,
    "eur-lex.europa.eu": dict(publisher="European Union (EUR-Lex)", authority_class="A",
                              jurisdiction="EU", modality_stratum="free text"),
    "github.com": _COMMUNITY, "raw.githubusercontent.com": _COMMUNITY,
    "gist.githubusercontent.com": _COMMUNITY, "kaggle.com": _COMMUNITY,
    "universe.roboflow.com": _COMMUNITY, "pypi.org": _COMMUNITY, "huggingface.co": _COMMUNITY,
    "arxiv.org": dict(publisher="arXiv (preprint server)", authority_class="D",
                      jurisdiction="none", modality_stratum="free text"),
    "en.wikipedia.org": dict(publisher="Wikipedia (community)", authority_class="D", jurisdiction="none"),
    "wikidata.org": dict(publisher="Wikidata (community)", authority_class="D", jurisdiction="none"),
    "query.wikidata.org": dict(publisher="Wikidata (community)", authority_class="D", jurisdiction="none"),
    "openstreetmap.org": dict(publisher="OpenStreetMap (community)", authority_class="D", jurisdiction="none"),
    "wiki.openstreetmap.org": dict(publisher="OpenStreetMap (community)", authority_class="D", jurisdiction="none"),
    "world.openfoodfacts.org": _OFF, "static.openfoodfacts.org": _OFF, "wiki.openfoodfacts.org": _OFF,
    "blog.openfoodfacts.org": _OFF, "forum.openfoodfacts.org": _OFF, "openfoodfacts.github.io": _OFF,
    "world.openbeautyfacts.org": _OFF, "world.openpetfoodfacts.org": _OFF,
    "world.openproductsfacts.org": _OFF,
}
GITHUB_HOSTS = {"github.com", "raw.githubusercontent.com", "gist.githubusercontent.com"}

# --------------------------------------------------------------------------- access check

LOGIN_HINT = re.compile(r"/(login|signin|sign-in|sign_in|sso|auth|anmelden)(/|\?|$)", re.I)


def _request_raw(url: str, method: str, want_body: bool) -> dict:
    req = urllib.request.Request(url, method=method,
                                 headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S, context=ssl.create_default_context()) as resp:
            body = resp.read(65536) if (want_body and method == "GET") else b""
            return dict(code=resp.status, final=resp.geturl(),
                        ctype=resp.headers.get_content_type(), body=body, err="")
    except urllib.error.HTTPError as exc:
        ctype = exc.headers.get_content_type() if exc.headers else ""
        return dict(code=exc.code, final=exc.geturl() or url, ctype=ctype, body=b"", err="")
    except Exception as exc:  # DNS, TLS, timeout, connection reset ...
        reason = getattr(exc, "reason", exc)
        return dict(code=0, final=url, ctype="", body=b"",
                    err=f"{type(reason).__name__}: {str(reason)[:80]}")


def _request(url: str, method: str, want_body: bool) -> dict:
    """Hard deadline around one request. The socket timeout does not cover name
    resolution, so a hanging resolver would otherwise block far longer than 15 s."""
    box: dict = {}
    worker = threading.Thread(target=lambda: box.update(r=_request_raw(url, method, want_body)),
                              daemon=True)
    worker.start()
    worker.join(TIMEOUT_S + 3)
    return box.get("r") or dict(code=0, final=url, ctype="", body=b"",
                                err="timeout: no answer within the deadline (name resolution or connection hung)")


def _safe_url(text: str) -> str:
    return re.sub(r"\s+", "", text).replace("|", "%7C").replace(";", "%3B")


def _safe_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).replace("|", "/").replace(";", ",").strip()


def check_url(url: str, want_title: bool = False) -> dict:
    """One attempt plus one retry; each attempt is a HEAD, then a small GET if the HEAD
    did not succeed. Nothing but status, final URL, content type (and optionally the
    <title>) is kept; page content is treated as untrusted data."""
    if "{" in url or "}" in url:
        return dict(label="unreachable", via="-", code="-", type="-", final="-",
                    remark="URL template with a placeholder, not requested", title="")
    res, via = None, "HEAD"
    for attempt in (1, 2):
        res, via = _request(url, "HEAD", False), "HEAD"
        if not 200 <= res["code"] < 400:
            got = _request(url, "GET", want_title)
            if got["code"] or not res["code"]:
                res, via = got, "GET"
        if 200 <= res["code"] < 400:
            break
        if attempt == 1:
            time.sleep(2)
    title = ""
    if want_title and 200 <= res["code"] < 400 and "html" in res["ctype"]:
        if via == "HEAD":
            got = _request(url, "GET", True)
            if 200 <= got["code"] < 400:
                res = dict(res, body=got["body"])
        m = re.search(rb"<title[^>]*>(.*?)</title>", res["body"], re.I | re.S)
        if m:
            title = re.sub(r"\s+", " ", m.group(1).decode("utf-8", "replace")).strip()[:160]
    code, final = res["code"], res["final"]
    remark = ""
    if code == 0:
        label, remark = "unreachable", res["err"]
    elif code in (401, 407):
        label = "needs login"
    elif 200 <= code < 400:
        fs, us = urlsplit(final), urlsplit(url)
        if LOGIN_HINT.search(fs.path) and not LOGIN_HINT.search(us.path):
            label = "needs login"
        elif host_of(final) != host_of(url):
            label = f"redirect -> {host_of(final)}"
        else:
            label = f"ok ({code})"
            if unquote(fs.path).rstrip("/") != unquote(us.path).rstrip("/"):
                remark = ("deep link redirected to the site root (possible soft 404)"
                          if fs.path in ("", "/") else "redirected within the host")
    else:
        label = f"error {code}"
        if code in (403, 429):
            remark = "may be bot protection, check in a browser"
        elif host_of(final) != host_of(url):
            remark = f"after a redirect to {host_of(final)}"
    return dict(label=label, via=via, code=str(code or "-"), type=res["ctype"] or "-",
                final=_safe_url(final) if final != url else "-", remark=_safe_text(remark), title=title)


def run_access_check(urls: list[str], want_title: bool) -> dict[str, dict]:
    today = datetime.date.today().isoformat()
    done: dict[str, dict] = {}

    def work(u: str) -> tuple[str, dict]:
        return u, dict(check_url(u, want_title), checked_on=today)

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as pool:
        for i, (u, rec) in enumerate(pool.map(work, urls), 1):
            done[u] = rec
            print(f"  [{i}/{len(urls)}] {rec['label']:<28} {u[:90]}", file=sys.stderr, flush=True)
    return done


ACCESS_SEG = re.compile(r"^access: url=(?P<label>.*?); via=(?P<via>.*?); code=(?P<code>.*?); "
                        r"type=(?P<type>.*?); final=(?P<final>.*?); remark=(?P<remark>.*)$")


def access_segment(rec: dict) -> str:
    return ("access: url={label}; via={via}; code={code}; type={type}; final={final}; "
            "remark={remark}").format(**rec)


def load_previous(path: Path) -> tuple[dict[str, dict], dict[str, tuple[str, str]]]:
    """Carry the last access check over from the existing output: the request record
    by URL, and access_status / checked_on by row."""
    by_url: dict[str, dict] = {}
    by_row: dict[str, tuple[str, str]] = {}
    if not path.exists():
        return by_url, by_row
    with path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            by_row[row.get("orig_row") or row.get("source_id", "")] = (row.get("access_status", ""),
                                                                      row.get("checked_on", ""))
            for part in row.get("notes", "").split(" | "):
                m = ACCESS_SEG.match(part)
                if m and row.get("url"):
                    by_url[row["url"]] = dict(m.groupdict(), checked_on=row.get("checked_on", ""), title="")
    return by_url, by_row


# --------------------------------------------------------------------------- build

def read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig", newline="") as fh:
        return [{k: (v or "").strip() for k, v in row.items()} for row in csv.DictReader(fh)]


def seg(text: str) -> str:
    """Notes are ' | '-separated segments; keep the separator out of the content."""
    return re.sub(r"\s+", " ", text).replace(" | ", " / ").replace("|", "/").strip()


def load_judgements(path: Path) -> tuple[dict[str, dict], list[dict]]:
    by_row: dict[str, dict] = {}
    added: list[dict] = []
    for j in (read_csv(path) if path.exists() else []):
        for col, vocab in (("modality_stratum", MODALITIES), ("authority_class", AUTHORITIES),
                           ("jurisdiction", JURISDICTIONS), ("population_screen", SCREENS)):
            if j.get(col) and j[col] not in vocab:
                sys.exit(f"{path.name}: {col}={j[col]!r} is not one of {vocab}")
        for f in filter(None, (x.strip() for x in j.get("format", "").split(";"))):
            if f not in FORMATS + ["unclear"]:
                sys.exit(f"{path.name}: format {f!r} is not in the vocabulary")
        if j.get("orig_row"):
            if j["orig_row"] in by_row:
                sys.exit(f"{path.name}: orig_row {j['orig_row']} appears twice")
            by_row[j["orig_row"]] = j
        elif j.get("source_id"):
            added.append(j)
        else:
            sys.exit(f"{path.name}: a row has neither orig_row nor source_id")
    return by_row, added


def build(args) -> list[dict]:
    cat = read_csv(args.catalogue)
    quellen = read_csv(args.quellenliste)
    manifest = {r["id"]: r for r in read_csv(args.manifest)}
    judge_by_row, added_specs = load_judgements(args.judgements)
    repo_root = args.manifest.resolve().parent.parent

    # ---- quellenliste index (its ids are sha1(canonical_url)[:12])
    q_exact, q_loose = defaultdict(list), defaultdict(list)
    for i, q in enumerate(quellen):
        q["_i"] = i
        q_exact[match_key(q["canonical_url"])].append(q)
        q_loose[loose_key(q["canonical_url"])].append(q)

    def quellen_id(raw_url: str) -> tuple[str, str]:
        """Rules I1 (same normalised URL) and I2 (same URL once hyphens are ignored)."""
        for tier, index, key in (("exact", q_exact, match_key(raw_url)),
                                 ("loose", q_loose, loose_key(raw_url))):
            cands = index.get(key)
            if not cands:
                continue
            cands = sorted(cands, key=lambda q: (q["canonical_url"] != raw_url,
                                                 q["canonical_url"].endswith('"'), q["_i"]))
            notes = []
            if tier == "loose":
                notes.append("quellenliste id matched ignoring hyphens (its URL reads "
                             f"{cands[0]['canonical_url']})")
            if len(cands) > 1:
                notes.append("the quellenliste holds further ids for this URL: "
                             + ", ".join(c["source_id"] for c in cands[1:]))
            return cands[0]["source_id"], "; ".join(notes)
        return "", ""

    # ---- URLs that the manifest itself names (rule M1)
    manifest_urls: dict[str, list[str]] = defaultdict(list)
    for mid, m in manifest.items():
        for u in re.findall(r"https?://[^\s;,]+", m["publisher"] + " " + m["notes"]):
            manifest_urls[match_key(u)].append(mid)

    out: list[dict] = []
    slug_counter: Counter = Counter()

    def blank() -> dict:
        rec = {c: "" for c in OUT_COLS}
        rec.update(_notes=[], _rep=[], _basis={}, _mids=[], _fmt_cat="", _target=None)
        return rec

    # ---- 1. catalogue rows: clean, repair, normalise, identify
    for r in cat:
        j = judge_by_row.get(r["#"], {})
        rec = blank()
        rec.update(orig_row=r["#"], added="no", url_original=r["URL"], _j=j, _cat=r)
        name, desc = r["Source Name"], r["Description"]
        raw_url = j.get("url") or r["URL"]
        rec["url"], changes = normalise_url(raw_url)
        rec["host"] = host_of(rec["url"])
        url_parts = bool(j.get("url"))       # P8: Description / Format held the rest of the URL
        if url_parts:
            rec["_rep"].append(f"P8 URL was split across columns, rejoined ({j.get('name_basis') or 'judgement'})")
        changes = [c for c in changes if c != "trailing slash added to bare host"]
        if changes:
            rec["_rep"].append("U URL normalised: " + ", ".join(changes))
        if "{" in rec["url"]:
            rec["_rep"].append("P9 URL is a template with a placeholder (flagged, cannot be repaired)")

        if DESC_RESIDUE.match(desc) or (url_parts and desc):
            rec["_rep"].append(f"P1 description was parser residue ({desc[:40]!r}), dropped")
            desc = ""
        elif desc and desc == name:
            rec["_rep"].append("P7 description only repeated the name, dropped")
            desc = ""

        flaw = ""
        if name.lower() in (rec["host"], "www." + rec["host"], urlsplit(r["URL"]).netloc.lower()):
            flaw = "P2 name was the host"
        elif name.lower() in GENERIC_NAMES or (name and name == r["Section"]):
            flaw = "P3 name was a section label"
        elif name.startswith(("URL", "@")) or "http" in name:
            flaw = "P4 name was markup residue"
        elif name.startswith("#"):
            flaw = "P4 name carried a markdown '#'"
            if not j.get("source_name"):
                rec["_rep"].append(f"{flaw} ({name!r}), stripped")
                name, flaw = name.lstrip("# ").strip(), ""
        if j.get("source_name"):
            rec["_rep"].append(f"{flaw or 'name clarified'} ({name[:40]!r}), replaced "
                               f"({j.get('name_basis') or 'judgement'})")
            name = j["source_name"]
        elif flaw:
            rec["_rep"].append(f"{flaw} ({name[:40]!r}), NOT repaired")
        rec["source_name"] = name

        fmt_cat, fmt_residue = map_catalogue_format(r["Format"])
        moved = []
        if fmt_residue and url_parts:
            rec["_rep"].append(f"P5 Format cell held a piece of the URL ({r['Format'][:30]!r}), dropped")
        elif fmt_residue:
            rec["_rep"].append("P5 Format cell held other text, moved to notes")
            moved.append(r["Format"])
        rec["_fmt_cat"] = fmt_cat

        # licence, rules L1-L4: copied, never inferred
        lic_cell, lic_kind = r["License"], ""
        if lic_cell:
            if LICENCE_NAME.search(lic_cell) and len(lic_cell) <= 30:
                lic_kind = "name"
            elif ACCESS_TERM.search(lic_cell) and len(lic_cell) <= 30:
                lic_kind = "access"
            else:
                rec["_rep"].append("P6 License cell held other text, moved to notes")
                if lic_cell not in moved:
                    moved.append(lic_cell)
        m_desc = LICENCE_NAME.search(desc.split(" - ", 1)[1]) if " - " in desc else None
        if j.get("licence"):
            rec["licence"] = j["licence"]
            rec["licence_basis"] = ("source page (quote in notes)" if lic_kind != "name" else
                                    f"catalogue License column ({lic_cell!r}), read again on the source page (quote in notes)")
        elif lic_kind == "name":
            rec["licence"], rec["licence_basis"] = lic_cell, "catalogue License column (LLM-stated, unverified)"
        elif m_desc:
            rec["licence"] = m_desc.group(1)
            rec["licence_basis"] = "catalogue Description column (LLM-stated, unverified)"
        elif lic_kind == "access":
            rec["licence"] = "none recorded"
            rec["licence_basis"] = f"catalogue says {lic_cell!r}: an access term, not a licence"
        else:
            rec["licence"], rec["licence_basis"] = "none recorded", "nothing stated"

        origin = f"origin: {r['Source File']} > {r['Category']}" + (f" > {r['Section']}" if r["Section"] else "")
        rec["_notes"].append(origin + (f"; relevance {r['Relevance']}" if r["Relevance"] else ""))
        text = "; ".join(x for x in [desc] + moved if x)
        if text:
            rec["_notes"].append("desc: " + text)
        if urlsplit(rec["url"]).path in ("", "/") and not urlsplit(rec["url"]).query:
            rec["_notes"].append("URL is a site root, not a specific document or dataset")

        sid, qnote = quellen_id(raw_url)
        if not sid:
            base = slug_host(rec["url"])
            slug_counter[base] += 1
            sid = f"cat-{base}-{slug_counter[base]:02d}"
        rec["source_id"] = sid
        if qnote:
            rec["_notes"].append(qnote)
        for mid in manifest_urls.get(match_key(rec["url"]), []):
            rec["_mids"].append(mid)
            rec["_basis"]["manifest_id"] = "rule M1 (URL named in the manifest)"
        out.append(rec)

    # ---- 2. added rows (known missing sources, listed in prefill_judgements.csv)
    for j in added_specs:
        rec = blank()
        rec.update(added="yes", _j=j, _cat=None, source_id=j["source_id"])
        rec["url"], _ = normalise_url(j.get("url", ""))
        rec["host"] = host_of(rec["url"]) if rec["url"] else ""
        qid, qnote = quellen_id(rec["url"]) if rec["url"] else ("", "")
        expect = qid or rf"cat-{re.escape(slug_host(rec['url']) if rec['url'] else 'local')}-\d\d"
        if not re.fullmatch(expect, rec["source_id"]):
            sys.exit(f"added row {rec['source_id']}: the id does not follow the scheme (expected {expect})")
        rec["source_name"] = j.get("source_name", "")
        rec["_notes"].append("added because: " + (j.get("added_reason") or "(no reason given)"))
        if qnote:
            rec["_notes"].append(qnote)
        if j.get("licence"):
            rec["licence"], rec["licence_basis"] = j["licence"], "source page (quote in notes)"
        else:
            rec["licence"], rec["licence_basis"] = "none recorded", "nothing stated"
        out.append(rec)

    # ---- 3. duplicates: D1 same normalised URL (rule), D2 same source (judgement)
    by_orig = {r["orig_row"]: r for r in out if r["orig_row"]}
    by_sid: dict[str, dict] = {}
    for r in out:
        by_sid.setdefault(r["source_id"], r)
    first_by_key: dict[str, dict] = {}
    for rec in out:
        ref = rec["_j"].get("duplicate_of")
        if ref:
            target = by_orig.get(ref) or by_sid.get(ref)
            if target is None or target is rec:
                sys.exit(f"duplicate_of {ref!r} (row {rec['orig_row'] or rec['source_id']}) not found")
            rec["_target"] = target
            rec["_basis"]["duplicate_of"] = "judgement D2 (same source under another URL)"
            rec["_dup_note"] = (f"same source as row {target['orig_row']}" if target["orig_row"]
                                else f"same source as added row {target['source_id']}")
        elif rec["url"]:
            k = match_key(rec["url"])
            if k in first_by_key:
                rec["_target"] = first_by_key[k]
                rec["_basis"]["duplicate_of"] = "rule D1 (same normalised URL)"
                rec["_dup_note"] = f"same URL as row {first_by_key[k]['orig_row'] or first_by_key[k]['source_id']}"
            else:
                first_by_key[k] = rec
    for rec in out:
        t = rec["_target"]
        if t is not None:
            if t["_target"] is not None:
                sys.exit(f"duplicate chain at row {rec['orig_row']}: point it at the canonical row")
            rec["duplicate_of"] = t["source_id"]
    for sid, n in Counter(r["source_id"] for r in out).items():
        if n > 1 and sum(1 for r in out if r["source_id"] == sid and not r["duplicate_of"]) != 1:
            sys.exit(f"source_id {sid} is shared by rows that are not marked as duplicates")

    # ---- 4. manifest links, publisher, format (first part), classes; canonical rows first
    linked: set[str] = set()
    for rec in sorted(out, key=lambda r: r["_target"] is not None):
        j, basis, target = rec["_j"], rec["_basis"], rec["_target"]
        for mid in filter(None, (x.strip() for x in j.get("manifest_id", "").split(";"))):
            if mid not in manifest:
                sys.exit(f"{args.judgements.name}: unknown manifest id {mid!r}")
            if mid not in rec["_mids"]:
                rec["_mids"].append(mid)
                basis["manifest_id"] = "judgement"
        linked.update(rec["_mids"])
        rec["manifest_id"] = "; ".join(rec["_mids"])
        ms = [manifest[m] for m in rec["_mids"]]
        own = rec["added"] == "yes" and bool(ms)          # the row IS the manifest source
        rec["_own"] = own
        rule = HOST_RULES.get(rec["host"], {})

        if own:
            if not rec["source_name"]:
                rec["source_name"], basis["source_name"] = ms[0]["title"], "manifest"
            lic = list(dict.fromkeys(m["licence"] for m in ms if m["licence"]))
            if lic and not j.get("licence"):
                rec["licence"], rec["licence_basis"] = " / ".join(lic), "manifest"
        elif ms and rec["licence_basis"] == "nothing stated":
            lic = list(dict.fromkeys(m["licence"] for m in ms if m["path"] and m["licence"]
                                     and not re.match(r"(not|none) recorded", m["licence"])))
            if lic:
                rec["licence"], rec["licence_basis"] = " / ".join(lic), "manifest (for the local copy)"

        if j.get("publisher"):
            rec["publisher"], basis["publisher"] = j["publisher"], "judgement"
        elif own:
            rec["publisher"], basis["publisher"] = ms[0]["publisher"], "manifest"
        elif rule.get("publisher"):
            rec["publisher"], basis["publisher"] = rule["publisher"], f"rule H (host {rec['host']})"
        elif rec["host"] in GITHUB_HOSTS and len(urlsplit(rec["url"]).path.split("/")) > 2:
            rec["publisher"] = "GitHub account " + urlsplit(rec["url"]).path.split("/")[1]
            basis["publisher"] = "rule H (GitHub owner from the URL)"
        elif target is not None and target["publisher"] != "unclear":
            rec["publisher"], basis["publisher"] = target["publisher"], "inherited from the canonical row"
        else:
            rec["publisher"], basis["publisher"] = "unclear", "no basis"

        ext = re.search(r"(\.[A-Za-z0-9]{2,7})$", urlsplit(rec["url"]).path)
        ext_fmt = EXT_FORMAT.get(ext.group(1).lower()) if ext else None
        if j.get("format"):
            rec["format"], basis["format"] = j["format"], "judgement"
        elif own:
            found = {MANIFEST_FORMAT.get(m["format"], "other") for m in ms}
            rec["format"], basis["format"] = "; ".join(f for f in FORMATS if f in found), "manifest"
            odd = sorted({m["format"] for m in ms
                          if MANIFEST_FORMAT.get(m["format"], "other") in ("other", "dataset download")})
            if odd:
                rec["_notes"].append("file format per manifest: " + ", ".join(odd))
        elif ext_fmt:
            rec["format"], basis["format"] = ext_fmt, "rule F1 (URL extension)"
            if rec["_fmt_cat"] and ext_fmt not in rec["_fmt_cat"]:
                rec["_notes"].append(f"the catalogue Format cell said {rec['_cat']['Format']!r}")
        elif rec["_fmt_cat"]:
            rec["format"], basis["format"] = rec["_fmt_cat"], "catalogue"

        for col in ("modality_stratum", "authority_class", "jurisdiction"):
            if j.get(col):
                rec[col], basis[col] = j[col], "judgement"
            elif col == "modality_stratum" and own and MANIFEST_STRATUM.get(ms[0]["stratum"]):
                rec[col], basis[col] = MANIFEST_STRATUM[ms[0]["stratum"]], "rule (manifest stratum)"
            elif rule.get(col):
                rec[col], basis[col] = rule[col], f"rule H (host {rec['host']})"
            elif target is not None:
                rec[col], basis[col] = target[col], "inherited from the canonical row"
            else:
                rec[col], basis[col] = "unclear", "no basis"
        if j.get("population_screen"):
            rec["population_screen"], basis["population_screen"] = j["population_screen"], "judgement"
            rec["screen_reason"] = j.get("screen_reason", "")
        elif target is not None:
            rec["population_screen"], rec["screen_reason"] = target["population_screen"], target["screen_reason"]
            basis["population_screen"] = "inherited from the canonical row"
        else:
            rec["population_screen"], basis["population_screen"] = "unclear", "no basis"
            rec["screen_reason"] = "not screened"

        if j.get("licence_quote"):
            rec["_notes"].append("licence quote: " + j["licence_quote"])
        if j.get("note"):
            rec["_notes"].append("note: " + j["note"])
        if own:
            rec["_notes"].append("manifest: " + "; ".join(
                f"{m['id']} = {m['path'] or 'no file, fetched at run time'}" for m in ms))
            for label, field in (("manifest notes", "notes"), ("manifest stratum", "stratum"),
                                 ("manifest status", "status")):
                extra = "; ".join(dict.fromkeys(m[field] for m in ms if m[field]))
                if extra:
                    rec["_notes"].append(f"{label}: {extra}")
        elif ms:
            rec["_notes"].append("local copy in the manifest: " + ", ".join(
                f"{m['id']} ({m['path'] or 'no file, fetched at run time'})" for m in ms))
        if target is not None:
            rec["_notes"].append("duplicate: " + rec["_dup_note"])
    missing = sorted(set(manifest) - linked)
    if missing:
        sys.exit("manifest ids without a row in the output: " + ", ".join(missing))

    # ---- 5. access
    urls = list(dict.fromkeys(r["url"] for r in out if r["url"]))
    prev_url, prev_row = load_previous(args.out)
    if args.check_access:
        access = run_access_check(urls, bool(args.titles_out))
        if args.titles_out:
            with open(args.titles_out, "w", encoding="utf-8", newline="") as fh:
                w = csv.writer(fh)
                w.writerow(["url", "label", "type", "final", "title"])
                for u, a in access.items():
                    w.writerow([u, a["label"], a["type"], a["final"], a["title"]])
    else:
        access = prev_url
    for rec in out:
        a = access.get(rec["url"]) if rec["url"] else None
        files = [manifest[m] for m in rec["_mids"] if manifest[m]["path"]]
        row_key = rec["orig_row"] or rec["source_id"]
        if args.check_access:
            gone = [m["id"] for m in files if not (repo_root / m["path"]).exists()]
            if files and not gone:
                rec["access_status"] = "on disk"
            elif a:
                rec["access_status"] = a["label"]
            else:
                rec["access_status"] = "not found"
            if gone:
                rec["_notes"].append("manifest file not found on this machine: " + ", ".join(gone))
            rec["checked_on"] = args.today
        elif row_key in prev_row and prev_row[row_key][0]:
            rec["access_status"], rec["checked_on"] = prev_row[row_key]
        else:
            rec["access_status"] = "on disk" if files else "not checked"
        if a:
            rec["_notes"].append(access_segment(a))
            if not rec["format"]:
                ct = CTYPE_FORMAT.get(a["type"])
                if ct and ct != "HTML" and a["label"].startswith(("ok", "redirect")):
                    rec["format"], rec["_basis"]["format"] = ct, "rule F3 (observed content type)"
        if not rec["format"]:
            parts = urlsplit(rec["url"])
            if rec["url"] and API_URL.search(rec["host"] + parts.path + ("?" if parts.query else "")):
                rec["format"], rec["_basis"]["format"] = "API", "rule F4 (API-like URL)"
            elif a and CTYPE_FORMAT.get(a["type"]) == "HTML" and a["label"].startswith(("ok", "redirect")):
                rec["format"], rec["_basis"]["format"] = "HTML", "rule F5 (the URL serves HTML)"
            else:
                rec["format"], rec["_basis"]["format"] = "unclear", "no basis"

    # ---- 6. finish
    order = ["source_name", "format", "publisher", "modality_stratum", "authority_class", "jurisdiction",
             "population_screen", "manifest_id", "duplicate_of"]
    for rec in out:
        rec["repaired"] = "; ".join(rec["_rep"])
        rec["prefill_basis"] = "; ".join(f"{k}={rec['_basis'][k]}" for k in order if k in rec["_basis"])
        rec["notes"] = " | ".join(seg(n) for n in rec["_notes"] if n)
        rec["verified_by_author"] = ""
    return out


# --------------------------------------------------------------------------- stats

def md_table(header: list[str], rows: list[list]) -> str:
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    lines += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(lines)


def stats(out: list[dict]) -> str:
    """Count tables for CLEANING_REPORT.md (markdown on stdout)."""
    cat = [r for r in out if r["added"] == "no"]
    added = [r for r in out if r["added"] == "yes"]
    distinct = [r for r in out if not r["duplicate_of"]]
    scope = [r for r in distinct if r["population_screen"] in ("in", "contrast")]
    border = [r for r in distinct if r["screen_reason"].startswith("borderline")]
    key = lambda r: r["orig_row"] or "added"
    p = ["### Counts\n"]
    p.append(md_table(["Measure", "Count"], [
        ["Catalogue rows in", len(cat)],
        ["Rows added", len(added)],
        ["Rows out", len(out)],
        ["Marked as duplicate", sum(1 for r in out if r["duplicate_of"])],
        ["... by rule D1 (same normalised URL)", sum(1 for r in out if "rule D1" in r["prefill_basis"])],
        ["... by judgement D2 (same source under another URL)", sum(1 for r in out if "judgement D2" in r["prefill_basis"])],
        ["Distinct sources (rows without duplicate_of)", len(distinct)],
        ["... from the catalogue", sum(1 for r in distinct if r["added"] == "no")],
        ["... added", sum(1 for r in distinct if r["added"] == "yes")],
        ["Catalogue rows with at least one repair", sum(1 for r in cat if r["repaired"])],
        ["Catalogue rows with a parse artefact (rules P1 to P9)", sum(1 for r in cat if re.search(r"\bP\d ", r["repaired"]))],
        ["Catalogue rows whose URL is only a site root", sum(1 for r in cat if "URL is a site root" in r["notes"])],
        ["Rows with a quellenliste id", sum(1 for r in out if not r["source_id"].startswith("cat-"))],
        ["... matched only when hyphens are ignored", sum(1 for r in out if "matched ignoring hyphens" in r["notes"])],
        ["Rows with a new cat- id", sum(1 for r in out if r["source_id"].startswith("cat-"))],
        ["Rows linked to the manifest", sum(1 for r in out if r["manifest_id"])],
    ]))
    p.append("\n### Population screen (distinct sources)\n")
    p.append(md_table(["screen", "sources", "of these marked borderline"], [
        [sc, sum(1 for r in distinct if r["population_screen"] == sc),
         sum(1 for r in border if r["population_screen"] == sc)] for sc in SCREENS]))
    reps = Counter()
    for r in cat:
        for code in set(re.findall(r"\b(P\d|U) ", r["repaired"])):
            reps[code] += 1
    p.append("\n### Repairs by rule (catalogue rows; a row can carry several)\n")
    p.append(md_table(["Rule", "Rows"], sorted(reps.items())))

    def cross(rows: list[dict]) -> str:
        jur = [j for j in JURISDICTIONS
               if j not in ("other municipality or state", "unclear") or any(r["jurisdiction"] == j for r in rows)]
        body = [[m] + [sum(1 for r in rows if r["modality_stratum"] == m and r["jurisdiction"] == j) for j in jur]
                + [sum(1 for r in rows if r["modality_stratum"] == m)] for m in MODALITIES]
        body.append(["total"] + [sum(1 for r in rows if r["jurisdiction"] == j) for j in jur] + [len(rows)])
        return md_table(["modality stratum"] + jur + ["total"], body)

    p.append("\n### Modality stratum by jurisdiction (distinct sources screened in or contrast)\n")
    p.append(cross(scope))
    p.append("\n### The same without the sources marked borderline\n")
    p.append(cross([r for r in scope if not r["screen_reason"].startswith("borderline")]))
    p.append("\n### The same, only sources that are on disk\n")
    p.append(cross([r for r in scope if r["access_status"] == "on disk"]))
    p.append("\n### Numeric tabular sources, all screens (distinct)\n")
    num = [r for r in distinct if r["modality_stratum"] == "numeric tabular"]
    p.append(md_table(["row", "source_id", "name", "jurisdiction", "screen", "access"], [
        [key(r), r["source_id"], r["source_name"][:60], r["jurisdiction"], r["population_screen"],
         r["access_status"]] for r in num]) if num else "none")
    p.append("\n### Authority class (distinct sources)\n")
    p.append(md_table(["class", "all", "in or contrast"], [
        [a, sum(1 for r in distinct if r["authority_class"] == a),
         sum(1 for r in scope if r["authority_class"] == a)] for a in AUTHORITIES]))
    p.append("\n### Access status (all rows)\n")
    acc = Counter(re.sub(r"^redirect -> .*", "redirect -> <other host>", r["access_status"]) for r in out)
    p.append(md_table(["status", "rows", "of these in or contrast (distinct)"], [
        [k, v, sum(1 for r in scope if re.sub(r"^redirect -> .*", "redirect -> <other host>", r["access_status"]) == k)]
        for k, v in sorted(acc.items(), key=lambda kv: -kv[1])]))
    p.append("\n### Licence basis (all rows)\n")
    lb = Counter(re.sub(r"catalogue says .*", "catalogue gives an access term only ('Public', 'Free' ...)",
                        re.sub(r"catalogue License column \('.*", "catalogue License column, read again on the source page",
                               r["licence_basis"])) for r in out)
    p.append(md_table(["basis", "rows"], sorted(lb.items(), key=lambda kv: -kv[1])))

    def why(r: dict) -> str:
        bits = re.findall(r"(?:modality|authority class|jurisdiction) unclear: [^.|]*", r["notes"])
        if r["population_screen"] == "unclear":
            bits.append("screen: " + r["screen_reason"])
        return "; ".join(b.strip() for b in bits)

    unc = [r for r in distinct if "unclear" in (r["modality_stratum"], r["authority_class"],
                                                r["jurisdiction"], r["population_screen"])]
    main_unc = [r for r in unc if r["population_screen"] != "out"]
    p.append("\n### Could not be classified, and not screened out (distinct sources)\n")
    p.append(md_table(["row", "source_id", "name", "modality", "class", "jurisdiction", "screen", "what is open"], [
        [key(r), r["source_id"], r["source_name"][:45], r["modality_stratum"], r["authority_class"],
         r["jurisdiction"], r["population_screen"], why(r)] for r in main_unc]) if main_unc else "none")
    p.append("\n### Screened out, with one column left `unclear` (distinct sources)\n")
    groups: dict[str, list[str]] = defaultdict(list)
    for r in unc:
        if r["population_screen"] == "out":
            w = why(r)
            g = ("modality: no stratum fits (" + re.search(r"no stratum fits \(([^)]*)\)", w).group(1) + ")"
                 if "no stratum fits (" in w else
                 "modality: the URL is only a site root" if "only a site root" in w else w or "see notes")
            groups[g].append(key(r))
    p.append(md_table(["reason", "rows", "orig_row"], [[g, len(v), ", ".join(v)] for g, v in
                                                       sorted(groups.items(), key=lambda kv: -len(kv[1]))]))
    p.append("\n### Dead links (error 404/410, other 4xx/5xx except 403, unreachable)\n")
    dead = [r for r in out if r["access_status"].startswith(("error", "unreachable", "not found"))
            and not r["access_status"].startswith("error 403") and "URL template" not in r["notes"]]
    p.append(md_table(["row", "source_id", "status", "screen", "url"], [
        [key(r), r["source_id"], r["access_status"], r["population_screen"], r["url"][:105]] for r in dead])
        if dead else "none")
    p.append("\n### URL templates that cannot be requested\n")
    tpl = [r for r in out if "URL template" in r["notes"]]
    p.append(md_table(["row", "source_id", "url"], [[key(r), r["source_id"], r["url"][:105]] for r in tpl]) if tpl else "none")
    p.append("\n### Answered 403 (often bot protection; open in a browser before calling them dead)\n")
    f403 = [r for r in out if r["access_status"].startswith("error 403")]
    p.append(md_table(["row", "source_id", "screen", "url"], [
        [key(r), r["source_id"], r["population_screen"], r["url"][:105]] for r in f403]) if f403 else "none")
    p.append("\n### Redirects to another host, logins, and deep links that landed on a site root\n")
    red = [r for r in out if r["access_status"].startswith(("redirect", "needs login"))
           or "possible soft 404" in r["notes"]]
    p.append(md_table(["row", "source_id", "status", "url"], [
        [key(r), r["source_id"], r["access_status"] + (" (landed on the site root)" if "possible soft 404" in r["notes"] else ""),
         r["url"][:105]] for r in red]) if red else "none")
    p.append("\n### Duplicates (row -> canonical row)\n")
    by_id = {}
    for r in out:
        if not r["duplicate_of"]:
            by_id[r["source_id"]] = r
    dups = [r for r in out if r["duplicate_of"]]
    p.append(md_table(["row", "name", "canonical", "canonical name", "basis"], [
        [key(r), r["source_name"][:48], by_id[r["duplicate_of"]]["orig_row"] or r["duplicate_of"],
         by_id[r["duplicate_of"]]["source_name"][:48], "D1" if "rule D1" in r["prefill_basis"] else "D2"]
        for r in dups]))
    p.append("\n### Manifest ids and the rows that carry them\n")
    mrows = defaultdict(list)
    for r in out:
        for m in filter(None, (x.strip() for x in r["manifest_id"].split(";"))):
            mrows[m].append(f"{r['source_id']} ({'row ' + r['orig_row'] if r['orig_row'] else 'added'}"
                            f"{', duplicate' if r['duplicate_of'] else ''})")
    p.append(md_table(["manifest id", "rows"], [[m, "; ".join(v)] for m, v in mrows.items()]))
    return "\n".join(p)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check-access", action="store_true", help="re-check every URL (network)")
    ap.add_argument("--stats", action="store_true", help="print the report tables")
    ap.add_argument("--catalogue", type=Path, default=HERE / "sources_catalog.csv")
    ap.add_argument("--quellenliste", type=Path, default=HERE / "quellenliste" / "research_link_sources_unique.csv")
    ap.add_argument("--manifest", type=Path, default=HERE.parent / "manifest.csv")
    ap.add_argument("--judgements", type=Path, default=HERE / "prefill_judgements.csv")
    ap.add_argument("--out", type=Path, default=HERE / "sources_catalog_clean.csv")
    ap.add_argument("--today", default=datetime.date.today().isoformat(), help=argparse.SUPPRESS)
    ap.add_argument("--titles-out", default="", help=argparse.SUPPRESS)
    ap.add_argument("--no-write", action="store_true", help=argparse.SUPPRESS)
    args = ap.parse_args()

    out = build(args)
    if not args.no_write:
        with args.out.open("w", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=OUT_COLS, extrasaction="ignore", lineterminator="\n")
            w.writeheader()
            w.writerows(out)
        print(f"wrote {args.out} ({len(out)} rows)", file=sys.stderr)
    if args.stats:
        print(stats(out))


if __name__ == "__main__":
    main()
