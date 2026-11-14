#!/usr/bin/env python3
"""Cut the source sample into self-contained units and write a draft manifest (EX-05).

Usage (from the repository root):
    .venv/bin/python experiments/sample/cut_units.py            # cut, select, write out/
    .venv/bin/python experiments/sample/cut_units.py --check    # re-cut and compare with out/manifest_draft.csv

Reads   sources/manifest.csv (file identity, checksums) and experiments/sample/sample_rules.yaml (all choices).
Writes  experiments/sample/out/units/<unit_id>.txt   one file per unit, exactly what an agent is handed
        experiments/sample/out/manifest_draft.csv    one row per unit
Everything is deterministic: same sources and same rules give byte-identical units and manifest.
Nothing here is frozen; the freeze is a tagged commit with the manifest hash.
"""
import csv, hashlib, io, json, random, re, subprocess, sys, zipfile
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
OUT = HERE / "out"
RULES = yaml.safe_load((HERE / "sample_rules.yaml").read_text(encoding="utf-8"))
MANIFEST = {r["id"]: r for r in csv.DictReader((ROOT / "sources" / "manifest.csv").open(encoding="utf-8"))}
SEED = RULES["seed"]


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def clean_text(s: str) -> str:
    """LF line ends, no trailing blanks, at most one empty line in a row, one final newline."""
    lines = [ln.rstrip() for ln in s.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    out, blank = [], 0
    for ln in lines:
        blank = blank + 1 if ln == "" else 0
        if blank <= 1:
            out.append(ln)
    return "\n".join(out).strip("\n") + "\n"


def read_text(path: Path) -> str:
    raw = path.read_bytes()
    for enc in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"cannot decode {path}")


# ---------------------------------------------------------------- readers: each yields (locator, part, content)

def csv_rows(path, src):
    delim, n = src["delimiter"], RULES["chunk"]["rows_per_unit"]
    records = [r for r in csv.reader(io.StringIO(read_text(path)), delimiter=delim) if any(c.strip() for c in r)]
    header, data = records[0], records[1:]
    for i in range(0, len(data), n):
        seg = data[i:i + n]
        buf = io.StringIO()
        w = csv.writer(buf, delimiter=delim, lineterminator="\n")
        w.writerow(header)
        w.writerows(seg)
        yield (f"rows {i + 1}-{i + len(seg)}", f"Zeilen {i + 1} bis {i + len(seg)} von {len(data)}", buf.getvalue())


def json_facilities(path, src):
    """Distinct facility records in first-seen order, a fixed number per unit, as a JSON list."""
    n = RULES["chunk"]["records_per_unit"]
    data = json.loads(read_text(path))
    seen, recs = set(), []
    for key in data:                      # insertion order of the file
        for rec in data[key]:
            k = json.dumps(rec, ensure_ascii=False, sort_keys=True)
            if k not in seen:
                seen.add(k)
                recs.append(rec)
    for i in range(0, len(recs), n):
        seg = recs[i:i + n]
        yield (f"records {i + 1}-{i + len(seg)}", f"Einträge {i + 1} bis {i + len(seg)} von {len(recs)}",
               json.dumps(seg, ensure_ascii=False, indent=1) + "\n")


def json_catalogue(path, src):
    """One catalogue entry per unit; a seeded draw of eligible entries, capped."""
    data = json.loads(read_text(path))
    need = src.get("eligible", [])
    idx = [i for i, e in enumerate(data) if all(str(e.get(f) or "").strip() for f in need)]
    rng = random.Random(f"{SEED}:{src['id']}:universe")
    cap = RULES["chunk"]["catalogue_universe_cap"]
    for i in sorted(rng.sample(idx, min(cap, len(idx)))):
        e = data[i]
        cat = str(e.get(src.get("category_field", "")) or "").strip()
        yield (f"entry {i} (barcode {e.get('barcode') or 'none'})", cat or "ohne Kategorie",
               json.dumps(e, ensure_ascii=False, indent=1) + "\n")


def docx_document(path, src):
    xml = zipfile.ZipFile(path).read("word/document.xml").decode("utf-8")
    paras = ["".join(re.findall(r"<w:t[^>]*>([^<]*)</w:t>", p)) for p in re.findall(r"<w:p[ >].*?</w:p>", xml, flags=re.S)]
    text = "\n".join(p for p in paras if p.strip())
    for a, b in (("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"), ("&quot;", '"'), ("&apos;", "'")):
        text = text.replace(a, b)
    yield ("document", "ganzes Dokument", text)


def _pdftotext(path, first=None, last=None):
    cmd = ["pdftotext", "-layout", "-enc", "UTF-8"]
    if first:
        cmd += ["-f", str(first), "-l", str(last or first)]
    return subprocess.run(cmd + [str(path), "-"], check=True, capture_output=True).stdout.decode("utf-8")


def pdf_pages(path, src):
    pages = _pdftotext(path).split("\f")
    for n, page in enumerate(pages, 1):
        if len(page.strip()) >= 200:      # skip empty and cover pages; the manifest shows which pages exist
            yield (f"page {n}", f"Seite {n}", page)


NUM = r"(\d+)\s?([a-z]?)"
HEAD_ALONE = re.compile(rf"^\s*§\s*{NUM}\s*\*?\s*$")            # "§ 8" alone on a line, title on the next
HEAD_INLINE = re.compile(rf"^\s*§\s*{NUM}\s+(?!Abs\b|Absatz\b|Satz\b|Nr\b|Nummer\b|Ziffer\b|bis\b|und\b|oder\b)"
                         r"([A-ZÄÖÜ]\S.*)$")                    # "§ 8 Abfallbehälter" on one line


def statute_sections(lines):
    """Find the section headings of a statute in extracted text. Returns (index of first body heading, list of
    (line index, number, title)). Rules: if at least three headings stand alone on a line, that style is used,
    else the one-line style (title starts with a capital, is at most 90 characters and does not end like a
    sentence). The body starts at the LAST heading numbered 1, which skips a table of contents; from there a
    heading is accepted only if its number follows the previous one (same number with a later letter, or up
    to three higher), which rejects references that begin a line."""
    alone = [(i, HEAD_ALONE.match(ln)) for i, ln in enumerate(lines) if HEAD_ALONE.match(ln)]
    cands = []
    if len(alone) >= 3:
        for i, m in alone:
            rest = [ln for ln in lines[i + 1:i + 6] if ln.strip()]
            cands.append((i, int(m.group(1)), m.group(2), " ".join(rest[0].split()) if rest else ""))
    else:
        for i, ln in enumerate(lines):
            m = HEAD_INLINE.match(ln)
            if m and len(ln.strip()) <= 90 and not ln.rstrip().endswith((".", ",", ";")):
                cands.append((i, int(m.group(1)), m.group(2), " ".join(m.group(3).split())))
    if not cands:
        raise ValueError("no § headings found")
    ones = [k for k, c in enumerate(cands) if c[1] == 1 and c[2] == ""]
    start = ones[-1] if ones else 0
    out, last = [], (0, "")
    for i, num, suf, title in cands[start:]:
        if (num, suf) > last and num - last[0] <= 3:
            out.append((i, f"{num}{suf}", title))
            last = (num, suf)
    return out


def pdf_statute(path, src):
    """One unit per section (§). The text before the table of contents is its own unit."""
    lines = _pdftotext(path).replace("\f", "\n").split("\n")
    if src.get("drop_lines"):
        drop = re.compile(src["drop_lines"])
        lines = [ln for ln in lines if not drop.match(ln)]
    heads = statute_sections(lines)
    first_mark = next(i for i, ln in enumerate(lines) if HEAD_ALONE.match(ln) or HEAD_INLINE.match(ln))
    pre = "\n".join(lines[:first_mark])
    if len(pre.strip()) >= 200:
        yield ("preamble", "Eingangsformel", pre)
    for (a, num, title), nxt in zip(heads, heads[1:] + [(len(lines), "", "")]):
        yield (f"section {num}", f"§ {num} {title}".strip(), "\n".join(lines[a:nxt[0]]))


def _xml_text(x):
    """Plain text of a gesetze-im-internet content fragment: paragraphs and list items on their own lines."""
    import html
    x = re.sub(r"</(P|LA|DD|row|Title)>|<BR\s*/>", "\n", x)
    x = re.sub(r"</(DT|entry)>", " ", x)
    x = html.unescape(re.sub(r"<[^>]+>", "", x))
    return "\n".join(" ".join(ln.split()) for ln in x.split("\n"))


def gii_xml(path, src):
    """Federal law as published in XML: one unit per section or annex; an annex that is a table of more than
    30 rows is cut like a table (fixed number of rows per unit, first row repeated as header)."""
    xml = read_text(path)
    n_rows = RULES["chunk"]["rows_per_unit"]
    seen = {}
    for norm in re.findall(r"<norm .*?</norm>", xml, flags=re.S):
        en = re.search(r"<enbez>(.*?)</enbez>", norm, flags=re.S)
        body = re.search(r"<textdaten>.*?<text[^>]*>(.*?)</text>", norm, flags=re.S)
        if not en or not body or not body.group(1).strip():
            continue                                  # structure headings and norms without text
        name = " ".join(_xml_text(en.group(1)).split())
        ti = re.search(r"<titel[^>]*>(.*?)</titel>", norm, flags=re.S)
        title = " ".join(_xml_text(ti.group(1)).split()) if ti else ""
        seen[name] = seen.get(name, 0) + 1
        loc = name if seen[name] == 1 else f"{name} ({seen[name]})"
        rows = re.findall(r"<row[^>]*>(.*?)</row>", body.group(1), flags=re.S)
        if len(rows) > 30:
            cells = [[" ".join(_xml_text(c).split()) for c in re.findall(r"<entry[^>]*>(.*?)</entry>", r, flags=re.S)] for r in rows]
            header, data = cells[0], cells[1:]
            for i in range(0, len(data), n_rows):
                seg = data[i:i + n_rows]
                buf = io.StringIO()
                w = csv.writer(buf, delimiter=";", lineterminator="\n")
                w.writerow(header)
                w.writerows(seg)
                yield (f"{loc} rows {i + 1}-{i + len(seg)}", f"{name} {title}, Zeilen {i + 1} bis {i + len(seg)} von {len(data)}".strip(), buf.getvalue())
        else:
            yield (loc, f"{name} {title}".strip(), _xml_text(body.group(1)))


READERS = dict(csv_rows=csv_rows, json_facilities=json_facilities, json_catalogue=json_catalogue,
               docx_document=docx_document, pdf_pages=pdf_pages, pdf_statute=pdf_statute, gii_xml=gii_xml)
RULE_TEXT = dict(
    csv_rows="table segment: header row plus {rows_per_unit} data records in file order; empty records dropped",
    json_facilities="distinct records in first-seen order, {records_per_unit} per unit, as a JSON list",
    json_catalogue="one entry per unit; seeded draw of at most {catalogue_universe_cap} eligible entries",
    docx_document="whole document, paragraph text in document order",
    pdf_pages="one page per unit; pages with fewer than 200 characters skipped",
    pdf_statute="one section (§) per unit; text before the table of contents as its own unit; table of contents dropped",
    gii_xml="one section or annex per unit; an annex table of more than 30 rows is cut into segments of {rows_per_unit} rows with the header row",
)


def unit_text(src, part, content):
    head = [f"Dokument: {src['display_title']}"]
    if src.get("display_publisher"):
        head.append(f"Herausgeber: {src['display_publisher']}")
    if src.get("applies_to"):
        head.append(f"Gilt für: {src['applies_to']}")
    head.append(f"Teil: {part}")
    return "\n".join(head) + "\n---\n" + clean_text(content)


def cut():
    pdf_version = subprocess.run(["pdftotext", "-v"], capture_output=True).stderr.decode().split("\n")[0].strip()
    units = []
    for src in RULES["sources"]:
        if not src.get("include"):
            continue
        m = MANIFEST[src["id"]]
        path = ROOT / m["path"]
        if sha256_bytes(path.read_bytes()) != m["sha256"]:
            raise SystemExit(f"{src['id']}: file differs from sources/manifest.csv")
        rule = RULE_TEXT[src["reader"]].format(**RULES["chunk"])
        if src["reader"].startswith("pdf"):
            rule += f"; text by {RULES['pdf_extractor']} ({pdf_version})"
        for locator, part, content in READERS[src["reader"]](path, src):
            text = unit_text(src, part, content).encode("utf-8")
            uid = "u" + sha256_bytes(f"{src['id']}\x1f{locator}".encode())[:12]
            units.append(dict(unit_id=uid, unit_file=f"units/{uid}.txt", unit_sha256=sha256_bytes(text), unit_bytes=len(text),
                              source_id=src["id"], source_path=m["path"], source_sha256=m["sha256"], locator=locator,
                              stratum=next((v for k, v in (src.get("stratum_overrides") or {}).items() if locator.startswith(k)), src["stratum"]),
                              jurisdiction=src["jurisdiction"], authority_class=src["authority_class"],
                              bucket="candidate", _held_out_only=bool(src.get("held_out_only")), _pilot_allowed=bool(src.get("pilot_allowed")), chunk_rule=rule, selection_seed=SEED,
                              retrieval_date=f"not recorded; file date {m['file_date']}", rules_version=RULES["version"], _text=text))
    ids = [u["unit_id"] for u in units]
    assert len(ids) == len(set(ids)), "unit id collision"
    return units


def select(units):
    """Per stratum: shuffle each source's units with its own seeded generator, then deal round-robin over sources.
    Sources marked held_out_only (contrast cities, law reserved for held-out question families) are skipped.
    If a stratum then has fewer pilot units than asked for, the pilot is filled, in the same seeded round-robin,
    from held-out-only sources marked pilot_allowed. Those units never enter development."""
    nd, npi = RULES["buckets"]["development_per_stratum"], RULES["buckets"]["pilot_per_stratum"]
    for stratum in sorted({u["stratum"] for u in units}):
        by_src = {}
        for u in units:
            if u["stratum"] == stratum and not u["_held_out_only"]:     # held-out-only sources never enter development or pilot
                by_src.setdefault(u["source_id"], []).append(u)
        for sid in by_src:
            by_src[sid].sort(key=lambda u: u["unit_id"])
            random.Random(f"{SEED}:{sid}:select").shuffle(by_src[sid])
        order, queues = [], [by_src[s] for s in sorted(by_src)]
        while any(queues):
            for q in queues:
                if q:
                    order.append(q.pop(0))
        for k, u in enumerate(order):
            u["bucket"] = "development" if k < nd else "pilot" if k < nd + npi else "candidate"
        missing = npi - sum(1 for u in order if u["bucket"] == "pilot")
        if missing > 0:
            fill = {}
            for u in units:
                if u["stratum"] == stratum and u["_held_out_only"] and u["_pilot_allowed"]:
                    fill.setdefault(u["source_id"], []).append(u)
            for sid in fill:
                fill[sid].sort(key=lambda u: u["unit_id"])
                random.Random(f"{SEED}:{sid}:select").shuffle(fill[sid])
            queues = [fill[s] for s in sorted(fill)]
            while missing > 0 and any(queues):
                for q in queues:
                    if q and missing > 0:
                        q.pop(0)["bucket"] = "pilot"
                        missing -= 1


FIELDS = ["unit_id", "unit_file", "unit_sha256", "unit_bytes", "source_id", "source_path", "source_sha256", "locator",
          "stratum", "jurisdiction", "authority_class", "bucket", "chunk_rule", "selection_seed", "retrieval_date", "rules_version"]


def manifest_bytes(units):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=FIELDS, lineterminator="\n", extrasaction="ignore")
    w.writeheader()
    for u in sorted(units, key=lambda u: u["unit_id"]):
        w.writerow(u)
    return buf.getvalue().encode("utf-8")


def main():
    units = cut()
    select(units)
    mb = manifest_bytes(units)
    if "--check" in sys.argv:
        old = (OUT / "manifest_draft.csv").read_bytes()
        bad = [u["unit_id"] for u in units if (OUT / u["unit_file"]).read_bytes() != u["_text"]]
        print("manifest identical:", old == mb, "| unit files differing:", len(bad))
        sys.exit(0 if old == mb and not bad else 1)
    (OUT / "units").mkdir(parents=True, exist_ok=True)
    for old in (OUT / "units").glob("u*.txt"):
        old.unlink()
    for u in units:
        (OUT / u["unit_file"]).write_bytes(u["_text"])
    (OUT / "manifest_draft.csv").write_bytes(mb)
    print(f"{len(units)} units cut; manifest sha256 {sha256_bytes(mb)}")
    rows = {}
    for u in units:
        k = (u["stratum"], u["source_id"])
        rows.setdefault(k, {"n": 0, "development": 0, "pilot": 0, "bytes": []})
        rows[k]["n"] += 1
        rows[k]["bytes"].append(u["unit_bytes"])
        if u["bucket"] in rows[k]:
            rows[k][u["bucket"]] += 1
    print(f"{'stratum':20} {'source':28} {'units':>5} {'dev':>4} {'pilot':>5} {'median bytes':>12} {'max':>7}")
    for (st, sid), r in sorted(rows.items()):
        b = sorted(r["bytes"])
        print(f"{st:20} {sid:28} {r['n']:5} {r['development']:4} {r['pilot']:5} {b[len(b) // 2]:12} {b[-1]:7}")
    groups = {"Frankfurt": "Frankfurt and campus", "campus": "Frankfurt and campus", "Munich": "Munich", "Berlin": "Berlin"}
    bound = [u for u in units if u["jurisdiction"] in groups]
    print(f"\nShares among the {len(bound)} units tied to a city or the campus (target: Frankfurt and campus at least "
          f"{RULES['shares']['frankfurt_and_campus_min']:.0%}, each contrast city about {RULES['shares']['contrast_city_each']:.0%}):")
    for g in ("Frankfurt and campus", "Munich", "Berlin"):
        n = sum(1 for u in bound if groups[u["jurisdiction"]] == g)
        print(f"  {g:22} {n:5} {n / len(bound):6.1%}")
    other = {}
    for u in units:
        if u["jurisdiction"] not in groups:
            other[u["jurisdiction"]] = other.get(u["jurisdiction"], 0) + 1
    print("  own groups, outside the shares:", ", ".join(f"{k} {v}" for k, v in sorted(other.items())))


if __name__ == "__main__":
    main()
