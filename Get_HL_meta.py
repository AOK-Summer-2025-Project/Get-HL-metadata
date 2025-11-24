#!/usr/bin/env python3
import argparse
import csv
import re
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
import xmltodict

BASE_URL = "https://api.lib.harvard.edu/v2/items"
DEFAULT_SLEEP = 0.15  # be polite between page fetches

# ---------------- utils ----------------
def as_list(x: Any) -> List:
    if x is None:
        return []
    return x if isinstance(x, list) else [x]

def strip_ns(obj):
    """Strip XML namespace prefixes (mods:titleInfo -> titleInfo)."""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            nk = k.split(":", 1)[1] if isinstance(k, str) and ":" in k else k
            out[nk] = strip_ns(v)
        return out
    if isinstance(obj, list):
        return [strip_ns(x) for x in obj]
    return obj

def nget(d: Any, key: str) -> Any:
    """Namespace-agnostic get: exact key or suffix after ':'."""
    if not isinstance(d, dict):
        return None
    if key in d:
        return d[key]
    for k, v in d.items():
        if isinstance(k, str) and k.split(":")[-1] == key:
            return v
    return None

def get_text(node: Any, *path: Union[str, int]) -> Optional[str]:
    """Path lookup with namespace-insensitive keys and #text handling."""
    cur = node
    for key in path:
        if cur is None:
            return None
        if isinstance(key, int):
            if isinstance(cur, list) and 0 <= key < len(cur):
                cur = cur[key]
            else:
                return None
        else:
            if isinstance(cur, dict) and key in cur:
                cur = cur[key]
            else:
                if isinstance(cur, dict):
                    found = False
                    for k, v in cur.items():
                        if isinstance(k, str) and k.split(":")[-1] == key:
                            cur = v
                            found = True
                            break
                    if not found:
                        return None
                else:
                    return None
    if cur is None:
        return None
    if isinstance(cur, dict):
        if "#text" in cur and cur["#text"] is not None:
            return str(cur["#text"])
        if "text" in cur and cur["text"] is not None:
            return str(cur["text"])
        return None
    if isinstance(cur, (str, int, float)):
        return str(cur)
    return None

def join_clean(values: List[Optional[str]]) -> str:
    return "; ".join(dict.fromkeys([v.strip() for v in values if v and str(v).strip()]))

# ---------------- extractors ----------------
def extract_title(mods: Dict) -> str:
    titles = []
    for ti in as_list(nget(mods, "titleInfo")):
        t = get_text(ti, "title")
        non = get_text(ti, "nonSort")
        sub = get_text(ti, "subTitle")
        # Skip "alternative"/uniform etc. here; main titleInfo has no @type or is 'translated' only when that's the main.
        ttype = (ti.get("@type") or "").lower() if isinstance(ti, dict) else ""
        if t and ttype in ("", "translated"):  # keep main; allow translated when it appears as primary
            full = " ".join([x for x in [non, t] if x])  # nonSort before title (e.g., "The ")
            if sub:
                full = f"{full}: {sub}"
            titles.append(full)
    return titles[0] if titles else ""

def extract_variant_titles(mods: Dict) -> str:
    out = []
    for ti in as_list(nget(mods, "titleInfo")):
        ttype = (ti.get("@type") or "").lower() if isinstance(ti, dict) else ""
        if ttype in {"alternative", "translated", "uniform", "abbreviated"}:
            t = get_text(ti, "title")
            non = get_text(ti, "nonSort")
            sub = get_text(ti, "subTitle")
            if t or non or sub:
                full = " ".join([x for x in [non, t] if x])
                if sub:
                    full = f"{full}: {sub}"
                if full:
                    out.append(full)
    return join_clean(out)

def _display_name_with_dates(name_node: Dict) -> str:
    other_parts, date_parts = [], []
    for np in as_list(nget(name_node, "namePart")):
        txt = (get_text(np) if isinstance(np, dict) else (str(np) if np is not None else "")).strip()
        if not txt:
            continue
        tp = (np.get("@type") or "").lower() if isinstance(np, dict) else ""
        (date_parts if tp == "date" else other_parts).append(txt)
    if not other_parts:
        disp = (get_text(name_node, "displayForm") or "").strip()
        if disp:
            other_parts.append(disp)
    name = " ".join(other_parts).strip()
    if date_parts:
        name = f"{name} ({'; '.join(date_parts)})"
    return name

def extract_creators(mods: Dict) -> str:
    """People that carry creator-ish roles: aut, cmp, cre, prf, voc OR roleTerm text matches."""
    want_codes = {"aut", "cmp", "cre", "prf", "voc"}
    want_texts = {"author", "composer", "creator", "performer", "vocalist", "singer", "voice"}
    out = []
    for n in as_list(nget(mods, "name")):
        # Only people for 'creator' column
        ntype = (n.get("@type") or "").lower() if isinstance(n, dict) else ""
        if ntype and ntype != "personal":
            continue
        is_creator = False
        for role in as_list(nget(n, "role")):
            for rt in as_list(nget(role, "roleTerm")):
                t = (get_text(rt) or "").strip().lower()
                is_code = isinstance(rt, dict) and (rt.get("@type") or "").lower() == "code"
                if (is_code and t in want_codes) or (t in want_texts):
                    is_creator = True
        # Heuristic: also include usage="primary" even if role terms are missing
        if not is_creator:
            usage = (n.get("@usage") or "").lower() if isinstance(n, dict) else ""
            if usage != "primary":
                continue
        disp = _display_name_with_dates(n)
        if disp:
            out.append(disp)
    return "; ".join(dict.fromkeys(out))

def extract_personal_names_split(mods: Dict) -> Tuple[str, str, str, str]:
    """All personal names, first 3 into name1..name3, rest in names_other; format Name (dates)."""
    names = []
    for n in as_list(nget(mods, "name")):
        ntype = (n.get("@type") or "").lower() if isinstance(n, dict) else ""
        if ntype and ntype != "personal":
            continue
        disp = _display_name_with_dates(n)
        if disp:
            names.append(disp)
    names = list(dict.fromkeys(names))  # de-dupe preserving order
    n1 = names[0] if len(names) > 0 else ""
    n2 = names[1] if len(names) > 1 else ""
    n3 = names[2] if len(names) > 2 else ""
    other = "; ".join(names[3:]) if len(names) > 3 else ""
    return n1, n2, n3, other

def extract_corporate_names(mods: Dict) -> str:
    corps = []
    for n in as_list(nget(mods, "name")):
        ntype = (n.get("@type") or "").lower() if isinstance(n, dict) else ""
        if ntype == "corporate" or (ntype == "" and get_text(n, "namePart") and not as_list(nget(n, "namePart"))[0] == ""):
            # treat explicit corporate types as corporate
            disp = _display_name_with_dates(n)
            if disp:
                corps.append(disp)
    return "; ".join(dict.fromkeys(corps))

def extract_publisher(mods: Dict) -> str:
    for oi in as_list(nget(mods, "originInfo")):
        pub = get_text(oi, "publisher")
        if pub:
            return pub
    return ""

def extract_place(mods: Dict) -> str:
    places = []
    for oi in as_list(nget(mods, "originInfo")):
        for pl in as_list(nget(oi, "place")):
            # prefer text
            txt = get_text(pl, "placeTerm")
            if not txt:
                # sometimes two placeTerm nodes (code/text)
                for pt in as_list(nget(pl, "placeTerm")):
                    t = get_text(pt)
                    if t:
                        places.append(t)
            else:
                places.append(txt)
    return join_clean(places)

def extract_date(mods: Dict) -> str:
    dates = []
    for oi in as_list(nget(mods, "originInfo")):
        for k in ("dateIssued", "dateCreated", "dateOther"):
            for v in as_list(nget(oi, k)):
                txt = get_text(v) if isinstance(v, dict) else (str(v) if v is not None else None)
                if txt:
                    dates.append(txt)
    return join_clean(dates)

def extract_language(mods: Dict) -> str:
    langs = []
    for lang in as_list(nget(mods, "language")):
        for lt in as_list(nget(lang, "languageTerm")):
            t = get_text(lt)
            if t:
                langs.append(t)
    return join_clean(langs)

def extract_type_of_resource(mods: Dict) -> str:
    vals = []
    for tor in as_list(nget(mods, "typeOfResource")):
        t = get_text(tor) if isinstance(tor, dict) else (str(tor) if tor is not None else None)
        if t:
            vals.append(t)
    return join_clean(vals)

def extract_physical_description(mods: Dict) -> str:
    chunks = []
    for pd in as_list(nget(mods, "physicalDescription")):
        for k in ("extent", "form", "note", "internetMediaType"):
            for v in as_list(nget(pd, k)):
                txt = get_text(v) if isinstance(v, dict) else (str(v) if v is not None else None)
                if txt:
                    chunks.append(txt)
    return join_clean(chunks)

def extract_keywords(mods: Dict) -> str:
    kw = []
    for subj in as_list(nget(mods, "subject")):
        for key in ("topic", "geographic", "temporal", "genre"):
            for val in as_list(nget(subj, key)):
                txt = get_text(val) if isinstance(val, dict) else (str(val) if val is not None else None)
                if txt:
                    kw.append(txt)
        for nm in as_list(nget(subj, "name")):
            for np in as_list(nget(nm, "namePart")):
                txt = get_text(np) if isinstance(np, dict) else (str(np) if np is not None else None)
                if txt:
                    kw.append(txt)
    return join_clean(kw)

def extract_repository_and_callnum(mods: Dict) -> Tuple[str, str]:
    repos = []
    callnums = []
    for loc in as_list(nget(mods, "location")):
        phys = get_text(loc, "physicalLocation")
        if phys:
            repos.append(phys)
        for pl in as_list(nget(loc, "physicalLocation")):
            t = get_text(pl)
            if t:
                repos.append(t)
        for sl in as_list(nget(loc, "shelfLocator")):
            t = get_text(sl)
            if t:
                callnums.append(t)
    return join_clean(repos), join_clean(callnums)

def extract_issue_number(mods: Dict) -> str:
    nums = []
    for idn in as_list(nget(mods, "identifier")):
        t = (idn.get("@type") or "").lower() if isinstance(idn, dict) else ""
        val = get_text(idn) if isinstance(idn, dict) else (str(idn) if idn is not None else None)
        if val and t == "issue number":
            nums.append(val)
    return join_clean(nums)

def extract_record_identifier(mods: Dict) -> str:
    """Return first recordInfo/recordIdentifier text."""
    for recinfo in as_list(nget(mods, "recordInfo")):
        for rid in as_list(nget(recinfo, "recordIdentifier")):
            txt = get_text(rid)
            if txt:
                return txt
    return ""

def find_digits(s: str) -> Optional[str]:
    m = re.search(r"(99\d{14,})", s)
    return m.group(1) if m else None

def extract_hollis_number(mods: Dict, item: Dict) -> str:
    # 1) Prefer recordInfo/recordIdentifier with ALMA-ish value
    rid = ""
    for recinfo in as_list(nget(mods, "recordInfo")):
        for ridnode in as_list(nget(recinfo, "recordIdentifier")):
            txt = get_text(ridnode) or ""
            src = (ridnode.get("@source") or "").lower() if isinstance(ridnode, dict) else ""
            if txt and (src.find("alma") >= 0 or txt.startswith("99")):
                rid = txt
                break
        if rid:
            break
    if rid:
        return rid

    # 2) Fall back to relatedItem or extension/librarycloud/originalDocument URLs
    for rel in as_list(nget(mods, "relatedItem")):
        url = get_text(rel, "location", "url")
        if url:
            d = find_digits(url)
            if d:
                return d

    # 3) Look inside any extension/originalDocument
    for ext in as_list(nget(mods, "extension")):
        od = get_text(ext, "librarycloud", "originalDocument")
        if od:
            d = find_digits(od)
            if d:
                return d

    return ""

def extract_permalink(item: Dict) -> str:
    # Prefer explicit HOLLIS relatedItem
    mods = item.get("mods") or {}
    for rel in as_list(nget(mods, "relatedItem")):
        oth = (rel.get("@otherType") or "").lower() if isinstance(rel, dict) else ""
        if "hollis" in oth:
            url = get_text(rel, "location", "url")
            if url:
                return url
    # Else any location/url that looks like HOLLIS or a stable link
    for loc in as_list(nget(mods, "location")):
        for urlnode in as_list(nget(loc, "url")):
            u = get_text(urlnode)
            if u:
                return u
    # Last resort: nothing
    return ""

def extract_tocs(mods: Dict) -> List[str]:
    out = []
    for toc in as_list(nget(mods, "tableOfContents")):
        txt = get_text(toc) if isinstance(toc, dict) else (str(toc) if toc is not None else None)
        if txt:
            out.append(txt)
    return out

def extract_notes(mods: Dict) -> List[str]:
    out = []
    for note in as_list(nget(mods, "note")):
        txt = get_text(note) if isinstance(note, dict) else (str(note) if note is not None else None)
        if txt:
            t = (note.get("@type") or "").strip() if isinstance(note, dict) else ""
            out.append(f"{t}: {txt}" if t else txt)
    return out

# ---------------- fetch & parse ----------------
def fetch(session: requests.Session, url: str, params: Dict, verbose: bool = False) -> Dict:
    full = requests.Request("GET", url, params=params).prepare().url
    if verbose:
        print("PARAMS:", params)
        print("GET", full)
    r = session.get(url, params=params, timeout=30)
    if verbose:
        print("Status", r.status_code, r.headers.get("content-type", ""))
    r.raise_for_status()
    return strip_ns(xmltodict.parse(r.text))

def parse_page(parsed: Dict) -> Tuple[List[Dict], Dict[str, int]]:
    results = parsed.get("results", {}) or {}
    pagination = results.get("pagination", {}) or {}
    start = int(pagination.get("start") or 0)
    limit = int(pagination.get("limit") or 10)
    num_found = int(pagination.get("numFound") or 0)

    items_node = results.get("items", {}) or {}
    mods_list = items_node.get("mods")
    if mods_list is None:
        items = []
    else:
        items = mods_list if isinstance(mods_list, list) else [mods_list]
    wrapped = [{"mods": m} for m in items]
    return wrapped, {"start": start, "limit": limit, "numFound": num_found}

# ---------------- row builder ----------------
def row_from_item(item: Dict) -> Dict[str, Any]:
    mods = item.get("mods") or {}

    name1, name2, name3, names_other = extract_personal_names_split(mods)
    repository, callnum = extract_repository_and_callnum(mods)

    row = {
        "identifier": "",  # first generic identifier if needed (fill below)
        "hollis_number": extract_hollis_number(mods, item),
        "title": extract_title(mods),
        "variant_title": extract_variant_titles(mods),
        "creator": extract_creators(mods),
        "name1": name1,
        "name2": name2,
        "name3": name3,
        "names_other": names_other,
        "corporate_name": extract_corporate_names(mods),
        "publisher": extract_publisher(mods),
        "place": extract_place(mods),
        "date": extract_date(mods),
        "language": extract_language(mods),
        "type_of_resource": extract_type_of_resource(mods),
        "physical_description": extract_physical_description(mods),
        "keyword": extract_keywords(mods),
        "repository": repository,
        "call_number": callnum,
        "issue_number": extract_issue_number(mods),
        "permalink": extract_permalink(item),
    }

    # choose a generic identifier if present
    first_ident = None
    for idn in as_list(nget(mods, "identifier")):
        val = get_text(idn) if isinstance(idn, dict) else (str(idn) if idn is not None else None)
        if val:
            first_ident = val
            break
    if not first_ident:
        # fall back to HOLLIS number
        first_ident = row["hollis_number"]
    row["identifier"] = first_ident or ""

    # attach lists for later expansion
    row["_tocs"] = extract_tocs(mods)
    row["_notes"] = extract_notes(mods)

    return row

# ---------------- CLI ----------------
def parse_args():
    ap = argparse.ArgumentParser(description="Harvest Harvard LibraryCloud items to CSV")
    ap.add_argument("--q", required=True, help="Query string")
    ap.add_argument("--page-size", type=int, default=50, help="Records per page (limit)")
    ap.add_argument("--max-records", type=int, default=2000, help="Max records to write")
    ap.add_argument("--output", default="librarycloud_items.csv", help="Output CSV filename")
    ap.add_argument("--verbose", action="store_true")
    return ap.parse_args()

BASE_COLUMNS = [
    "identifier",
    "hollis_number",
    "title",
    "variant_title",
    "creator",
    "name1",
    "name2",
    "name3",
    "names_other",
    "corporate_name",
    "publisher",
    "place",
    "date",
    "language",
    "type_of_resource",
    "physical_description",
    "keyword",
    "repository",
    "call_number",
    "issue_number",
    "permalink",
]

# ---------------- main ----------------
def main():
    args = parse_args()

    rows: List[Dict[str, Any]] = []
    max_tocs = 0
    max_notes = 0
    written = 0
    seen_keys = set()

    with requests.Session() as s:
        start = 0
        limit = max(1, min(args.page_size, 250))  # keep it reasonable

        # fetch first page to learn numFound
        parsed = fetch(s, BASE_URL, {"q": args.q, "start": start, "limit": limit}, verbose=args.verbose)
        items, pg = parse_page(parsed)
        num_found = pg["numFound"]
        if args.verbose:
            print(f"Parsed {len(items)} item(s) from XML; pagination={pg}")

        # process loop
        while True:
            for it in items:
                row = row_from_item(it)
                # de-dupe based on hollis_number or (identifier, permalink)
                key = row.get("hollis_number") or (row.get("identifier"), row.get("permalink"))
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                rows.append(row)
                max_tocs = max(max_tocs, len(row.get("_tocs", [])))
                max_notes = max(max_notes, len(row.get("_notes", [])))
                written += 1
                if written >= args.max_records:
                    break
            if written >= args.max_records:
                break

            start += limit
            if start >= num_found:
                if args.verbose:
                    print("Reached end of results.")
                break

            time.sleep(DEFAULT_SLEEP)
            parsed = fetch(s, BASE_URL, {"q": args.q, "start": start, "limit": limit}, verbose=args.verbose)
            items, pg = parse_page(parsed)
            if args.verbose:
                print(f"Parsed {len(items)} item(s) from XML; pagination={pg}")
            if not items:
                break

    # build dynamic header
    fieldnames = list(BASE_COLUMNS)
    for i in range(1, max_tocs + 1):
        fieldnames.append(f"toc{i}")
    for i in range(1, max_notes + 1):
        fieldnames.append(f"note{i}")

    # write CSV
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            out = {k: r.get(k, "") for k in BASE_COLUMNS}
            # expand tocs/notes
            for i in range(1, max_tocs + 1):
                out[f"toc{i}"] = r.get("_tocs", [])[i - 1] if i - 1 < len(r.get("_tocs", [])) else ""
            for i in range(1, max_notes + 1):
                out[f"note{i}"] = r.get("_notes", [])[i - 1] if i - 1 < len(r.get("_notes", [])) else ""
            w.writerow(out)

    print(f"Wrote {len(rows)} rows to {args.output}")

if __name__ == "__main__":
    main()
