"""
19_2_extract_subdistricts_sparql.py
=====================================
Fetches Indian subdistricts from Wikidata state-by-state to avoid timeouts.
Matches against wikipedia_subdistricts using multiple name patterns.

Wikidata types:
  Q817477  = mandal (India)
  Q1229870 = tehsil
  Q6465    = taluka
  Q2514330 = community development block
"""

import os, sys
import requests
import time
import re

sys.path.insert(0, os.path.dirname(__file__))
from db_config import get_db

SPARQL_URL = "https://query.wikidata.org/sparql"
HEADERS    = {"User-Agent": "villagesindia.com (balamurali@example.com)"}

# All Indian state QIDs on Wikidata
INDIA_STATES = {
    "Andhra Pradesh":         "Q1159",
    "Arunachal Pradesh":      "Q1162",
    "Assam":                  "Q1164",
    "Bihar":                  "Q1165",
    "Chhattisgarh":           "Q1168",
    "Goa":                    "Q1171",
    "Gujarat":                "Q1061",
    "Haryana":                "Q1174",
    "Himachal Pradesh":       "Q1177",
    "Jharkhand":              "Q1184",
    "Karnataka":              "Q1185",
    "Kerala":                 "Q1186",
    "Madhya Pradesh":         "Q1188",
    "Maharashtra":            "Q1191",
    "Manipur":                "Q1193",
    "Meghalaya":              "Q1195",
    "Mizoram":                "Q1502",
    "Nagaland":               "Q1599",
    "Odisha":                 "Q22048",
    "Punjab":                 "Q22424",
    "Rajasthan":              "Q1137",
    "Sikkim":                 "Q1505",
    "Tamil Nadu":             "Q1445",
    "Telangana":              "Q677037",
    "Tripura":                "Q1363",
    "Uttar Pradesh":          "Q1498",
    "Uttarakhand":            "Q1499",
    "West Bengal":            "Q1356",
    "Jammu and Kashmir":      "Q1030",
    "Delhi":                  "Q1353",
    "Andaman and Nicobar":    "Q40888",
    "Chandigarh":             "Q43433",
    "Dadra and Nagar Haveli": "Q46107",
    "Daman and Diu":          "Q46208",
    "Lakshadweep":            "Q26253",
    "Puducherry":             "Q66743",
}

SUBDISTRICT_TYPES = "wd:Q817477 wd:Q1229870 wd:Q6465 wd:Q2514330"


# =====================
#  DB
# =====================
# get_db is imported from db_config

def add_column_if_missing():
    conn = get_db(); cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE `wikipedia_subdistricts` ADD COLUMN `wikidata_id` VARCHAR(20) NULL")
        print("  Added column: wikipedia_subdistricts.wikidata_id")
    except Exception as e:
        if "Duplicate column" not in str(e):
            print(f"  Warning: {e}")
    conn.commit(); cur.close(); conn.close()


# =====================
#  SPARQL
# =====================
def sparql_query(q, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(SPARQL_URL,
                             params={"query": q, "format": "json"},
                             headers=HEADERS, timeout=60)
            if r.status_code == 200:
                return r.json()["results"]["bindings"]
            elif r.status_code == 429:
                print("  [RATE LIMITED] Waiting 30s...")
                time.sleep(30)
            else:
                print(f"  [HTTP {r.status_code}]")
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"  [Error attempt {attempt+1}]: {e}")
            time.sleep(5)
    return []

def parse_coord(val):
    if not val:
        return None, None
    try:
        c = val.replace("Point(","").replace(")","").strip()
        lon, lat = c.split()
        return float(lat), float(lon)
    except Exception:
        return None, None


# =====================
#  NORMALIZER
# =====================
def normalize(name):
    """Normalize using word boundaries to avoid corrupting embedded words."""
    if not name:
        return ""
    name = name.lower()
    # Strip type words with word boundary
    for s in [r'\bmandal\b', r'\btehsil\b', r'\btaluka?\b', r'\bblock\b',
              r'\bcd\b', r'\bcircle\b', r'\bdistrict\b', r'\bsubdistrict\b',
              r'\bsub-district\b', r'\bsdo\b', r'\blmd\b', r'\beac\b',
              r'\badc\b', r'\bhq\b', r'\bst\b', r'\bpt\b']:
        name = re.sub(s, '', name)
    # Remove punctuation except spaces
    name = re.sub(r'[^a-z0-9 ]', ' ', name)
    # Collapse whitespace
    name = re.sub(r'\s+', ' ', name)
    return name.strip()


# =====================
#  LOAD PER STATE
# =====================
def load_state_subdistricts(state_name, state_qid):
    """
    Fetch subdistricts for ONE state using a simple 2-hop query:
      item → district → state  (avoids P131* timeout)
    Falls back to P17=Q668 (country=India) if 2-hop returns 0.
    """
    # PRIMARY: item in district, district in state (2 hops, fast)
    q = f"""
    SELECT ?item ?itemLabel ?district ?districtLabel ?altLabel ?coord WHERE {{
      ?item wdt:P31 ?type.
      VALUES ?type {{ {SUBDISTRICT_TYPES} }}
      ?item wdt:P131 ?district.
      ?district wdt:P131 wd:{state_qid}.
      OPTIONAL {{ ?item wdt:P625 ?coord. }}
      OPTIONAL {{
        ?item skos:altLabel ?altLabel.
        FILTER(LANG(?altLabel)="en")
      }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """
    rows = sparql_query(q)

    # FALLBACK: direct P131 = state (item directly in state)
    if not rows:
        q2 = f"""
        SELECT ?item ?itemLabel ?district ?districtLabel ?altLabel ?coord WHERE {{
          ?item wdt:P31 ?type.
          VALUES ?type {{ {SUBDISTRICT_TYPES} }}
          ?item wdt:P131 wd:{state_qid}.
          OPTIONAL {{ ?item wdt:P131 ?district.
                     ?district wdt:P31 wd:Q1149652. }}
          OPTIONAL {{ ?item wdt:P625 ?coord. }}
          OPTIONAL {{
            ?item skos:altLabel ?altLabel.
            FILTER(LANG(?altLabel)="en")
          }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
        """
        rows = sparql_query(q2)

    entries = []
    for r in rows:
        qid      = r["item"]["value"].split("/")[-1]
        label    = r.get("itemLabel",     {}).get("value", "")
        district = r.get("districtLabel", {}).get("value", "")
        alt      = r.get("altLabel",      {}).get("value", "")
        lat, lon = parse_coord(r.get("coord", {}).get("value"))
        entries.append({
            "qid": qid, "lat": lat, "lon": lon,
            "label": label, "district": district,
            "alt": alt, "state": state_name
        })
    return entries


# =====================
#  MATCH FOR ONE DB ROW
# =====================
def find_match(name, district, state, entries):
    """
    Match one census subdistrict against all Wikidata entries for that state.
    Tries multiple name patterns (same strategy as 19_3_extract_subdistricts.py).
    """
    n  = normalize(name)
    d  = normalize(district)

    for entry in entries:
        wl  = normalize(entry["label"])
        wla = normalize(entry["alt"])
        wd_d = normalize(entry["district"])

        # Check name match (label or altlabel)
        name_match = False
        for wn in [wl, wla]:
            if not wn:
                continue
            # Exact match
            if n == wn:
                name_match = True; break
            # Partial — census name inside wikidata label or vice versa
            if n and wn and (n in wn or wn in n):
                name_match = True; break

        if not name_match:
            continue

        # District cross-check (soft — only reject if clearly wrong)
        if wd_d and d:
            if wd_d not in d and d not in wd_d:
                continue

        return entry

    return None


# =====================
#  MAIN
# =====================
def extract_subdistricts():
    print("=" * 55)
    print("Subdistricts — SPARQL State-by-State")
    print("=" * 55)

    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT * FROM wikipedia_subdistricts
        WHERE wikidata_id IS NULL
        ORDER BY state_name, district_name, subdistrict_name
    """)
    db_rows = cur.fetchall()

    # Group DB rows by state
    by_state = {}
    for row in db_rows:
        s = row["state_name"]
        by_state.setdefault(s, []).append(row)

    print(f"States to process: {len(by_state)}")
    print(f"Total subdistricts: {len(db_rows)}\n")

    found = 0; not_found = 0

    for state_name, rows in sorted(by_state.items()):
        # Find Wikidata QID for this state
        state_qid = None
        for k, v in INDIA_STATES.items():
            if normalize(k) == normalize(state_name):
                state_qid = v
                break

        if not state_qid:
            print(f"\n[SKIP] No Wikidata QID for state: {state_name} ({len(rows)} rows)")
            not_found += len(rows)
            continue

        print(f"\n[{state_name}] ({len(rows)} subdistricts) — fetching from Wikidata...")
        entries = load_state_subdistricts(state_name, state_qid)
        print(f"  Wikidata returned {len(entries)} entries")

        if not entries:
            print(f"  [SKIP] No Wikidata data for {state_name}")
            not_found += len(rows)
            continue

        upd = conn.cursor()
        state_found = 0

        for row in rows:
            name     = row["subdistrict_name"]
            district = row["district_name"]
            code     = row["subdistrict_code"]

            entry = find_match(name, district, state_name, entries)

            if entry:
                upd.execute("""
                    UPDATE wikipedia_subdistricts
                    SET wikidata_id=%s, latitude=%s, longitude=%s
                    WHERE subdistrict_code=%s
                """, (entry["qid"], entry["lat"], entry["lon"], code))
                conn.commit()
                print(f"  ✓ {name} ({district}) → [{entry['qid']}]")
                found += 1; state_found += 1
            else:
                not_found += 1

        upd.close()
        print(f"  → {state_found}/{len(rows)} matched for {state_name}")
        time.sleep(1)  # polite pause between states

    cur.close(); conn.close()

    # Summary
    print(f"\n{'='*55}")
    print("FINAL SUMMARY")
    print(f"{'='*55}")
    conn2 = get_db(); c2 = conn2.cursor()
    c2.execute("SELECT COUNT(*) FROM wikipedia_subdistricts WHERE wikidata_id IS NOT NULL")
    total_matched = c2.fetchone()[0]
    c2.execute("SELECT COUNT(*) FROM wikipedia_subdistricts")
    grand_total = c2.fetchone()[0]
    c2.close(); conn2.close()
    pct = 100 * total_matched // grand_total if grand_total else 0
    print(f"  FOUND this run:      {found}")
    print(f"  NOT FOUND this run:  {not_found}")
    print(f"  TOTAL DB MATCHED:    {total_matched}/{grand_total} ({pct}%)")
    print("\n✅ Done!\n")


if __name__ == "__main__":
    print("\n🇮🇳  Subdistrict Wikidata Extractor — State-by-State SPARQL\n")
    add_column_if_missing()
    extract_subdistricts()