"""
19_1_extract_districts_sparql.py
=================================
Fetches ALL Indian districts from Wikidata via ONE SPARQL query,
then matches them to wikipedia_districts table using multiple name patterns.

Saves: wikidata_id, latitude, longitude
"""

import os, sys
import requests
import time
import re

sys.path.insert(0, os.path.dirname(__file__))
from db_config import get_db

SPARQL_URL = "https://query.wikidata.org/sparql"
HEADERS    = {"User-Agent": "villagesindia.com (balamurali@example.com)"}

def add_column_if_missing():
    conn = get_db(); cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE `wikipedia_districts` ADD COLUMN `wikidata_id` VARCHAR(20) NULL")
        print("  Added column: wikipedia_districts.wikidata_id")
    except mysql.connector.Error as e:
        if e.errno != 1060:
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
                             headers=HEADERS, timeout=90)
            if r.status_code == 200:
                return r.json()["results"]["bindings"]
            elif r.status_code == 429:
                print("  [RATE LIMITED] Waiting 30s...")
                time.sleep(30)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"  [SPARQL error attempt {attempt+1}]: {e}")
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
    name = name.lower()
    for s in [' district', ' mandal', ' tehsil', ' taluk', ' taluka',
              ' block', ' subdistrict', ' state', ' and ']:
        name = name.replace(s, ' ')
    name = re.sub(r'[^a-z0-9 ]', '', name)
    return name.strip()


# =====================
#  LOAD FROM WIKIDATA
# =====================
def load_wikidata_districts():
    """
    Single SPARQL query — gets all Indian districts (P31=Q1149652)
    with labels, altLabels, parent state, and coordinates.
    """
    print("Loading all Indian districts from Wikidata via SPARQL...")
    q = """
    SELECT ?item ?itemLabel ?stateLabel ?altLabel ?coord WHERE {
      ?item wdt:P31 wd:Q1149652.
      ?item wdt:P17 wd:Q668.
      OPTIONAL { ?item wdt:P131 ?state.
                 ?state wdt:P31 wd:Q12443800. }
      OPTIONAL { ?item wdt:P625 ?coord. }
      OPTIONAL { ?item skos:altLabel ?altLabel. FILTER(LANG(?altLabel)="en") }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    """
    rows = sparql_query(q)
    print(f"  Got {len(rows)} rows from Wikidata")

    lookup = {}   # normalized_name → {qid, lat, lon, label, state}
    for r in rows:
        qid   = r["item"]["value"].split("/")[-1]
        label = r.get("itemLabel", {}).get("value", "")
        state = r.get("stateLabel", {}).get("value", "")
        alt   = r.get("altLabel",   {}).get("value", "")
        lat, lon = parse_coord(r.get("coord", {}).get("value"))
        entry = {"qid": qid, "lat": lat, "lon": lon, "label": label, "state": state}

        for name_var in [label, alt]:
            if name_var:
                key = normalize(name_var)
                if key and key not in lookup:
                    lookup[key] = entry

    print(f"  Built lookup with {len(lookup)} unique normalized names\n")
    return lookup


# =====================
#  MATCH
# =====================
def find_match(name, state, lookup):
    """
    Try multiple name patterns — same strategy as 19_2_extract_districts.py:
      1. "Nagaon district assam"
      2. "Nagaon district"
      3. "Nagaon assam"
      4. "Nagaon"
    """
    patterns = [
        normalize(f"{name} district {state}"),
        normalize(f"{name} district"),
        normalize(f"{name} {state}"),
        normalize(name),
    ]
    for pattern in patterns:
        if pattern in lookup:
            entry = lookup[pattern]
            # State cross-check (skip if state clearly wrong)
            if entry["state"] and state:
                es = normalize(entry["state"])
                ds = normalize(state)
                if es not in ds and ds not in es:
                    continue
            return entry
    return None


# =====================
#  MAIN
# =====================
def extract_districts():
    print("=" * 55)
    print("Extracting Districts — Pure SPARQL")
    print("=" * 55)

    lookup = load_wikidata_districts()

    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT * FROM wikipedia_districts
        WHERE wikidata_id IS NULL
        ORDER BY state_name, district_name
    """)
    rows = cur.fetchall()
    total = len(rows)
    print(f"Districts to process: {total}\n")

    found = 0; not_found = 0

    for row in rows:
        name  = row["district_name"]
        state = row["state_name"]
        code  = row["district_code"]

        entry = find_match(name, state, lookup)

        if entry:
            upd = conn.cursor()
            upd.execute("""
                UPDATE wikipedia_districts
                SET wikidata_id=%s, latitude=%s, longitude=%s
                WHERE district_code=%s
            """, (entry["qid"], entry["lat"], entry["lon"], code))
            conn.commit(); upd.close()
            print(f"  ✓ {name} ({state}) → [{entry['qid']}]  {entry['label']}")
            found += 1
        else:
            print(f"  ✗ {name}, {state}")
            not_found += 1

    cur.close(); conn.close()

    print(f"\n{'='*55}")
    print(f"RESULT")
    print(f"{'='*55}")
    # Summary from DB
    conn2 = get_db(); c2 = conn2.cursor()
    c2.execute("SELECT COUNT(*) FROM wikipedia_districts WHERE wikidata_id IS NOT NULL")
    total_matched = c2.fetchone()[0]
    c2.execute("SELECT COUNT(*) FROM wikipedia_districts")
    grand_total = c2.fetchone()[0]
    c2.close(); conn2.close()
    pct = 100 * total_matched // grand_total if grand_total else 0
    print(f"  FOUND:     {found}")
    print(f"  NOT FOUND: {not_found}")
    print(f"  TOTAL IN DB MATCHED: {total_matched}/{grand_total} ({pct}%)")
    print("\n✅ Done!\n")


if __name__ == "__main__":
    print("\n🇮🇳  District Wikidata Extractor — Pure SPARQL\n")
    add_column_if_missing()
    extract_districts()
