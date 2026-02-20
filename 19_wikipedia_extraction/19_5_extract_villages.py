"""
19_5_extract_villages.py
==========================
Extracts Wikipedia data for Indian villages.

KEY IMPROVEMENTS vs old script:
  - Uses direct requests + Wikipedia REST API (with timeout=10s) instead of
    the `wikipediaapi` library which has NO timeout and crashes the script.
  - Smarter match logic: checks name + district/state, no hard "village" requirement
    because many valid village articles don't use the word "village".
  - Skips obviously unsearchable names (too short, special chars, Nicobar etc).
  - `is_village_category()` removed as a REQUIRED check — used as bonus only.
  - Tries more search patterns in the right order.
"""

import mysql.connector
import requests
import re
import time

# =====================
# CONFIG
# =====================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "census_india_2011"
}

HEADERS = {"User-Agent": "villages-india-project (balamurali@example.com)"}
WIKI_API  = "https://en.wikipedia.org/w/api.php"
WIKI_REST = "https://en.wikipedia.org/api/rest_v1/page/summary/"
DELAY = 1.0   # seconds between rows

# States/UTs where almost no villages have Wikipedia pages — skip early
SKIP_STATES = {"andaman and nicobar islands", "lakshadweep"}


# =====================
# DB
# =====================
def get_db():
    return mysql.connector.connect(**DB_CONFIG)


# =====================
# CLEAN NAME for searching
# =====================
def clean_name(name):
    """Remove asterisks, brackets, slashes, extra spaces from census names."""
    name = re.sub(r'\*', '', name)                  # trailing asterisks
    name = re.sub(r'\(.*?\)', '', name)             # parenthetical notes
    name = re.sub(r'/.*', '', name)                 # alternate names after /
    name = re.sub(r'[^a-zA-Z0-9 \-\.]', ' ', name)
    return re.sub(r'\s+', ' ', name).strip()


# =====================
# WIKIPEDIA PAGE (via REST API with timeout)
# =====================
def get_page_rest(title):
    """
    Fetch a Wikipedia page summary via the REST API.
    Has a proper timeout — will never hang.
    """
    try:
        encoded = requests.utils.quote(title.replace(' ', '_'))
        r = requests.get(f"{WIKI_REST}{encoded}",
                         headers=HEADERS, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data.get("type") in ("standard", "disambiguation"):
                return {
                    "title": data.get("title", title),
                    "summary": data.get("extract", "")[:1000],
                    "url": data.get("content_urls", {}).get("desktop", {}).get("page", ""),
                }
    except KeyboardInterrupt:
        raise
    except Exception as e:
        pass
    return None


# =====================
# WIKIPEDIA SEARCH (API with timeout)
# =====================
def search_wiki(term, limit=5):
    """Search Wikipedia and return a list of (title, snippet) tuples."""
    try:
        r = requests.get(WIKI_API, params={
            "action": "query",
            "list": "search",
            "srsearch": term,
            "format": "json",
            "srlimit": limit,
        }, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            return r.json().get("query", {}).get("search", [])
    except KeyboardInterrupt:
        raise
    except Exception:
        pass
    return []


# =====================
# GET COORDINATES
# =====================
def get_coordinates(title):
    try:
        r = requests.get(WIKI_API, params={
            "action": "query",
            "prop": "coordinates",
            "titles": title,
            "format": "json",
        }, headers=HEADERS, timeout=10)
        pages = r.json().get("query", {}).get("pages", {})
        for page in pages.values():
            coords = page.get("coordinates", [])
            if coords:
                return coords[0]["lat"], coords[0]["lon"]
    except Exception:
        pass
    return None, None


# =====================
# VALIDATION
# =====================
GEO_WORDS = [
    "village", "town", "hamlet", "settlement", "populated place",
    "panchayat", "gram", "locality", "census", "population",
    "district", "tehsil", "mandal", "taluk",
]

BLACKLIST = [
    "list of", "constituency", "election", "railway station",
    "airport", "disambiguation",
]

def is_correct_page(title, summary, name, district, state):
    t = title.lower()
    s = summary.lower()
    n = name.lower()
    d = district.lower()
    st = state.lower()

    # Must contain the village name
    if n not in t and n not in s:
        return False

    # Must mention district or state
    if d not in s and st not in s:
        return False

    # Must look like a geographic article
    if not any(w in s[:700] for w in GEO_WORDS):
        return False

    # Reject wrong page types
    if any(b in t for b in BLACKLIST):
        return False

    return True


# =====================
# FIND WIKIPEDIA MATCH
# =====================
def find_wiki_result(name, subdistrict, district, state):
    """
    Strategy:
    1. Direct lookup: "Name, District"
    2. Direct lookup: "Name village"
    3. Direct lookup: "Name, State"
    4. Direct lookup: "Name"
    5. Search API: "Name village District State"
    6. Search API: "Name District State"
    7. Search API: "Name State"
    """
    clean = clean_name(name)
    if not clean or len(clean) < 2:
        return None

    # Direct lookups — fast, no search
    direct_attempts = [
        f"{clean}, {district}",
        f"{clean} village",
        f"{clean}, {state}",
        f"{clean}, {district}, {state}",
        clean,
    ]
    for attempt in direct_attempts:
        result = get_page_rest(attempt)
        if result and is_correct_page(result["title"], result["summary"], clean, district, state):
            return result

    # Search API — broader but slower
    search_terms = [
        f"{clean} village {district} {state}",
        f"{clean} {district}",
        f"{clean} {state}",
    ]
    for term in search_terms:
        hits = search_wiki(term, limit=5)
        for hit in hits:
            hit_title = hit["title"]
            # Quick pre-filter: title must contain first word of name
            first_word = clean.split()[0].lower()
            if first_word not in hit_title.lower():
                continue
            result = get_page_rest(hit_title)
            if result and is_correct_page(result["title"], result["summary"], clean, district, state):
                return result

    return None


# =====================
# MAIN
# =====================
def extract_villages():
    conn = get_db()
    cur  = conn.cursor(dictionary=True)

    print("Fetching PENDING villages...")
    cur.execute("""
        SELECT * FROM wikipedia_villages
        WHERE status = 'PENDING'
        ORDER BY state_name, district_name, village_name
        LIMIT 500
    """)
    rows = cur.fetchall()
    cur.close()
    print(f"Found {len(rows)} villages to process.\n")

    found = 0; not_found = 0; skipped = 0

    for row in rows:
        name        = row["village_name"]
        subdistrict = row["subdistrict_name"]
        district    = row["district_name"]
        state       = row["state_name"]
        code        = row["village_code"]

        print(f"Processing: {name}, {district}, {state}...")

        # Skip states with near-zero Wikipedia coverage
        if state.lower() in SKIP_STATES:
            print(f"  [SKIP] No Wikipedia coverage for {state}")
            upd = conn.cursor()
            upd.execute("""
                UPDATE wikipedia_villages SET status='NOT_FOUND'
                WHERE village_code=%s
            """, (code,))
            conn.commit(); upd.close()
            skipped += 1
            continue

        result = find_wiki_result(name, subdistrict, district, state)

        if result:
            title   = result["title"]
            url     = result["url"]
            summary = result["summary"]
            lat, lon = get_coordinates(title)
            # Fall back to census coordinates if Wikipedia has none
            if lat is None:
                lat = row.get("latitude")
                lon = row.get("longitude")
            status = "FOUND"
            print(f"  [FOUND] {title}")
            found += 1
        else:
            title = url = summary = None
            lat = row.get("latitude")
            lon = row.get("longitude")
            status = "NOT_FOUND"
            print(f"  [NOT FOUND]")
            not_found += 1

        upd = conn.cursor()
        upd.execute("""
            UPDATE wikipedia_villages
            SET wiki_title=%s, wiki_url=%s, wiki_summary=%s,
                status=%s, latitude=%s, longitude=%s
            WHERE village_code=%s
        """, (title, url, summary, status, lat, lon, code))
        conn.commit(); upd.close()

        time.sleep(DELAY)

    conn.close()
    print(f"\n=== DONE ===")
    print(f"  FOUND:    {found}")
    print(f"  NOT FOUND:{not_found}")
    print(f"  SKIPPED:  {skipped}")


if __name__ == "__main__":
    extract_villages()
