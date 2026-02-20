"""
19_3_scrape_wikipedia_urls_with_bing.py
=========================================
Searches Bing for "{subdistrict_name} wikipedia" to find the Wikipedia URL
for NOT_FOUND subdistricts, then saves wiki_url and sets status=FOUND.

Goal: Only save the Wikipedia URL. Wikidata ID and coordinates are optional.
"""

import requests
import mysql.connector
import time
import re

# =====================
# CONFIG
# =====================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "census_india_2011"
}

# Real browser headers to avoid bot detection
HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
}

WIKI_API = "https://en.wikipedia.org/w/api.php"
DELAY    = 2  # seconds between requests


# =====================
# DB
# =====================
def get_db():
    return mysql.connector.connect(**DB_CONFIG)


# =====================
# BING SEARCH
# =====================
def search_bing(query):
    """
    Search Bing with the given query and return the first Wikipedia URL found.
    Tries multiple CSS selectors since Bing's HTML structure can vary.
    """
    try:
        from bs4 import BeautifulSoup

        r = requests.get("https://www.bing.com/search",
                         params={"q": query},
                         headers=HEADERS,
                         timeout=15)

        soup = BeautifulSoup(r.text, "html.parser")

        # Try multiple Bing selectors
        selectors = [
            "li.b_algo h2 a",
            "li.b_algo a",
            ".b_algo h2 a",
            "#b_results h2 a",
            "h2 a[href*='wikipedia.org']",
        ]
        links_found = []
        for sel in selectors:
            links_found = soup.select(sel)
            if links_found:
                break

        for a in links_found:
            href = a.get("href", "")
            if "wikipedia.org/wiki/" in href:
                return href

    except ImportError:
        print("  [WARN] BeautifulSoup not installed, using Wikipedia API fallback")
    except KeyboardInterrupt:
        raise
    except Exception as e:
        print(f"  [Bing Error] {e}")

    return None


# =====================
# WIKIPEDIA API SEARCH (fallback)
# =====================
def search_wikipedia_api(name, district, state):
    """
    Fallback: use Wikipedia's own search API with multiple terms.
    """
    search_terms = [
        name,
        f"{name} {district}",
        f"{name} {state}",
        f"{name} mandal {district}",
        f"{name} tehsil {district}",
    ]
    for term in search_terms:
        try:
            r = requests.get(WIKI_API, params={
                "action": "query",
                "list": "search",
                "srsearch": term,
                "format": "json",
                "srlimit": 5,
            }, headers={"User-Agent": "villagesindia.com"}, timeout=10)

            results = r.json().get("query", {}).get("search", [])
            for result in results:
                title = result["title"]
                # Check if first significant word of name appears in title
                first_word = re.sub(r'[^a-z]', '', name.lower().split()[0])
                if first_word and first_word in title.lower().replace(" ", ""):
                    return f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"
        except KeyboardInterrupt:
            raise
        except Exception:
            pass
    return None


# =====================
# FIND WIKIPEDIA URL
# =====================
def find_wikipedia_url(name, district, state):
    """
    Step 1: Try Bing with '{name} wikipedia'
    Step 2: Try Bing with '{name} {district} wikipedia'
    Step 3: Fallback to Wikipedia API
    """
    bing_queries = [
        f"{name} wikipedia",
        f"{name} {district} wikipedia",
        f"{name} {state} wikipedia",
    ]

    for query in bing_queries:
        print(f"  Bing: \"{query}\"")
        url = search_bing(query)
        if url:
            print(f"  → {url}")
            return url
        time.sleep(DELAY)

    # Fallback to Wikipedia API
    print(f"  Wikipedia API fallback...")
    url = search_wikipedia_api(name, district, state)
    if url:
        print(f"  → {url}")
    return url


# =====================
# GET WIKIDATA ID (optional)
# =====================
def get_wikidata_id(wikipedia_url):
    """Get Wikidata QID via Wikipedia pageprops API."""
    title = wikipedia_url.split("/wiki/")[-1].replace("_", " ")
    try:
        r = requests.get(WIKI_API, params={
            "action": "query",
            "titles": title,
            "prop": "pageprops",
            "format": "json",
        }, headers={"User-Agent": "villagesindia.com"}, timeout=10)

        pages = r.json().get("query", {}).get("pages", {})
        for page in pages.values():
            return page.get("pageprops", {}).get("wikibase_item")
    except Exception:
        pass
    return None


# =====================
# UPDATE DB
# =====================
def update_db(conn, code, wiki_url, qid, status):
    cur = conn.cursor()
    cur.execute("""
        UPDATE wikipedia_subdistricts
        SET wiki_url=%s, wikidata_id=%s, status=%s
        WHERE subdistrict_code=%s
    """, (wiki_url, qid, status, code))
    conn.commit()
    cur.close()


# =====================
# MAIN
# =====================
def main():
    conn = get_db()
    cur  = conn.cursor(dictionary=True)

    # Only process NOT_FOUND or PENDING rows
    cur.execute("""
        SELECT *
        FROM wikipedia_subdistricts
        WHERE status IS NULL OR status = 'PENDING' OR status = 'NOT_FOUND'
        ORDER BY state_name, district_name, subdistrict_name
    """)
    rows = cur.fetchall()
    cur.close()

    print(f"Total to process: {len(rows)}\n")

    found = 0; not_found = 0

    for row in rows:
        name     = row["subdistrict_name"]
        district = row["district_name"]
        state    = row["state_name"]
        code     = row["subdistrict_code"]

        print(f"\n[{name}] — {district}, {state}")

        wiki_url = find_wikipedia_url(name, district, state)

        if wiki_url:
            # Try to also get wikidata_id
            qid = get_wikidata_id(wiki_url)
            update_db(conn, code, wiki_url, qid, "FOUND")
            print(f"  ✓ SAVED — {wiki_url}  QID={qid}")
            found += 1
        else:
            update_db(conn, code, None, None, "NOT_FOUND")
            print(f"  ✗ NOT FOUND")
            not_found += 1

        time.sleep(DELAY)

    conn.close()

    print(f"\n{'='*50}")
    print(f"FOUND:    {found}")
    print(f"NOT FOUND:{not_found}")
    print("✅ Done!\n")


if __name__ == "__main__":
    main()