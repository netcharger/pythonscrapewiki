import mysql.connector
import wikipediaapi
import requests
import time

# Database configuration
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "census_india_2011"
}

HEADERS = {
    'User-Agent': 'villages-india-project (balamurali@example.com)'
}

wiki_wiki = wikipediaapi.Wikipedia(
    language='en',
    user_agent=HEADERS['User-Agent']
)

def get_connection():
    return mysql.connector.connect(**db_config)

def get_page_direct(term):
    try:
        page = wiki_wiki.page(term)
        if page.exists():
            return {
                "title": page.title,
                "summary": page.summary[0:1000],
                "url": page.fullurl
            }
    except KeyboardInterrupt:
        raise
    except Exception as e:
        print(f"  [WARN] Error fetching '{term}': {e}")
    return None

def get_page_via_search(term):
    url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "list": "search",
        "srsearch": term,
        "format": "json",
        "srlimit": 5
    }
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            return None
        data = response.json()
        if data.get('query') and data['query'].get('search'):
            for hit in data['query']['search']:
                page = get_page_direct(hit['title'])
                if page:
                    return page
    except KeyboardInterrupt:
        raise
    except Exception as e:
        print(f"  [WARN] Search API error for '{term}': {e}")
    return None

def get_coordinates(title):
    url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "prop": "coordinates",
        "titles": title,
        "format": "json"
    }
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=10)
        data = response.json()
        pages = data.get('query', {}).get('pages', {})
        for page_id in pages:
            coords = pages[page_id].get('coordinates', [])
            if coords:
                return coords[0]['lat'], coords[0]['lon']
    except Exception:
        pass
    return None, None

# Geographic keywords to confirm it's a place
GEO_KEYWORDS = [
    "town", "city", "municipality", "municipal", "corporation", "nagarpalika",
    "nagar panchayat", "urban", "population", "district", "state", "india"
]

def is_correct_page(title, summary, target_name, state):
    if not title or not summary:
        return False

    title_lower = title.lower()
    summary_lower = summary.lower()
    target_lower = target_name.lower()
    state_lower = state.lower()

    # MUST contain the target ULB name
    if target_lower not in title_lower and target_lower not in summary_lower:
        return False

    # MUST contain state name in summary
    if state_lower not in summary_lower:
        return False

    # MUST look like a geographic place page
    if not any(k in summary_lower[:600] for k in GEO_KEYWORDS):
        return False

    # EXCLUDE clearly wrong pages
    blacklist = [
        "list of", "assembly constituency", "lok sabha constituency",
        "elections in", "vidhan sabha", "railway station"
    ]
    for bad in blacklist:
        if bad in title_lower:
            return False

    return True

def find_wiki_result(name, state, type_):
    """
    Search order:
    1. Just the ULB name (e.g. "Nellore")
    2. Name + state (e.g. "Nellore Andhra Pradesh")
    3. Name + type qualifiers (Municipality, City, Town, etc.)
    4. Name + state variations
    5. Wikipedia search API fallback
    """

    # ---- STEP 1: Just the ULB name ----
    result = get_page_direct(name)
    if result and is_correct_page(result['title'], result['summary'], name, state):
        print(f"  [HIT-1] Found by name only: {result['title']}")
        return result

    # ---- STEP 2: name + state ----
    result = get_page_direct(f"{name} {state}")
    if result and is_correct_page(result['title'], result['summary'], name, state):
        print(f"  [HIT-2] Found by name+state: {result['title']}")
        return result

    # ---- STEP 3: name + type qualifiers ----
    qualifiers = ["Municipality", "Municipal Corporation", "City", "Town", "Nagar Palika", "Nagar Panchayat"]
    if type_:
        qualifiers = [type_] + [q for q in qualifiers if q.lower() != type_.lower()]

    for qualifier in qualifiers:
        for attempt in [
            f"{name} {qualifier}",
            f"{name} {qualifier}, {state}",
            f"{name}, {state} {qualifier}",
        ]:
            result = get_page_direct(attempt)
            if result and is_correct_page(result['title'], result['summary'], name, state):
                print(f"  [HIT-3] Found by qualifier '{qualifier}': {result['title']}")
                return result

    # ---- STEP 4: name + state variations ----
    for attempt in [
        f"{name}, {state}",
        f"{name} ({state})",
    ]:
        result = get_page_direct(attempt)
        if result and is_correct_page(result['title'], result['summary'], name, state):
            print(f"  [HIT-4] Found by variation: {result['title']}")
            return result

    # ---- STEP 5: Wikipedia Search API fallback ----
    search_terms = [
        name,
        f"{name} {state}",
        f"{name} {type_} {state}" if type_ else f"{name} municipality {state}",
        f"{name} town {state}",
    ]
    for search_term in search_terms:
        result = get_page_via_search(search_term)
        if result and is_correct_page(result['title'], result['summary'], name, state):
            print(f"  [HIT-5] Found via search API: {result['title']}")
            return result

    return None

def extract_ulbs():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    
    print("Fetching PENDING / NOT_FOUND ULBs...")
    cursor.execute("""
        SELECT * FROM wikipedia_ulbs 
        WHERE status = 'PENDING'  
        ORDER BY state_name, ulb_name
    """)
    rows = cursor.fetchall()
    print(f"Found {len(rows)} ULBs to process.")
    
    found_count = 0
    not_found_count = 0

    for row in rows:
        name = row['ulb_name']
        state = row['state_name']
        code = row['ulb_code']
        type_ = row.get('ulb_type', '')
        
        print(f"\nProcessing: {name}, {state} ({type_})...")
        
        result = find_wiki_result(name, state, type_)
        
        status = 'FOUND' if result else 'NOT_FOUND'
        wiki_title = result['title'] if result else None
        wiki_url = result['url'] if result else None
        wiki_summary = result['summary'] if result else None
        
        lat, lon = (None, None)
        if result:
            lat, lon = get_coordinates(wiki_title)
            found_count += 1
        else:
            not_found_count += 1
            print(f"  [NOT FOUND]")

        upd_cursor = conn.cursor()
        upd_cursor.execute("""
            UPDATE wikipedia_ulbs 
            SET wiki_title = %s, wiki_url = %s, wiki_summary = %s, status = %s, latitude = %s, longitude = %s
            WHERE ulb_code = %s
        """, (wiki_title, wiki_url, wiki_summary, status, lat, lon, code))
        conn.commit()
        upd_cursor.close()
            
        time.sleep(1)

    print(f"\n=== DONE === Found: {found_count} | Not Found: {not_found_count}")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    extract_ulbs()
