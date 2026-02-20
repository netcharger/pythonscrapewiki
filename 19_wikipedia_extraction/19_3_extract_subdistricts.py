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

# Wikipedia API Headers (Required by Wikipedia)
HEADERS = {
    'User-Agent': 'villages-india-project (balamurali@example.com)'
}

# Initialize Wikipedia API
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
        raise  # Always allow Ctrl+C to stop
    except Exception as e:
        print(f"  [WARN] Error fetching '{term}': {e}")
    return None

def get_page_via_search(term):
    print("Search term", term)
    url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "list": "search",
        "srsearch": term,
        "format": "json"
    }
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            print(f"  [ERROR] Wikipedia API returned {response.status_code}")
            return None
            
        data = response.json()
        if data.get('query') and data['query'].get('search'):
            title = data['query']['search'][0]['title']
            return get_page_direct(title)
    except Exception as e:
        print(f"Error in search API for {term}: {e}")
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
        if response.status_code != 200:
            return None, None
            
        data = response.json()
        pages = data.get('query', {}).get('pages', {})
        for page_id in pages:
            coords = pages[page_id].get('coordinates', [])
            if coords:
                return coords[0]['lat'], coords[0]['lon']
    except Exception as e:
        print(f"Error fetching coordinates for {title}: {e}")
    return None, None

def is_subdistrict_category(title):
    url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "prop": "categories",
        "titles": title,
        "format": "json"
    }
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=10)
        data = response.json()
        pages = data.get("query", {}).get("pages", {})
        for page_id in pages:
            categories = pages[page_id].get("categories", [])
            for cat in categories:
                cat_title = cat['title'].lower()
                # Subdistricts are categorized as Tehsils, Taluks, Mandals, or Community Development Blocks
                subdist_terms = ["tehsil", "taluk", "mandal", "community development block", "subdistrict"]
                for term in subdist_terms:
                    if term in cat_title:
                        return True
    except Exception as e:
        print(f"Error checking category for {title}: {e}")
    return False

def is_correct_page(title, summary, target_name, district, state):
    if not title or not summary:
        return False

    title_lower = title.lower()
    summary_lower = summary.lower()
    target_lower = target_name.lower()
    district_lower = district.lower()
    state_lower = state.lower()

    # MUST contain target name (in title OR summary)
    if target_lower not in title_lower and target_lower not in summary_lower:
        return False

    # MUST contain district OR state name in summary
    if district_lower not in summary_lower and state_lower not in summary_lower:
        return False

    # EXCLUDE clearly wrong pages
    blacklist = ["list of", "assembly constituency", "lok sabha constituency", "elections in"]
    for bad in blacklist:
        if bad in title_lower:
            return False

    return True

def find_wiki_result(name, district, state):
    """
    Search order:
    1. Just the name (e.g. "Nellore")
    2. Name + state (e.g. "Nellore Andhra Pradesh")
    3. Name + tehsil/taluk/mandal/block qualifiers
    4. Name + district  
    5. Wikipedia search API fallback
    """
    result = None

    # ---- STEP 1: Just the subdistrict name ----
    result = get_page_direct(name)
    if result and is_correct_page(result['title'], result['summary'], name, district, state):
        print(f"  [HIT-1] Found by name only: {result['title']}")
        return result

    # ---- STEP 2: name + state ----
    result = get_page_direct(f"{name} {state}")
    if result and is_correct_page(result['title'], result['summary'], name, district, state):
        print(f"  [HIT-2] Found by name+state: {result['title']}")
        return result

    # ---- STEP 3: name + qualifier variants ----
    for qualifier in ["Tehsil", "Taluk", "Mandal", "Block", "Taluka"]:
        for attempt in [
            f"{name} {qualifier}",
            f"{name} {qualifier}, {district}",
            f"{name} {qualifier}, {district} district",
            f"{name} {qualifier}, {state}",
        ]:
            result = get_page_direct(attempt)
            if result and is_correct_page(result['title'], result['summary'], name, district, state):
                print(f"  [HIT-3] Found by qualifier '{qualifier}': {result['title']}")
                return result

    # ---- STEP 4: name + district ----
    for attempt in [
        f"{name}, {district}",
        f"{name} {district}",
        f"{name}, {district} district",
    ]:
        result = get_page_direct(attempt)
        if result and is_correct_page(result['title'], result['summary'], name, district, state):
            print(f"  [HIT-4] Found by name+district: {result['title']}")
            return result

    # ---- STEP 5: Wikipedia Search API fallback ----
    for search_term in [
        name,
        f"{name} {state}",
        f"{name} {district}",
        f"{name} tehsil {district}",
        f"{name} mandal {district}",
    ]:
        result = get_page_via_search(search_term)
        if result and is_correct_page(result['title'], result['summary'], name, district, state):
            print(f"  [HIT-5] Found via search API: {result['title']}")
            return result

    return None

def extract_subdistricts():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    
    print("Fetching PENDING / NOT_FOUND subdistricts...")
    cursor.execute("""
        SELECT * FROM wikipedia_subdistricts 
        WHERE status = 'PENDING'  
        ORDER BY state_name, district_name, subdistrict_name
    """)
    rows = cursor.fetchall()
    print(f"Found {len(rows)} subdistricts to process.")
    
    found_count = 0
    not_found_count = 0

    for row in rows:
        name = row['subdistrict_name']
        district = row['district_name']
        state = row['state_name']
        code = row['subdistrict_code']
        
        print(f"\nProcessing: {name}, {district}, {state}...")
        
        result = find_wiki_result(name, district, state)
        
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
            UPDATE wikipedia_subdistricts 
            SET wiki_title = %s, wiki_url = %s, wiki_summary = %s, status = %s, latitude = %s, longitude = %s
            WHERE subdistrict_code = %s
        """, (wiki_title, wiki_url, wiki_summary, status, lat, lon, code))
        conn.commit()
        upd_cursor.close()
            
        time.sleep(1)

    print(f"\n=== DONE === Found: {found_count} | Not Found: {not_found_count}")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    extract_subdistricts()
