import mysql.connector
import wikipediaapi
import requests
import time
from bs4 import BeautifulSoup

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
    except Exception as e:
        print(f"Error fetching {term}: {e}")
    return None

def get_page_via_search(term):
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

def get_website_url(title):
    url = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            infobox = soup.find('table', {'class': 'infobox'})
            if infobox:
                for th in infobox.find_all('th'):
                    if th.get_text() and 'Website' in th.get_text():
                        td = th.find_next_sibling('td')
                        if td:
                            a_tag = td.find('a')
                            if a_tag and a_tag.has_attr('href'):
                                link = a_tag['href']
                                if not link.startswith('http'):
                                    link = 'https://' + link.lstrip('/')
                                return link
                            text = td.get_text(strip=True)
                            if text:
                                return 'https://' + text
    except Exception as e:
        print(f"Error fetching website for {title}: {e}")
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

def is_district_category(title):
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
                if "districts of" in cat_title or "districts in" in cat_title:
                    return True
    except Exception as e:
        print(f"Error checking category for {title}: {e}")
    return False

def is_correct_page(title, summary, target_name, state):
    if not title or not summary:
        return False

    title_lower = title.lower()
    summary_lower = summary.lower()
    target_lower = target_name.lower()
    state_lower = state.lower()

    # MUST contain "district"
    if "district" not in title_lower and "district" not in summary_lower:
        return False

    # MUST contain target district name
    if target_lower not in title_lower and target_lower not in summary_lower:
        return False

    # MUST contain state name
    if state_lower not in summary_lower:
        return False

    # EXCLUDE unwanted pages
    blacklist = [
        "list of",
        "mandal",
        "taluk",
        "tehsil",
        "village",
        "assembly constituency",
        "lok sabha constituency",
        "division"
    ]

    for bad in blacklist:
        if bad in title_lower:
            return False

    return True

def extract_districts():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    
    print("Fetching PENDING districts or those missing website_url...")
    cursor.execute("SELECT * FROM wikipedia_districts WHERE status = 'PENDING' ")
    districts = cursor.fetchall()
    print(f"Found {len(districts)} districts to process.")
    
    for row in districts:
        name = row['district_name']
        state = row['state_name']
        code = row['district_code']
        
        print(f"Processing: {name}, {state}...")
        
        search_terms = [
            f"{name} district, {state}",
            f"{name} district {state}",
            f"{name} district",
        ]
        
        result = None
        for term in search_terms:
            result = get_page_direct(term)
            if result and is_correct_page(result['title'], result['summary'], name, state):
                # Extra accuracy: Check Wikipedia category
                if is_district_category(result['title']):
                    break
                else:
                    result = None # Close but not a district page
        
        if not result:
            result = get_page_via_search(f"{name} district {state}")
            if result:
                if not is_correct_page(result['title'], result['summary'], name, state) or not is_district_category(result['title']):
                    result = None
        
        status = 'FOUND' if result else 'NOT_FOUND'
        wiki_title = result['title'] if result else None
        wiki_url = result['url'] if result else None
        wiki_summary = result['summary'] if result else None
        
        lat, lon = (None, None)
        website_url = None
        if result:
            lat, lon = get_coordinates(wiki_title)
            website_url = get_website_url(wiki_title)

        upd_cursor = conn.cursor()
        upd_cursor.execute("""
            UPDATE wikipedia_districts 
            SET wiki_title = %s, wiki_url = %s, wiki_summary = %s, status = %s, latitude = %s, longitude = %s, website_url = %s
            WHERE district_code = %s
        """, (wiki_title, wiki_url, wiki_summary, status, lat, lon, website_url, code))
        conn.commit()
        upd_cursor.close()
        
        if result:
            print(f"  [FOUND] {wiki_title}")
        else:
            print(f"  [NOT FOUND]")
            
        time.sleep(1.5) # Polite delay
        
    cursor.close()
    conn.close()

if __name__ == "__main__":
    extract_districts()
