import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from db_config import get_db

def get_connection():
    return get_db()

def setup_tables():
    conn = get_connection()
    cursor = conn.cursor()
    
    print("⚠️  WARNING: This script will DROP and recreate the following tables:")
    print("   - wikipedia_districts")
    print("   - wikipedia_subdistricts")
    print("   - wikipedia_villages")
    print("   - wikipedia_ulbs")
    print("   ALL existing Wikipedia data will be PERMANENTLY DELETED!")
    print()
    confirm = input("Type 'YES' to confirm and continue, or anything else to cancel: ").strip()
    if confirm != "YES":
        print("❌ Cancelled. No tables were dropped.")
        conn.close()
        return

    print("\nDropping existing Wikipedia tables (for fresh start)...")
    cursor.execute("DROP TABLE IF EXISTS wikipedia_districts")
    cursor.execute("DROP TABLE IF EXISTS wikipedia_subdistricts")
    cursor.execute("DROP TABLE IF EXISTS wikipedia_villages")
    cursor.execute("DROP TABLE IF EXISTS wikipedia_ulbs")


    print("Creating Wikipedia tables...")
    
    # 1. Districts
    print("Creating wikipedia_districts...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS wikipedia_districts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            district_code VARCHAR(10) UNIQUE,
            state_name VARCHAR(100),
            district_name VARCHAR(100),
            latitude DECIMAL(10,8),
            longitude DECIMAL(10,8),
            wiki_title VARCHAR(255),
            wiki_url VARCHAR(500),
            wiki_summary TEXT,
            status ENUM('PENDING', 'FOUND', 'NOT_FOUND', 'ERROR') DEFAULT 'PENDING',
            last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    """)

    # 2. Subdistricts
    print("Creating wikipedia_subdistricts...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS wikipedia_subdistricts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            subdistrict_code VARCHAR(10) UNIQUE,
            state_name VARCHAR(100),
            district_name VARCHAR(100),
            subdistrict_name VARCHAR(100),
            latitude DECIMAL(10,8),
            longitude DECIMAL(10,8),
            wiki_title VARCHAR(255),
            wiki_url VARCHAR(500),
            wiki_summary TEXT,
            status ENUM('PENDING', 'FOUND', 'NOT_FOUND', 'ERROR') DEFAULT 'PENDING',
            last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    """)

    # 3. Villages
    print("Creating wikipedia_villages...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS wikipedia_villages (
            id INT AUTO_INCREMENT PRIMARY KEY,
            village_code VARCHAR(20) UNIQUE,
            state_name VARCHAR(100),
            district_name VARCHAR(100),
            subdistrict_name VARCHAR(100),
            village_name VARCHAR(100),
            pincode VARCHAR(20),
            latitude DECIMAL(10,8),
            longitude DECIMAL(10,8),
            wiki_title VARCHAR(255),
            wiki_url VARCHAR(500),
            wiki_summary TEXT,
            status ENUM('PENDING', 'FOUND', 'NOT_FOUND', 'ERROR') DEFAULT 'PENDING',
            last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    """)

    # 4. ULBs (Towns)
    print("Creating wikipedia_ulbs...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS wikipedia_ulbs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ulb_code VARCHAR(20) UNIQUE,
            state_name VARCHAR(100),
            district_name VARCHAR(100),
            ulb_name VARCHAR(100),
            ulb_type VARCHAR(50), 
            latitude DECIMAL(10,8),
            longitude DECIMAL(10,8),
            wiki_title VARCHAR(255),
            wiki_url VARCHAR(500),
            wiki_summary TEXT,
            status ENUM('PENDING', 'FOUND', 'NOT_FOUND', 'ERROR') DEFAULT 'PENDING',
            last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    """)

    conn.commit()
    print("Tables created successfully.")
    
    # Pre-populate
    print("Pre-populating data...")
    
    # Districts
    print("  1. Districts...")
    cursor.execute("""
        INSERT IGNORE INTO wikipedia_districts (district_code, state_name, district_name)
        SELECT d.district_code, s.state_name, d.district_name
        FROM census_2011_districts d
        JOIN census_2011_states s ON d.state_code = s.state_code
    """)
    conn.commit()
    
    # Subdistricts
    print("  2. Subdistricts...")
    cursor.execute("""
        INSERT IGNORE INTO wikipedia_subdistricts (subdistrict_code, state_name, district_name, subdistrict_name)
        SELECT sd.subdistrict_code, s.state_name, d.district_name, sd.subdistrict_name
        FROM census_2011_subdistricts sd
        JOIN census_2011_states s ON sd.state_code = s.state_code
        JOIN census_2011_districts d ON sd.district_code = d.district_code
    """)
    conn.commit()

    # ULBs (Simplified, user will map later)
    print("  3. ULBs (Towns)...")
    cursor.execute("""
        INSERT IGNORE INTO wikipedia_ulbs (ulb_code, state_name, district_name, ulb_name, ulb_type)
        SELECT u.ulb_code, s.state_name, NULL, u.ulb_name, u.ulb_type
        FROM census_2011_ulbs u
        JOIN census_2011_states s ON u.state_code = s.state_code
    """)
    conn.commit()
    
    # Villages
    print("  4. Villages (Large batch, might take time)...")
    cursor.execute("""
        INSERT IGNORE INTO wikipedia_villages (village_code, state_name, district_name, subdistrict_name, village_name, pincode, latitude, longitude)
        SELECT v.village_code, s.state_name, d.district_name, sd.subdistrict_name, v.village_name, v.pincode, v.latitude, v.longitude
        FROM census_2011_villages v
        JOIN census_2011_states s ON v.state_code = s.state_code
        JOIN census_2011_districts d ON v.district_code = d.district_code
        JOIN census_2011_subdistricts sd ON v.subdistrict_code = sd.subdistrict_code
    """)
    conn.commit()
    print("Pre-population complete.")

    conn.commit()
    cursor.close()
    conn.close()
    print("Pre-population complete.")

if __name__ == "__main__":
    setup_tables()
