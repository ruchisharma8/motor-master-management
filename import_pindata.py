import pandas as pd
import psycopg2
import toml
import os
import re

# --- CONFIGURATION ---
# REPLACE THIS with the exact path to your Pincode CSV file
CSV_FILE_PATH = r"C:\Users\ruchi\Downloads\PinCode_Ensuredit_Master_ruchi.csv"

# Path to your secrets file
SECRETS_PATH = ".streamlit/secrets.toml"

def load_db_config():
    """Loads database credentials from .streamlit/secrets.toml"""
    try:
        secrets = toml.load(SECRETS_PATH)
        return secrets["postgres"]
    except Exception as e:
        print(f"❌ Error loading secrets from {SECRETS_PATH}")
        print(f"Details: {e}")
        exit()

def connect_db(config):
    """Connects to PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            dbname=config["dbname"],
            user=config["user"],
            password=config["password"]
        )
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        exit()

def clean_control_chars(text):
    """
    Removes non-printable control characters from strings.
    """
    if not isinstance(text, str):
        return text
    return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', text)

def create_table_if_not_exists(conn):
    """Creates the pincode_master table if it doesn't exist"""
    # Note: Column names in Postgres are case-insensitive (lowercase) by default
    create_table_query = """
    CREATE TABLE IF NOT EXISTS pincode_master (
        pincode VARCHAR(50) PRIMARY KEY,
        district VARCHAR(255),
        city VARCHAR(255),
        state VARCHAR(255),
        
        icici TEXT,
        digit TEXT,
        reliance TEXT,
        hdfc TEXT,
        bajaj TEXT,
        tata TEXT,
        sbi TEXT,
        future TEXT,
        iffco TEXT,
        chola TEXT,
        kotak TEXT,
        acko TEXT,
        magma TEXT,
        zuno TEXT,
        royalSundaram TEXT,
        united TEXT,
        shriram TEXT,
        care TEXT,
        cigna TEXT,
        hdfclife TEXT,
        tataaia TEXT,
        hdfchealth TEXT,
        carecashless TEXT,
        nivabupa TEXT,
        cholapa TEXT,
        oic TEXT,
        tatamhg TEXT,
        icicihealth TEXT
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
            print("✅ Table 'pincode_master' checked/created successfully.")
    except Exception as e:
        print(f"❌ Failed to create table: {e}")
        conn.rollback()
        exit()

def import_csv_to_db():
    print("--- Starting Pincode Data Import ---")
    
    # 1. Load Configuration
    db_config = load_db_config()
    conn = connect_db(db_config)
    # Enable autocommit for immediate saves
    conn.autocommit = True
    
    # 2. Ensure Table Exists
    create_table_if_not_exists(conn)
    
    cur = conn.cursor()
    
    try:
        # 3. Read CSV using Pandas
        print(f"📖 Reading CSV from: {CSV_FILE_PATH}")
        df = pd.read_csv(CSV_FILE_PATH, dtype=str)
        
        # Clean column names: strip spaces and convert to lowercase
        # This ensures matching works regardless of CSV header casing (e.g., 'CareCashless' vs 'carecashless')
        df.columns = [c.strip().lower() for c in df.columns]
        
        print(f"✅ CSV Loaded. Found {len(df)} rows.")
        print("⏳ Inserting data into PostgreSQL...")

        # 4. Prepare SQL Query
        insert_query = """
        INSERT INTO pincode_master (
            pincode, district, city, state, 
            icici, digit, reliance, hdfc, bajaj, tata, sbi, future, 
            iffco, chola, kotak, acko, magma, zuno, royalSundaram, united, shriram,
            care, cigna, hdfclife, tataaia, hdfchealth, carecashless, nivabupa, 
            cholapa, oic, tatamhg, icicihealth
        ) VALUES (
            %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s
        )
        ON CONFLICT (pincode) DO NOTHING;
        """

        success_count = 0
        skipped_count = 0
        error_count = 0

        # 5. Iterate and Insert
        for index, row in df.iterrows():
            try:
                def clean(val): 
                    if pd.notna(val) and str(val).strip() != '':
                        return clean_control_chars(str(val).strip())
                    return None
                
                # Map CSV Row to Database Tuple
                # KEYS MUST BE LOWERCASE because we lowercased df.columns above
                data_tuple = (
                    clean(row.get('pincode')),
                    clean(row.get('district')),
                    clean(row.get('city')),
                    clean(row.get('state')),
                    
                    # General Insurers
                    clean(row.get('icici')),
                    clean(row.get('digit')),
                    clean(row.get('reliance')),
                    clean(row.get('hdfc')),
                    clean(row.get('bajaj')),
                    clean(row.get('tata')),
                    clean(row.get('sbi')),
                    clean(row.get('future')),
                    clean(row.get('iffco')),
                    clean(row.get('chola')),
                    clean(row.get('kotak')),
                    clean(row.get('acko')),
                    clean(row.get('magma')),
                    clean(row.get('zuno')),
                    clean(row.get('royalsundaram')), # Matches 'royalSundaram' in CSV (lowercased)
                    clean(row.get('united')),
                    clean(row.get('shriram')),
                    
                    # Health/Life Specific Insurers
                    clean(row.get('care')),
                    clean(row.get('cigna')),
                    clean(row.get('hdfclife')),
                    clean(row.get('tataaia')),
                    clean(row.get('hdfchealth')),
                    clean(row.get('carecashless')),
                    clean(row.get('nivabupa')),
                    clean(row.get('cholapa')),
                    clean(row.get('oic')),
                    clean(row.get('tatamhg')),
                    clean(row.get('icicihealth'))
                )

                cur.execute(insert_query, data_tuple)
                
                if cur.rowcount > 0:
                    success_count += 1
                else:
                    skipped_count += 1

                if (index + 1) % 1000 == 0:
                    print(f"   Processed {index + 1} rows...")

            except Exception as row_error:
                error_count += 1
                print(f"⚠️ Error on row {index + 2}: {row_error}")
                continue

        print("-" * 30)
        print(f"🎉 Pincode Import Finished!")
        print(f"✅ Successfully inserted (New): {success_count}")
        print(f"⏭️ Skipped (Duplicates): {skipped_count}")
        print(f"❌ Failed rows (Errors): {error_count}")

    except FileNotFoundError:
        print(f"❌ Error: File not found at {CSV_FILE_PATH}")
    except Exception as e:
        print(f"❌ Critical Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    import_csv_to_db()