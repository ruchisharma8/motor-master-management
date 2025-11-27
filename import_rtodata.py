import pandas as pd
import psycopg2
import toml
import os
import re
import uuid

# --- CONFIGURATION ---
# Path to your RTO CSV file
CSV_FILE_PATH = r"C:\Users\ruchi\Downloads\RTO_Ensuredit_Master_ruchi.csv"
SECRETS_PATH = ".streamlit/secrets.toml"

def load_db_config():
    try:
        secrets = toml.load(SECRETS_PATH)
        return secrets["postgres"]
    except Exception as e:
        print(f"❌ Error loading secrets: {e}")
        exit()

def connect_db(config):
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

def import_rto_to_db():
    print("--- Starting RTO Master Data Import ---")
    
    db_config = load_db_config()
    conn = connect_db(db_config)
    conn.autocommit = True 
    cur = conn.cursor()
    
    try:
        print(f"📖 Reading CSV from: {CSV_FILE_PATH}")
        df = pd.read_csv(CSV_FILE_PATH, dtype=str)
        # Clean column names: trim spaces
        df.columns = [c.strip() for c in df.columns]
        
        print(f"✅ CSV Loaded. Found {len(df)} rows.")
        print("⏳ Inserting data into PostgreSQL (rto_master)...")

        # Query matching the schema in init_db.py
        insert_query = """
        INSERT INTO rto_master (
            id, search_string, display_string, rto, city, state,
            chola, tata, iffco, icici, sbi, bajaj, reliance, hdfc, future, 
            zuno, kotak, magma, united, royalSundaram, shriram, digit, acko
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (id) DO NOTHING;
        """

        success_count = 0
        skipped_count = 0
        error_count = 0

        for index, row in df.iterrows():
            try:
                def clean(val): 
                    if pd.notna(val) and str(val).strip() != '':
                        return clean_control_chars(str(val).strip())
                    return None
                
                # FIX: Use the ID from the CSV row. 
                # Only generate a UUID if the CSV ID is missing.
                csv_id = clean(row.get('id'))
                record_id = csv_id if csv_id else str(uuid.uuid4())

                data_tuple = (
                    record_id,
                    clean(row.get('searchString')),
                    clean(row.get('displayString')),
                    clean(row.get('rto')),
                    clean(row.get('city')),
                    clean(row.get('state')),
                    # Insurers
                    clean(row.get('chola')),
                    clean(row.get('tata')),
                    clean(row.get('iffco')),
                    clean(row.get('icici')),
                    clean(row.get('sbi')),
                    clean(row.get('bajaj')),
                    clean(row.get('reliance')),
                    clean(row.get('hdfc')),
                    clean(row.get('future')),
                    clean(row.get('zuno')),
                    clean(row.get('kotak')),
                    clean(row.get('magma')),
                    clean(row.get('united')),
                    # Handle legacy header name 'royal' if 'royalSundaram' is missing
                    clean(row.get('royalSundaram') or row.get('royal')), 
                    clean(row.get('shriram')),
                    clean(row.get('digit')),
                    clean(row.get('acko'))
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
        print(f"🎉 RTO Import Finished!")
        print(f"✅ Successfully inserted: {success_count}")
        print(f"⏭️ Skipped: {skipped_count}")
        print(f"❌ Failed rows: {error_count}")

    except FileNotFoundError:
        print(f"❌ Error: File not found at {CSV_FILE_PATH}")
    except Exception as e:
        print(f"❌ Critical Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    import_rto_to_db()