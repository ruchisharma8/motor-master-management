import pandas as pd
import psycopg2
import toml
import os
import re  # Added for cleaning control characters
import uuid # Added for generating unique IDs

# --- CONFIGURATION ---
CSV_FILE_PATH = r"C:\Users\ruchi\Downloads\4W_Ensuredit_Master_ruchi.csv"
TARGET_PRODUCT_ID = 2 
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
    Removes non-printable control characters (like 0x13) from strings.
    Keeps newlines (\n), tabs (\t), and carriage returns (\r).
    """
    if not isinstance(text, str):
        return text
    # Regex to find control characters excluding \n, \t, \r
    # 0x00-0x08, 0x0B-0x0C, 0x0E-0x1F (0x13 falls here)
    return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', text)

def import_csv_to_db():
    print(f"--- Starting Data Import for Product ID: {TARGET_PRODUCT_ID} ---")
    
    db_config = load_db_config()
    conn = connect_db(db_config)
    # Set autocommit to True so every successful insert is saved immediately.
    conn.autocommit = True 
    cur = conn.cursor()
    
    try:
        print(f"📖 Reading CSV from: {CSV_FILE_PATH}")
        df = pd.read_csv(CSV_FILE_PATH, dtype=str)
        df.columns = [c.strip() for c in df.columns]
        
        print(f"✅ CSV Loaded. Found {len(df)} rows.")
        print("⏳ Inserting data into PostgreSQL...")

        # UPDATED: Added 'id' column and changed 'fuel' to 'fuelType'
        insert_query = """
        INSERT INTO mmv_master (
            id, product_id, make, model, variant, cc, fuelType, ensuredit_id, body_type, 
            seating_capacity, carrying_capacity, 
            digit, icici, hdfc, reliance, bajaj, tata, sbi, future, iffco, chola, royalSundaram, zuno, kotak, acko, magma, united
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (ensuredit_id) DO NOTHING;
        """

        success_count = 0
        skipped_count = 0
        error_count = 0

        for index, row in df.iterrows():
            try:
                def clean(val): 
                    if pd.notna(val) and str(val).strip() != '':
                        # Clean the string value of control chars before returning
                        return clean_control_chars(str(val).strip())
                    return None
                
                def clean_int(val, default=0): 
                    if pd.notna(val) and str(val).strip() != '':
                        try: return int(float(str(val)))
                        except: return default
                    return default

                p_id = TARGET_PRODUCT_ID
                
                data_tuple = (
                    str(uuid.uuid4()),  # Generated Unique ID
                    p_id,
                    clean(row.get('make')),
                    clean(row.get('model')),
                    clean(row.get('variant')),
                    clean_int(row.get('cc'), 0),
                    clean(row.get('fuelType') or row.get('fuel')),
                    clean(row.get('ensureditId') or row.get('ensuredit_id')),
                    clean(row.get('bodyType') or row.get('body_type')),
                    clean_int(row.get('seating') or row.get('seating_capacity'), 5),
                    clean_int(row.get('carrying') or row.get('carrying_capacity'), 4),
                    # Insurers - Clean these specifically as they are JSON strings
                    clean(row.get('digit')),
                    clean(row.get('icici')),
                    clean(row.get('hdfc')),
                    clean(row.get('reliance')),
                    clean(row.get('bajaj')),
                    clean(row.get('tata')),
                    clean(row.get('sbi')),
                    clean(row.get('future')),
                    clean(row.get('iffco')),
                    clean(row.get('chola')),
                    clean(row.get('royalSundaram')),
                    clean(row.get('zuno')),
                    clean(row.get('kotak')),
                    clean(row.get('acko')),
                    clean(row.get('magma')),
                    clean(row.get('united'))
                )

                cur.execute(insert_query, data_tuple)
                
                if cur.rowcount > 0:
                    success_count += 1
                else:
                    skipped_count += 1
                
                # Optional: Print status every 1000 rows
                if (index + 1) % 1000 == 0:
                    print(f"   Processed {index + 1} rows...")

            except Exception as row_error:
                error_count += 1
                print(f"⚠️ Error on row {index + 2}: {row_error}")
                # No need to conn.rollback() because autocommit is True
                continue

        print("-" * 30)
        print(f"🎉 Import Finished for Product ID {TARGET_PRODUCT_ID}!")
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