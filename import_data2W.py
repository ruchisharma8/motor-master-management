import pandas as pd
import psycopg2
import toml
import os
import uuid

# --- CONFIGURATION ---
# REPLACE THIS with the exact path to your CSV file
CSV_FILE_PATH = r"C:\Users\ruchi\Downloads\2W_Ensuredit_Master_ruchi.csv"

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

def import_csv_to_db():
    print("--- Starting 2W Data Import ---")
    
    # 1. Load Configuration
    db_config = load_db_config()
    conn = connect_db(db_config)
    cur = conn.cursor()
    
    try:
        # 2. Read CSV using Pandas
        # dtype=str ensures we read everything as text first to allow cleaning
        print(f"📖 Reading CSV from: {CSV_FILE_PATH}")
        df = pd.read_csv(CSV_FILE_PATH, dtype=str)
        
        # Clean column names (remove spaces)
        df.columns = [c.strip() for c in df.columns]
        
        print(f"✅ CSV Loaded. Found {len(df)} rows.")
        print("⏳ Inserting data into PostgreSQL... this might take a moment.")

        # 3. Prepare SQL Query with ON CONFLICT Clause
        # UPDATED: Added 'id' column and included it in values
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

        # 4. Iterate and Insert
        for index, row in df.iterrows():
            try:
                # Helper to handle NaN/None values safely
                def clean(val): return val if pd.notna(val) and str(val).strip() != '' else None
                def clean_int(val, default=0): 
                    if pd.notna(val) and str(val).strip() != '':
                        try:
                            return int(float(str(val)))
                        except:
                            return default
                    return default

                # Map CSV Row to Database Tuple
                # Ensure these keys match your CSV headers exactly
                data_tuple = (
                    str(uuid.uuid4()),              # Generated ID (Primary Key)
                    clean_int(row.get('productId'), 1),
                    clean(row.get('make')),
                    clean(row.get('model')),
                    clean(row.get('variant')),
                    clean_int(row.get('cc'), 0),
                    clean(row.get('fuelType')),     # Maps to fuelType in DB
                    clean(row.get('ensureditId')),  
                    clean(row.get('bodyType')),     
                    clean_int(row.get('seating'), 2),
                    clean_int(row.get('carrying'), 1),
                    # Insurers
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
                    clean(row.get('royalSundaram')), # Ensure CSV header matches this
                    clean(row.get('zuno')),
                    clean(row.get('kotak')),
                    clean(row.get('acko')),
                    clean(row.get('magma')),
                    clean(row.get('united'))
                )

                cur.execute(insert_query, data_tuple)
                
                # Check if the row was inserted or skipped
                if cur.rowcount > 0:
                    success_count += 1
                else:
                    skipped_count += 1 # Row existed, so DB ignored it

            except Exception as row_error:
                error_count += 1
                print(f"⚠️ Error on row {index + 2}: {row_error}")
                conn.rollback() # Rollback ONLY the failed row so we can continue
                continue

        # 5. Commit Transaction
        conn.commit()
        print("-" * 30)
        print(f"🎉 Import Finished!")
        print(f"✅ Successfully inserted (New): {success_count}")
        print(f"⏭️ Skipped (Duplicates): {skipped_count}")
        print(f"❌ Failed rows (Errors): {error_count}")

    except FileNotFoundError:
        print(f"❌ Error: File not found at {CSV_FILE_PATH}")
        print("Please check the path in the script.")
    except Exception as e:
        print(f"❌ Critical Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    import_csv_to_db()