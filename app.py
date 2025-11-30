import streamlit as st
import time

# --- 🔐 AUTHENTICATION LOGIC START ---
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

# 👇 FIX: Initialize the password key to prevent KeyError
if "password_input" not in st.session_state:
    st.session_state["password_input"] = ""

def check_password():
    def password_entered():
        # Use .get() for extra safety, though initialization above fixes it mostly
        entered_password = st.session_state.get("password_input", "")
        if entered_password == st.secrets["app_password"]:
            st.session_state.authenticated = True
            # We don't delete the key anymore to avoid complexity, just clear it
            st.session_state["password_input"] = "" 
        else:
            st.error("😕 Password incorrect")

    if not st.session_state.authenticated:
        st.title("🔒 Motor Data Portal")
        st.write("Please log in to access the Master Data.")
        st.text_input(
            "Enter Password:", 
            type="password", 
            key="password_input", 
            on_change=password_entered
        )
        st.stop()  # 🛑 App stops here if not logged in

# Run the check
if not st.session_state.authenticated:
    check_password()
# --- 🔐 AUTHENTICATION LOGIC END ---

import pandas as pd
import json
import uuid
import time

# --- DATABASE CONFIGURATION ---
USE_DATABASE = True 

if USE_DATABASE:
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
    except ImportError:
        st.error("psycopg2 library not found. Please install it using: pip install psycopg2-binary")

# --- CONFIGURATION & INITIALIZATION ---

INSURERS = [
    'icici', 'digit', 'reliance', 'hdfc', 'bajaj', 'tata', 'sbi', 'future',
    'iffco', 'chola', 'kotak', 'acko', 'magma', 'zuno', 'royalSundaram', 'united', 'shriram',
    'care', 'cigna', 'hdfcLife', 'tataAIA', 'hdfcHealth', 'careCashless', 'nivaBupa', 
    'cholaPA', 'oic', 'tataMhg', 'iciciHealth'
]

st.set_page_config(
    page_title="Motor Master Data Management",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- DATABASE CONNECTION HELPERS ---

def get_db_connection():
    if not USE_DATABASE: return None
    try:
        return psycopg2.connect(
            host=st.secrets["postgres"]["host"],
            port=st.secrets["postgres"]["port"],
            dbname=st.secrets["postgres"]["dbname"],
            user=st.secrets["postgres"]["user"],
            password=st.secrets["postgres"]["password"]
        )
    except Exception as e:
        st.error(f"Database Connection Error: {e}")
        return None

def run_query(query, params=None, fetch_one=False, fetch_all=False, commit=False):
    if not USE_DATABASE: return None
    conn = get_db_connection()
    if not conn: return None
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            if commit:
                conn.commit()
                return True
            if fetch_one:
                return cur.fetchone()
            if fetch_all:
                return cur.fetchall()
    except Exception as e:
        st.error(f"Query Error: {e}")
        if commit: conn.rollback()
        return None
    finally:
        conn.close()

# --- HELPER: Get Next RTO ID ---
def get_next_rto_id():
    if USE_DATABASE:
        # Finds the maximum numeric ID and adds 1
        sql = "SELECT MAX(CAST(id AS INTEGER)) as m FROM rto_master WHERE id ~ '^[0-9]+'"
        res = run_query(sql, fetch_one=True)
        if res and res['m'] is not None:
            return int(res['m']) + 1
    return 1

# ------------------------------------------------------------------
#  SHARED UTILITIES
# ------------------------------------------------------------------
# --- FIX START: Added product_id=None to arguments ---
def process_bulk_mapping_upload(table_name, id_col_name, insurer, df, overwrite_existing, product_id=None):
# --- FIX END ---
    if not USE_DATABASE:
        st.warning("Bulk upload is only available in Database mode.")
        return

    # Clean column names
    df.columns = df.columns.str.strip()

    id_map_col = None
    payload_map_col = None
    
    valid_id_cols = ['ensuredit_id', 'ensureditid', 'id', 'rto_id', 'pincode', 'pin_code']
    
    # Handle specific legacy case for Royal Sundaram
    search_insurer = insurer.lower()
    valid_payload_cols = ['json_payload', 'payload', 'data', search_insurer]
    if search_insurer == 'royalsundaram':
        valid_payload_cols.append('royal')

    for c in df.columns:
        cl = c.lower()
        if cl in valid_id_cols: id_map_col = c
        if cl in valid_payload_cols: payload_map_col = c

    if not id_map_col or not payload_map_col:
        st.error(f"CSV must contain a primary ID column {valid_id_cols} and a Data column (e.g., '{insurer}', 'json_payload', or 'data'). Found: {list(df.columns)}")
        return

    success_count = 0
    error_count = 0
    skipped_count = 0
    progress_bar = st.progress(0)
    
    conn = get_db_connection()
    if not conn: return

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            total_rows = len(df)
            for i, row in df.iterrows():
                rec_id = str(row[id_map_col]).strip()
                raw_payload = row[payload_map_col]
                clean_str = str(raw_payload).strip()
                if pd.isna(raw_payload) or clean_str.lower() in ['nan', 'none', 'null', '']:
                    clean_str = ""

                progress_bar.progress((i + 1) / total_rows)
                
                # Determine correct column name
                db_col = insurer.lower()
                
                # --- FIX START: Context-Aware Select Query ---
                if product_id is not None:
                    # Filter strictly by product_id to ensure we don't cross-contaminate 2W/4W
                    check_sql = f"SELECT {db_col} FROM {table_name} WHERE {id_col_name} = %s AND product_id = %s"
                    check_params = (rec_id, product_id)
                else:
                    check_sql = f"SELECT {db_col} FROM {table_name} WHERE {id_col_name} = %s"
                    check_params = (rec_id,)
                
                cur.execute(check_sql, check_params)
                result = cur.fetchone()
                # --- FIX END ---
                
                # Legacy check for Royal Sundaram column naming if needed
                current_val_db = None
                if result:
                    current_val_db = result.get(db_col)
                elif search_insurer == 'royalsundaram':
                    try:
                        # Also apply product_id filter to legacy check if applicable
                        if product_id is not None:
                            l_sql = f"SELECT royal FROM {table_name} WHERE {id_col_name} = %s AND product_id = %s"
                            l_params = (rec_id, product_id)
                        else:
                            l_sql = f"SELECT royal FROM {table_name} WHERE {id_col_name} = %s"
                            l_params = (rec_id,)
                            
                        cur.execute(l_sql, l_params)
                        legacy_res = cur.fetchone()
                        if legacy_res: current_val_db = legacy_res.get('royal')
                    except: pass

                if not result and current_val_db is None:
                    error_count += 1
                    continue

                should_update = False
                
                if overwrite_existing:
                    should_update = True
                else:
                    if not current_val_db or current_val_db == "" or current_val_db == "{}":
                        should_update = True if clean_str else False
                    elif str(current_val_db) != clean_str:
                         should_update = True

                if should_update:
                    val_to_save = clean_str if clean_str else None
                    
                    # --- FIX START: Context-Aware Update Query ---
                    if product_id is not None:
                         sql = f"UPDATE {table_name} SET {db_col} = %s WHERE {id_col_name} = %s AND product_id = %s"
                         params = (val_to_save, rec_id, product_id)
                    else:
                         sql = f"UPDATE {table_name} SET {db_col} = %s WHERE {id_col_name} = %s"
                         params = (val_to_save, rec_id)
                    # --- FIX END ---
                    
                    cur.execute(sql, params)
                    if cur.rowcount > 0: success_count += 1
                    else: error_count += 1
                else:
                    skipped_count += 1
            
            conn.commit()
            st.cache_data.clear()
            st.toast("Bulk Import Finished", icon="🏁")

            # Result Logic
            if success_count == 0 and error_count > 0:
                # Customized diagnosis based on whether product_id was enforced
                context_hint = " (e.g., uploading 2W/RTO/Pincode CSV while in 4W Workspace)" if product_id else ""
                
                st.error(
                    f"**Process Completed but No Records Updated**\n\n"
                    f"❌ **IDs Not Found:** {error_count}\n"
                    f"✅ **Updated:** {success_count}\n\n"
                    f"**Diagnosis:** The IDs in your CSV were not found in the **'{table_name}'** database table within the current scope.\n"
                    f"**Possible Cause:** You might be uploading **Wrong Master Data**{context_hint}. Please check your file."
                )
            elif error_count > 0:
                st.warning(
                    f"**Process Completed with Warnings**\n\n"
                    f"✅ **Updated:** {success_count} | ⏭️ **Skipped:** {skipped_count}\n"
                    f"⚠️ **IDs Not Found:** {error_count} (These rows were ignored as the IDs do not exist in the current context)"
                )
            else:
                st.success(f"**Result:** ✅ Updated: {success_count} | ⏭️ Skipped: {skipped_count} | ❌ Errors: {error_count}")
            
    except Exception as e:
        st.error(f"Transaction Error: {e}")
        conn.rollback()
    finally:
        conn.close()
# ------------------------------------------------------------------
#  SECTION A: RTO LOGIC & UI
# ------------------------------------------------------------------

@st.cache_data(ttl=600, show_spinner=False)
def get_all_rto_records():
    if USE_DATABASE:
        rows = run_query("SELECT * FROM rto_master ORDER BY CAST(id AS INTEGER) ASC", fetch_all=True)
        return rows if rows else []
    return []

def get_rto_record(record_id):
    if USE_DATABASE: return run_query("SELECT * FROM rto_master WHERE id = %s", (record_id,), fetch_one=True)
    return None

def update_rto_record(data):
    if USE_DATABASE:
        sql = "UPDATE rto_master SET search_string=%s, display_string=%s, rto=%s, city=%s, state=%s WHERE id=%s"
        params = (data['searchString'], data['displayString'], data['rto'], data['city'], data['state'], data['id'])
        res = run_query(sql, params, commit=True)
        if res: st.cache_data.clear()
        return res
    return False

def add_rto_record(data):
    if USE_DATABASE:
        exists = run_query("SELECT id FROM rto_master WHERE id=%s", (data['id'],), fetch_one=True)
        if exists: return False, "ID already exists."
        sql = "INSERT INTO rto_master (id, search_string, display_string, rto, city, state) VALUES (%s, %s, %s, %s, %s, %s)"
        params = (data['id'], data['searchString'], data['displayString'], data['rto'], data['city'], data['state'])
        success = run_query(sql, params, commit=True)
        if success: st.cache_data.clear()
        return (True, "Success") if success else (False, "Database Insert Failed")
    return False, "No Database"

def update_rto_insurer_mapping(rto_id, insurer, mapping_val):
    if USE_DATABASE:
        sql = f"UPDATE rto_master SET {insurer.lower()} = %s WHERE id = %s"
        run_query(sql, (mapping_val, rto_id), commit=True)
        st.cache_data.clear()
        st.success(f"Mapping for {insurer} updated successfully!")

def display_rto_form(is_edit_mode, record=None):
    import re  # Importing regex for validation

    form_title = "Update RTO" if is_edit_mode else "Add New RTO"
    
    suffix = str(record.get('id', 'new')) if (is_edit_mode and record) else "new"
    
    if is_edit_mode and record:
        current_id = record.get('id')
    else:
        current_id = get_next_rto_id()

    def sync_rto_fields():
        r = st.session_state.get(f"rto_code_{suffix}", "")
        c = st.session_state.get(f"rto_city_{suffix}", "")
        s = st.session_state.get(f"rto_state_{suffix}", "")
        
        parts_disp = [str(x).strip() for x in [r, c] if x and str(x).strip()]
        parts_search = [str(x).strip() for x in [r, c, s] if x and str(x).strip()]
        
        st.session_state[f"rto_disp_{suffix}"] = " ".join(parts_disp)
        st.session_state[f"rto_search_{suffix}"] = " ".join(parts_search)

    # --- HELPER: VALIDATION LOGIC ---
    def validate_rto_inputs(code, city_val, state_val):
        # 1. RTO Code Validation: STRICT 2 Alphabets + 2 Digits
        # Pattern ^[a-zA-Z]{2}[0-9]{2}$ ensures:
        # - Starts with exactly 2 letters (Case Insensitive)
        # - Ends with exactly 2 numbers
        # Rejects: "33" (No letters), "f6" (1 letter), "DL1" (1 number)
        if not code or not re.match(r"^[a-zA-Z]{2}[0-9]{2}$", str(code).strip()):
            st.error("❌ **Validation Error:** The RTO code format is invalid. Please enter a valid Indian RTO code (First 2 alphabets then 2 numeric), e.g., DL01, KA05, MH14.")
            return False

        # 2. Text-Only Validation for City and State
        text_pattern = r"^[a-zA-Z\s]+$"
        
        if not bool(re.match(text_pattern, str(city_val).strip())):
            st.error("❌ **Validation Error:** City must contain only Alphabets and Spaces.")
            return False
            
        if not bool(re.match(text_pattern, str(state_val).strip())):
            st.error("❌ **Validation Error:** State must contain only Alphabets and Spaces.")
            return False
            
        return True
    # --------------------------------

    with st.container():
        st.subheader(form_title)
        
        c1, c2, c3 = st.columns(3)
        rto_id = c1.text_input("ID(PK)", value=str(current_id), disabled=True, key=f"rto_id_{suffix}")
        
        def_rto = record.get('rto', '') if record else ""
        rto_code = c2.text_input("RTO Code", value=def_rto, key=f"rto_code_{suffix}", on_change=sync_rto_fields)
        
        def_city = record.get('city', '') if record else ""
        city = c3.text_input("City", value=def_city, key=f"rto_city_{suffix}", on_change=sync_rto_fields)
        
        c4, c5, c6 = st.columns(3)
        
        def_state = record.get('state', '') if record else ""
        state = c4.text_input("State", value=def_state, key=f"rto_state_{suffix}", on_change=sync_rto_fields)
        
        def_disp = record.get('display_string', record.get('displayString', '')) if record else ""
        def_search = record.get('search_string', record.get('searchString', '')) if record else ""

        disp_str = c5.text_input("Display String", value=def_disp, key=f"rto_disp_{suffix}")
        search_str = c6.text_input("Search String", value=def_search, key=f"rto_search_{suffix}")
        
        btn_txt = "Update RTO" if is_edit_mode else "Save New RTO"
        
        if st.button(btn_txt, type="primary", key=f"rto_btn_{suffix}"):
            # Apply Validation
            if not validate_rto_inputs(rto_code, city, state):
                return

            final_id = rto_id
            data = {"id": final_id, "searchString": search_str, "displayString": disp_str, "rto": rto_code, "city": city, "state": state}
            
            if is_edit_mode:
                update_rto_record(data)
                st.toast("RTO Updated", icon="✅")
                st.rerun()
            else:
                success, msg = add_rto_record(data)
                if success: 
                    st.toast("RTO Created", icon="🎉")
                    st.rerun()
                else: 
                    st.error(msg)
                    
def display_rto_mapping_workspace():
    st.header("RTO Insurer Mapping Workspace")
    
    with st.expander("📂 Bulk Upload RTO Mappings (CSV)", expanded=False):
        b_c1, b_c2 = st.columns([1, 2])
        target_insurer = b_c1.selectbox("Select Insurer", INSURERS, key="rto_bulk_ins")
        overwrite = b_c1.checkbox("Force Overwrite?", key="rto_ovr")
        uploaded_file = b_c2.file_uploader("Upload CSV (Columns: id, json_payload or data)", type=["csv"], key="rto_csv")
        if uploaded_file and st.button("Start RTO Bulk Upload"):
            df = pd.read_csv(uploaded_file)
            process_bulk_mapping_upload('rto_master', 'id', target_insurer, df, overwrite)

    st.markdown("---")
    
    all_rto = get_all_rto_records()
    rto_opts = {f"{r['id']} - {r.get('search_string', r.get('searchString', ''))}" : r['id'] for r in all_rto}

    sel_rto_display = st.selectbox("Select RTO to Map", list(rto_opts.keys()), key="rto_map_select")
    if not sel_rto_display: return
    rto_id = rto_opts[sel_rto_display]
    current_rto = get_rto_record(rto_id)
    
    def get_val(key):
        val = current_rto.get(key)
        if val is None: val = current_rto.get(key.lower())
        if val is None and key.lower() == 'royalsundaram': val = current_rto.get('royal')
        
        if isinstance(val, dict): return val
        if isinstance(val, str) and val.strip():
            try: return json.loads(val)
            except: return {}
        return {}

    tabs = st.tabs([i.upper() for i in INSURERS])
    
    with tabs[INSURERS.index('reliance')]:
        d = get_val('reliance')
        c1, c2 = st.columns(2)
        sid = c1.text_input("State ID", value=d.get('stateId', ''), key=f"rto_rel_sid_{rto_id}")
        rid = c2.text_input("Region ID", value=d.get('regionId', ''), key=f"rto_rel_rid_{rto_id}")
        j = json.dumps({"stateId": sid, "regionId": rid}) if sid or rid else ""
        
        st.markdown("JSON Payload")
        st.code(j if j else "{}", language="json")
        if st.button("Save Reliance", key=f"rto_s_rel_{rto_id}"): update_rto_insurer_mapping(rto_id, 'reliance', j)

    with tabs[INSURERS.index('chola')]:
        d = get_val('chola')
        d2, d4 = d.get('2W', {}), d.get('4W', {})
        c1, c2 = st.columns(2)
        c1.markdown("**2W Settings**"); c2.markdown("**4W Settings**")
        w2_r = c1.text_input("2W RTO", value=d2.get('RTO',''), key=f"rto_c2r_{rto_id}"); w2_sc = c1.text_input("2W State Code", value=d2.get('NUM_STATE_CODE',''), key=f"rto_c2sc_{rto_id}")
        w4_r = c2.text_input("4W RTO", value=d4.get('RTO',''), key=f"rto_c4r_{rto_id}"); w4_sc = c2.text_input("4W State Code", value=d4.get('NUM_STATE_CODE',''), key=f"rto_c4sc_{rto_id}")
        w2_lc = c1.text_input("2W Loc Code", value=d2.get('TXT_RTO_LOCATION_CODE',''), key=f"rto_c2lc_{rto_id}"); w4_lc = c2.text_input("4W Loc Code", value=d4.get('TXT_RTO_LOCATION_CODE',''), key=f"rto_c4lc_{rto_id}")
        w2_reg = c1.text_input("2W Reg State", value=d2.get('TXT_REGISTRATION_STATE_CODE',''), key=f"rto_c2rg_{rto_id}"); w4_reg = c2.text_input("4W Reg State", value=d4.get('TXT_REGISTRATION_STATE_CODE',''), key=f"rto_c4rg_{rto_id}")
        w2_desc = c1.text_input("2W Loc Desc", value=d2.get('TXT_RTO_LOCATION_DESC',''), key=f"rto_c2de_{rto_id}"); w4_desc = c2.text_input("4W Loc Desc", value=d4.get('TXT_RTO_LOCATION_DESC',''), key=f"rto_c4de_{rto_id}")
        
        pl = {"2W": {"RTO": w2_r, "NUM_STATE_CODE": w2_sc, "TXT_RTO_LOCATION_CODE": w2_lc, "TXT_RTO_LOCATION_DESC": w2_desc, "TXT_REGISTRATION_STATE_CODE": w2_reg}, 
              "4W": {"RTO": w4_r, "NUM_STATE_CODE": w4_sc, "TXT_RTO_LOCATION_CODE": w4_lc, "TXT_RTO_LOCATION_DESC": w4_desc, "TXT_REGISTRATION_STATE_CODE": w4_reg}}
        j = json.dumps(pl, indent=2)
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save Chola", key=f"rto_s_cho_{rto_id}"): update_rto_insurer_mapping(rto_id, 'chola', j)

    with tabs[INSURERS.index('tata')]:
        d = get_val('tata')
        t1, t2 = st.columns(2)
        tr = t1.text_input("RTO", value=d.get('RTO',''), key=f"rto_tr_{rto_id}"); tz = t1.text_input("Zone", value=d.get('ZONE',''), key=f"rto_tz_{rto_id}")
        tz2 = t1.text_input("Zone 2", value=d.get('ZONE2',''), key=f"rto_tz2_{rto_id}"); tp = t2.text_input("Place Reg", value=d.get('PLACE_REG',''), key=f"rto_tp_{rto_id}")
        tpn = t2.text_input("Place Reg No", value=d.get('PLACE_REG_NO',''), key=f"rto_tpn_{rto_id}"); trl = t2.text_input("Loc Code", value=d.get('RTO_LOCATION',''), key=f"rto_trl_{rto_id}")
        trn = t2.text_input("Loc Name", value=d.get('RTO_LOCATION_NAME',''), key=f"rto_trn_{rto_id}")
        j = json.dumps({"RTO": tr, "ZONE": tz, "ZONE2": tz2, "PLACE_REG": tp, "PLACE_REG_NO": tpn, "RTO_LOCATION": trl, "RTO_LOCATION_NAME": trn}, indent=2)
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save Tata", key=f"rto_s_tat_{rto_id}"): update_rto_insurer_mapping(rto_id, 'tata', j)

    with tabs[INSURERS.index('iffco')]:
        d = get_val('iffco')
        i1, i2 = st.columns(2)
        iz = i1.text_input("Zone", value=d.get('zone',''), key=f"rto_iz_{rto_id}"); ic = i1.text_input("City Name", value=d.get('cityName',''), key=f"rto_ic_{rto_id}")
        icd = i1.text_input("City Desc", value=d.get('cityDesc',''), key=f"rto_icd_{rto_id}"); isc = i2.text_input("State Code", value=d.get('stateCode',''), key=f"rto_isc_{rto_id}")
        j = json.dumps({"zone": iz, "cityDesc": icd, "cityName": ic, "stateCode": isc})
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save Iffco", key=f"rto_s_iff_{rto_id}"): update_rto_insurer_mapping(rto_id, 'iffco', j)

    with tabs[INSURERS.index('icici')]:
        d = get_val('icici')
        ri = d.get('rto', {})
        st.text_input("State Name", value=d.get('state',''), key=f"rto_ics_{rto_id}")
        c1, c2 = st.columns(2)
        r2 = c1.text_input("2W ID", value=ri.get('2W',''), key=f"rto_ir2_{rto_id}"); r4 = c2.text_input("4W ID", value=ri.get('4W',''), key=f"rto_ir4_{rto_id}")
        rg = c1.text_input("GCV ID", value=ri.get('GCV',''), key=f"rto_irg_{rto_id}"); rm = c2.text_input("MISCD ID", value=ri.get('MISCD',''), key=f"rto_irm_{rto_id}")
        st.caption("Use JSON editor below for complex PCV array")
        
        st.markdown("JSON Payload")
        j_raw = st.text_area("Raw Data", value=json.dumps(d, indent=2), label_visibility="collapsed", key=f"rto_ic_raw_{rto_id}")
        if st.button("Save ICICI", key=f"rto_s_ic_{rto_id}"): update_rto_insurer_mapping(rto_id, 'icici', j_raw)

    with tabs[INSURERS.index('sbi')]:
        d = get_val('sbi')
        s1, s2 = st.columns(2)
        src = s1.text_input("RTO Code", value=d.get('RTO_Code',''), key=f"rto_sbi_src_{rto_id}"); srz = s1.text_input("Zone", value=d.get('RTO_Zone',''), key=f"rto_srz_{rto_id}")
        sid = s1.text_input("State ID", value=d.get('State_ID',''), key=f"rto_sbi_sid_{rto_id}"); sreg = s2.text_input("Region", value=d.get('RTO_Region',''), key=f"rto_sbi_reg_{rto_id}")
        lid = s2.text_input("Loc ID", value=d.get('Location_ID',''), key=f"rto_sbi_lid_{rto_id}"); scl = s2.text_input("Cluster", value=d.get('RTO_Cluster',''), key=f"rto_sbi_cl_{rto_id}")
        sdc = s1.text_input("Dist Code", value=d.get('District_Code',''), key=f"rto_sdc_{rto_id}"); sln = s2.text_input("Loc Name", value=d.get('Location_Name',''), key=f"rto_sln_{rto_id}")
        j = json.dumps({"RTO_Code": src, "RTO_Zone": srz, "State_ID": sid, "RTO_Region": sreg, "Location_ID": lid, "RTO_Cluster": scl, "District_Code": sdc, "Location_Name": sln, "RTO_Blacklist": "No"}, indent=2)
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save SBI", key=f"rto_s_sbi_{rto_id}"): update_rto_insurer_mapping(rto_id, 'sbi', j)
    
    with tabs[INSURERS.index('bajaj')]:
        d = get_val('bajaj')
        b1, b2 = st.columns(2)
        br = b1.text_input("RTO", value=d.get('RTO',''), key=f"rto_br_{rto_id}"); bc = b1.text_input("City", value=d.get('CITY',''), key=f"rto_bc_{rto_id}")
        bz = b2.text_input("Zone", value=d.get('ZONE',''), key=f"rto_bz_{rto_id}"); bs = b2.text_input("State", value=d.get('STATE',''), key=f"rto_bs_{rto_id}")
        j = json.dumps({"RTO": br, "CITY": bc, "ZONE": bz, "STATE": bs}, indent=2)
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save Bajaj", key=f"rto_s_baj_{rto_id}"): update_rto_insurer_mapping(rto_id, 'bajaj', j)

    with tabs[INSURERS.index('hdfc')]:
        d = get_val('hdfc')
        h1, h2 = st.columns(2)
        hsid = h1.text_input("State ID", value=d.get('v1', {}).get('stateId',''), key=f"rto_hsid_{rto_id}")
        hm2 = h1.text_input("V1 2W", value=d.get('v1', {}).get('MOTOR_2W',''), key=f"rto_hm2_{rto_id}")
        hm4 = h1.text_input("V1 4W", value=d.get('v1', {}).get('MOTOR_4W',''), key=f"rto_hm4_{rto_id}")
        hh2 = h2.text_input("V2 2W", value=d.get('v2', {}).get('HDFC_2W',''), key=f"rto_hh2_{rto_id}")
        hh4 = h2.text_input("V2 4W", value=d.get('v2', {}).get('HDFC_4W',''), key=f"rto_hh4_{rto_id}")
        j = json.dumps({"v1": {"stateId": hsid, "MOTOR_2W": hm2, "MOTOR_4W": hm4}, "v2": {"HDFC_2W": hh2, "HDFC_4W": hh4, "HDFC_CV": ""}}, indent=2)
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save HDFC", key=f"rto_s_hdf_{rto_id}"): update_rto_insurer_mapping(rto_id, 'hdfc', j)

    with tabs[INSURERS.index('future')]:
        d = get_val('future')
        fc = st.text_input("Code", value=d.get('code',''), key=f"rto_fc_{rto_id}"); fz = st.text_input("Zone", value=d.get('zone',''), key=f"rto_fz_{rto_id}")
        fs = st.text_input("State", value=d.get('state',''), key=f"rto_fs_{rto_id}"); fp = st.text_input("Pincode", value=d.get('pincode',''), key=f"rto_fp_{rto_id}")
        fl = st.text_input("Long Desc", value=d.get('longdesc',''), key=f"rto_fl_{rto_id}")
        j = json.dumps({"code": fc, "zone": fz, "state": fs, "pincode": fp, "longdesc": fl}, indent=2)
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save Future", key=f"rto_s_fut_{rto_id}"): update_rto_insurer_mapping(rto_id, 'future', j)

    with tabs[INSURERS.index('zuno')]:
        d = get_val('zuno')
        z1, z2 = st.columns(2)
        zcz = z1.text_input("Car Zone", value=d.get('carZone',''), key=f"rto_zcz_{rto_id}"); zrz = z1.text_input("RTO Zone", value=d.get('rtoZone',''), key=f"rto_zrz_{rto_id}")
        zidv = z1.text_input("IDV City", value=d.get('idvCity',''), key=f"rto_zidv_{rto_id}"); zsn = z1.text_input("State Name", value=d.get('stateName',''), key=f"rto_zsn_{rto_id}")
        zcl = z2.text_input("Cluster", value=d.get('clusterZone',''), key=f"rto_zcl_{rto_id}"); zrsc = z2.text_input("RTO State Code", value=d.get('rtoStateCode',''), key=f"rto_zrsc_{rto_id}")
        zln = z2.text_input("Loc Name", value=d.get('rtoLocationName',''), key=f"rto_zln_{rto_id}"); zcd = z2.text_input("City/Dist", value=d.get('rtoCityOrDistrict',''), key=f"rto_zcd_{rto_id}")
        j = json.dumps({"carZone": zcz, "idvCity": zidv, "rtoZone": zrz, "stateName": zsn, "clusterZone": zcl, "rtoStateCode": zrsc, "rtoLocationName": zln, "rtoCityOrDistrict": zcd}, indent=2)
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save Zuno", key=f"rto_s_zun_{rto_id}"): update_rto_insurer_mapping(rto_id, 'zuno', j)
    
    with tabs[INSURERS.index('kotak')]:
        d = get_val('kotak')
        kz = st.text_input("Zone", value=d.get('zone',''), key=f"rto_kz_{rto_id}"); krc = st.text_input("RTO Code", value=d.get('rtoCode',''), key=f"rto_krc_{rto_id}")
        ksc = st.text_input("State Code", value=d.get('stateCode',''), key=f"rto_ksc_{rto_id}"); kcl = st.text_input("RTO Cluster", value=d.get('rtoCluster',''), key=f"rto_kcl_{rto_id}")
        j = json.dumps({"zone": kz, "rtoCode": krc, "stateCode": ksc, "rtoCluster": kcl}, indent=2)
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save Kotak", key=f"rto_s_kot_{rto_id}"): update_rto_insurer_mapping(rto_id, 'kotak', j)

    with tabs[INSURERS.index('magma')]:
        d = get_val('magma')
        mz = st.text_input("Zone", value=d.get('TXT_REGISTRATION_ZONE',''), key=f"rto_mz_{rto_id}")
        mlc = st.text_input("Loc Code", value=d.get('TXT_RTO_LOCATION_CODE',''), key=f"rto_mlc_{rto_id}")
        mld = st.text_input("Loc Desc", value=d.get('TXT_RTO_LOCATION_DESC',''), key=f"rto_mld_{rto_id}")
        j = json.dumps({"TXT_REGISTRATION_ZONE": mz, "TXT_RTO_LOCATION_CODE": mlc, "TXT_RTO_LOCATION_DESC": mld}, indent=2)
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save Magma", key=f"rto_s_mag_{rto_id}"): update_rto_insurer_mapping(rto_id, 'magma', j)

    with tabs[INSURERS.index('united')]:
        d = get_val('united')
        ud = st.text_input("RTA Desc", value=d.get('TXT_RTA_DESC',''), key=f"rto_ud_{rto_id}")
        uz = st.text_input("Zone", value=d.get('TXT_VEHICLE_ZONE',''), key=f"rto_uz_{rto_id}")
        j = json.dumps({"TXT_RTA_DESC": ud, "TXT_VEHICLE_ZONE": uz}, indent=2)
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save United", key=f"rto_s_uni_{rto_id}"): update_rto_insurer_mapping(rto_id, 'united', j)

    with tabs[INSURERS.index('royalSundaram')]:
        d = get_val('royalSundaram')
        rrto = st.text_input("RTO", value=d.get('rto',''), key=f"rto_rrto_{rto_id}")
        rcity = st.text_input("RTO City", value=d.get('rtoCity',''), key=f"rto_rcity_{rto_id}")
        rnm = st.text_input("RTO Name", value=d.get('rtoName',''), key=f"rto_rnm_{rto_id}")
        j = json.dumps({"rto": rrto, "rtoCity": rcity, "rtoName": rnm})
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save Royal", key=f"rto_s_roy_{rto_id}"): update_rto_insurer_mapping(rto_id, 'royalSundaram', j)

    with tabs[INSURERS.index('shriram')]:
        d = get_val('shriram')
        src = st.text_input("City", value=d.get('rtoCity',''), key=f"rto_shr_src_{rto_id}")
        srs = st.text_input("State", value=d.get('rtoState',''), key=f"rto_srs_{rto_id}")
        srcd = st.text_input("Code", value=d.get('rtoCode',''), key=f"rto_srcd_{rto_id}")
        j = json.dumps({"rtoCity": src, "rtoCode": srcd, "rtoState": srs}, indent=2)
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save Shriram", key=f"rto_s_shr_{rto_id}"): update_rto_insurer_mapping(rto_id, 'shriram', j)
    
    specific_rto_ins = ['reliance', 'chola', 'tata', 'iffco', 'icici', 'sbi', 'bajaj', 'hdfc', 'future', 'zuno', 'kotak', 'magma', 'united', 'royalSundaram', 'shriram']
    for i in range(len(INSURERS)):
        ins_name = INSURERS[i]
        if ins_name not in specific_rto_ins:
            with tabs[i]:
                st.info(f"Generic JSON Editor for {ins_name.upper()}")
                curr_val = get_val(ins_name)
                val_str = json.dumps(curr_val) if isinstance(curr_val, dict) else str(curr_val)
                
                st.markdown("JSON Payload")
                new_val = st.text_area("Raw JSON", value=val_str, label_visibility="collapsed", key=f"rto_gen_{ins_name}_{rto_id}")
                if st.button(f"Save {ins_name.upper()}", key=f"rto_s_{ins_name}_{rto_id}"): update_rto_insurer_mapping(rto_id, ins_name, new_val)

def display_rto_registry(key_suffix="main"):
    st.markdown(f"### Master Data Registry")
    rows = get_all_rto_records()
    if not rows: st.info("No RTO Data Found"); return
    df = pd.DataFrame(rows)
    st.dataframe(df, width="stretch", hide_index=True)
    
    st.markdown("### Raw Inspector")
    # Updated display logic: ID + Search String
    rto_opts = {}
    for r in rows:
        d_str = r.get('search_string', r.get('searchString', 'Unknown'))
        label = f"{r['id']} {d_str}"
        rto_opts[label] = r['id']
    
    sel = st.selectbox("Inspect Record", list(rto_opts.keys()), key=f"rto_insp_{key_suffix}")
    if sel: st.json(get_rto_record(rto_opts[sel]))

# ------------------------------------------------------------------
#  SECTION B: MMV (2W/4W) LOGIC & UI
# ------------------------------------------------------------------

@st.cache_data(ttl=600, show_spinner=False)
def get_all_mmv_records(product_id):
    if USE_DATABASE:
        # --- FIX EXPLANATION ---
        # 1. WHERE product_id = %s: STRICTLY separates 2W (1) and 4W (2) data.
        # 2. ORDER BY ensuredit_id ASC: Sorts by the generated ID. 
        #    Since IDs are generated sequentially (101 -> 102 -> 103), 
        #    newly created Makes/Models will appear at the BOTTOM of the list.
        rows = run_query(
            "SELECT * FROM mmv_master WHERE product_id = %s ORDER BY ensuredit_id ASC", 
            (product_id,), 
            fetch_all=True
        )
        return rows if rows else []
    return []

def get_mmv_record(record_id):
    if USE_DATABASE: return run_query("SELECT * FROM mmv_master WHERE id = %s", (record_id,), fetch_one=True)
    return None

def get_mmv_by_hierarchy(product_id, make, model, variant):
    if USE_DATABASE:
        sql = "SELECT * FROM mmv_master WHERE make=%s AND model=%s AND variant=%s AND product_id=%s"
        return run_query(sql, (make, model, variant, product_id), fetch_one=True)
    return None

def get_or_create_ids(product_id, make, model, variant):
    make_start = 101 if product_id == 1 else 401
    model_start = 101 
    if USE_DATABASE:
        # 1. Check if Make already exists
        make_res = run_query(
            "SELECT DISTINCT SUBSTRING(ensuredit_id, 1, 3) as mid FROM mmv_master WHERE make = %s AND product_id = %s LIMIT 1", 
            (make, product_id), 
            fetch_one=True
        )
        
        if make_res and make_res['mid']: 
            make_id = int(make_res['mid'])
        else:
            # 2. Find Max Make ID for this Product ID
            # FIX: Regex to ensure we only get valid integer IDs.
            query = "SELECT MAX(CAST(SUBSTRING(TRIM(ensuredit_id), 1, 3) AS INTEGER)) as m FROM mmv_master WHERE product_id = %s AND TRIM(ensuredit_id) ~ '^[0-9]+'"
            max_res = run_query(query, (product_id,), fetch_one=True)
            
            current_max = max_res['m'] if max_res and max_res['m'] else (make_start - 1)
            make_id = current_max + 1
        
        # 3. Check if Model already exists
        model_res = run_query(
            "SELECT DISTINCT SUBSTRING(ensuredit_id, 4, 3) as mid FROM mmv_master WHERE make = %s AND model = %s AND product_id = %s LIMIT 1", 
            (make, model, product_id), 
            fetch_one=True
        )
        
        if model_res and model_res['mid']: 
            model_id = int(model_res['mid'])
        else:
            # FIX: Proper parameter binding for LIKE
            prefix_make = f"{make_id}"
            query = "SELECT MAX(CAST(SUBSTRING(TRIM(ensuredit_id), 4, 3) AS INTEGER)) as m FROM mmv_master WHERE product_id = %s AND TRIM(ensuredit_id) LIKE %s"
            max_res = run_query(query, (product_id, f"{prefix_make}%"), fetch_one=True)
            current_model_max = max_res['m'] if max_res and max_res['m'] else (model_start - 1)
            model_id = current_model_max + 1

        # 4. Check if Variant already exists
        variant_res = run_query(
            "SELECT DISTINCT SUBSTRING(ensuredit_id, 7, 2) as vid FROM mmv_master WHERE make = %s AND model = %s AND variant = %s AND product_id = %s LIMIT 1", 
            (make, model, variant, product_id), 
            fetch_one=True
        )

        if variant_res and variant_res['vid']: 
            variant_id = int(variant_res['vid'])
        else:
            # FIX: Proper parameter binding for LIKE
            prefix_model = f"{make_id}{model_id}"
            query = "SELECT MAX(CAST(SUBSTRING(TRIM(ensuredit_id), 7, 2) AS INTEGER)) as v FROM mmv_master WHERE product_id = %s AND TRIM(ensuredit_id) LIKE %s"
            max_res = run_query(query, (product_id, f"{prefix_model}%"), fetch_one=True)
            variant_id = (max_res['v'] if max_res and max_res['v'] else 0) + 1

        return make_id, model_id, variant_id
    
    # Fallback if no DB
    return 101, 101, 1

def update_mmv_record(new_data):
    if USE_DATABASE:
        # FIX: Updated SQL to use 'fuelType' instead of 'fuel' to match DB schema
        sql = """
            UPDATE mmv_master 
            SET product_id=%s, make=%s, model=%s, variant=%s, fuelType=%s, cc=%s, body_type=%s, 
                seating_capacity=%s, carrying_capacity=%s, ensuredit_id=%s 
            WHERE id=%s
        """
        params = (
            new_data.get('product_id'), new_data['make'], new_data['model'], new_data['variant'], new_data['fuel'],
            new_data['cc'], new_data['body_type'], new_data['seating_capacity'], 
            new_data.get('carrying_capacity', 1), new_data['ensuredit_id'], new_data['id']
        )
        res = run_query(sql, params, commit=True)
        if res: st.cache_data.clear()
        return res
    return True

def add_mmv_record(new_data):
    if USE_DATABASE:
        exists = get_mmv_by_hierarchy(new_data['product_id'], new_data['make'], new_data['model'], new_data['variant'])
        if exists: return False, "This Make-Model-Variant combination already exists."
        # FIX: Updated SQL to use 'fuelType' instead of 'fuel'
        sql = """
            INSERT INTO mmv_master 
            (product_id, id, make, model, variant, fuelType, cc, body_type, seating_capacity, carrying_capacity, ensuredit_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            new_data.get('product_id'), new_data['id'], new_data['make'], new_data['model'], new_data['variant'],
            new_data['fuel'], new_data['cc'], new_data['body_type'],
            new_data['seating_capacity'], new_data.get('carrying_capacity', 1),
            new_data['ensuredit_id']
        )
        success = run_query(sql, params, commit=True)
        if success: st.cache_data.clear()
        return (True, "Success") if success else (False, "Database Insert Failed")
    return True, "Success"

def update_insurer_mapping_mmv(mmv_id, insurer, mapping_val):
    if USE_DATABASE:
        sql = f"UPDATE mmv_master SET {insurer.lower()} = %s WHERE id = %s"
        if mapping_val and insurer not in ['digit', 'zuno', 'royalSundaram']: 
             try: json.loads(mapping_val)
             except: 
                 st.error(f"Invalid JSON for {insurer}")
                 return
        run_query(sql, (mapping_val, mmv_id), commit=True)
        st.cache_data.clear()
        st.success(f"Mapping for {insurer} updated successfully!")

@st.cache_data(ttl=300)
def get_makes(product_id):
    if USE_DATABASE:
        rows = run_query("SELECT DISTINCT make FROM mmv_master WHERE product_id = %s ORDER BY make", (product_id,), fetch_all=True)
        return [r['make'] for r in rows] if rows else []
    return []

@st.cache_data(ttl=300)
def get_models(product_id, make):
    if USE_DATABASE:
        rows = run_query("SELECT DISTINCT model FROM mmv_master WHERE make=%s AND product_id=%s ORDER BY model", (make, product_id), fetch_all=True)
        return [r['model'] for r in rows] if rows else []
    return []

@st.cache_data(ttl=300)
def get_variants(product_id, make, model):
    if USE_DATABASE:
        rows = run_query("SELECT DISTINCT variant FROM mmv_master WHERE make=%s AND model=%s AND product_id=%s ORDER BY variant", (make, model, product_id), fetch_all=True)
        return [r['variant'] for r in rows] if rows else []
    return []

def display_mmv_form(product_id, is_edit_mode, record=None):
    import re  # Import regex for validation

    form_title = "Update Existing Record" if is_edit_mode else f"Define New {('2W' if product_id==1 else '4W')} Configuration"
    mmv_id = record.get('id') if record else str(uuid.uuid4())
    
    if is_edit_mode and record:
        current_make = record['make']
        current_model = record['model']
        current_variant = record['variant']
    else:
        current_make = st.session_state.get('last_make_selection')
        current_model = st.session_state.get('last_model_selection')
        current_variant = None

    suffix = record['id'] if (is_edit_mode and record) else "new"

    # --- HELPER: VALIDATION LOGIC ---
    def validate_mmv_inputs(mk, md, vt, cc_val):
        # 1. CC Validation: Must be positive
        if cc_val <= 0:
            st.error("❌ **Validation Error:** CC (Cubic Capacity) must be greater than 0.")
            return False

        # 2. Text Validation: Make/Model/Variant
        # Regex: Allows Alphanumeric, Spaces, Dots (.), Hyphens (-), and Brackets ()
        # Example valid: "Alto 800", "1.2 Kappa (O)", "GLS-350d"
        name_pattern = r"^[a-zA-Z0-9\s\.\-\(\)]+$"

        # Check Make
        if not mk or not mk.strip():
            st.error("❌ **Validation Error:** Make cannot be empty.")
            return False
        if not bool(re.match(name_pattern, str(mk).strip())):
            st.error(f"❌ **Validation Error:** Invalid Make name '{mk}'. Only alphabets, numbers, spaces, dots (.), hyphens (-), and brackets () are allowed.")
            return False

        # Check Model
        if not md or not md.strip():
            st.error("❌ **Validation Error:** Model cannot be empty.")
            return False
        if not bool(re.match(name_pattern, str(md).strip())):
            st.error(f"❌ **Validation Error:** Invalid Model name '{md}'. Only alphabets, numbers, spaces, dots (.), hyphens (-), and brackets () are allowed.")
            return False

        # Check Variant
        if not vt or not vt.strip():
            st.error("❌ **Validation Error:** Variant cannot be empty.")
            return False
        if not bool(re.match(name_pattern, str(vt).strip())):
            st.error(f"❌ **Validation Error:** Invalid Variant name '{vt}'. Only alphabets, numbers, spaces, dots (.), hyphens (-), and brackets () are allowed.")
            return False

        return True
    # --------------------------------

    with st.container():
        st.subheader(form_title)
        st.markdown("**1. Vehicle Hierarchy**")
        c1, c2, c3 = st.columns(3)
        with c1:
            make_opts = get_makes(product_id) + ["➕ Add New Make"]
            idx = make_opts.index(current_make) if is_edit_mode and current_make in make_opts else 0
            sel_make = st.selectbox("Make", make_opts, index=idx, key=f"sb_make_{product_id}_{suffix}")
            final_make = st.text_input("New Make Name", key=f"nm_{product_id}_{suffix}") if sel_make == "➕ Add New Make" else sel_make
        with c2:
            if final_make and final_make != "➕ Add New Make":
                model_opts = get_models(product_id, final_make) + ["➕ Add New Model"]
                idx = model_opts.index(current_model) if is_edit_mode and current_model in model_opts else 0
                sel_model = st.selectbox("Model", model_opts, index=idx, key=f"sb_mod_{product_id}_{suffix}")
                final_model = st.text_input("New Model Name", key=f"nmod_{product_id}_{suffix}") if sel_model == "➕ Add New Model" else sel_model
            else: final_model = st.text_input("New Model Name", key=f"nmod_f_{product_id}_{suffix}"); sel_model = "➕ Add New Model"
        with c3:
            if final_model and final_model != "➕ Add New Model":
                var_opts = get_variants(product_id, final_make, final_model) + ["➕ Add New Variant"]
                idx = var_opts.index(current_variant) if is_edit_mode and current_variant in var_opts else 0
                sel_variant = st.selectbox("Variant", var_opts, index=idx, key=f"sb_var_{product_id}_{suffix}")
                final_variant = st.text_input("New Variant Name", key=f"nvar_{product_id}_{suffix}") if sel_variant == "➕ Add New Variant" else sel_variant
            else: final_variant = st.text_input("New Variant Name", key=f"nvar_f_{product_id}_{suffix}"); sel_variant = "➕ Add New Variant"

        calc_id = "Pending..."
        if final_make and final_model and final_variant:
            found_record = get_mmv_by_hierarchy(product_id, final_make, final_model, final_variant)
            if found_record:
                record = found_record; mmv_id = found_record['id']; calc_id = found_record['ensuredit_id']
                suffix = mmv_id # Dynamic key update
            else:
                if USE_DATABASE:
                      mk_id, md_id, vt_id = get_or_create_ids(product_id, final_make, final_model, final_variant)
                      calc_id = f"{mk_id}{md_id}{int(vt_id):02d}"

        st.markdown("**2. Technical Specifications & ID**")
        s1, s2, s3, s4 = st.columns([2, 1, 2, 2])
        with s1:
            def get_index_case_insensitive(value, options):
                if not value: return 0
                val_str = str(value).lower().strip()
                for i, opt in enumerate(options):
                    if opt.lower().strip() == val_str:
                        return i
                return 0

            existing_fuel = record.get('fueltype', record.get('fuel', 'Petrol')) if record else 'Petrol'
            fuel_opts = ["Petrol", "Diesel", "Electric", "CNG", "Hybrid"]
            f_idx = get_index_case_insensitive(existing_fuel, fuel_opts)
            fuel = st.selectbox("Fuel Type", fuel_opts, index=f_idx, key=f"fl_{product_id}_{suffix}")
        
        with s2:
            cc = st.number_input("CC", value=(record.get('cc', 0) if record else 0), min_value=0, key=f"cc_{product_id}_{suffix}")
        with s3:
            if product_id == 1:
                 body_opts = ["Scooter", "Motorcycle", "Moped"]
            else:
                 body_opts = ["Sedan", "SUV", "Hatchback", "MUV", "Van", "Coupe", "Convertible", "Station Wagon", "Pickup"]
            
            ex_body = record.get('body_type', '') if record else ''
            b_idx = get_index_case_insensitive(ex_body, body_opts)
            if ex_body and b_idx == 0 and body_opts[0].lower() != str(ex_body).lower():
                body_opts.append(str(ex_body))
                b_idx = len(body_opts) - 1
            
            bodyType = st.selectbox("Body Type", body_opts, index=b_idx, key=f"bt_{product_id}_{suffix}")
        with s4:
            st.text_input("Ensuredit ID", value=calc_id, disabled=True, key=f"eid_{product_id}_{suffix}")

        seat_cap = 2 if product_id == 1 else 5
        carry_cap = 1 if product_id == 1 else 4 
        
        mk_id_fin, md_id_fin, vt_id_fin = get_or_create_ids(product_id, final_make, final_model, final_variant)
        final_ensuredit_id = f"{mk_id_fin}{md_id_fin}{int(vt_id_fin):02d}"
        
        new_data = {
            "product_id": product_id, "id": mmv_id, "make": final_make, "model": final_model, "variant": final_variant,
            "fuel": fuel, "cc": cc, "ensuredit_id": final_ensuredit_id, "body_type": bodyType,
            "seating_capacity": seat_cap, "carrying_capacity": carry_cap
        }

        btn_label = "Update Configuration" if is_edit_mode else "Create New MMV"
        
        if st.button(btn_label, type="primary", key=f"btn_main_{suffix}"):
            # --- Apply Validation ---
            if not validate_mmv_inputs(final_make, final_model, final_variant, cc):
                return
            # ------------------------

            if is_edit_mode:
                update_mmv_record(new_data)
                st.toast(f"Updated {final_make} {final_model}", icon="✅")
                st.rerun()
            else:
                success, msg = add_mmv_record(new_data)
                if success: 
                    # --- FIX: Celebration Icon ---
                    st.toast(f"Created {final_make}", icon="🎉") 
                    st.rerun()
                else: 
                    st.error(msg)

def display_insurer_mapping_form_mmv(product_id):
    st.header("Insurer Mapping Workspace")
    
    # Bulk Upload
    with st.expander("📂 Bulk Upload Mappings (CSV)", expanded=False):
        b_c1, b_c2 = st.columns([1, 2])
        target_insurer = b_c1.selectbox("1. Select Insurer", INSURERS, key="mmv_bulk_ins")
        overwrite = b_c1.checkbox("Force Overwrite?", key="mmv_ovr")
        uploaded_file = b_c2.file_uploader("2. Upload CSV (ensuredit_id, json_payload)", type=["csv"], key="mmv_csv")
        if uploaded_file and st.button(f"Start Bulk Update"):
            df = pd.read_csv(uploaded_file)
            process_bulk_mapping_upload('mmv_master', 'ensuredit_id', target_insurer, df, overwrite, product_id=product_id)

    st.markdown("---")
    
    # Manual Mapping
    all_records = get_all_mmv_records(product_id)
    mmv_options = {f"{r['make']} - {r['model']} - {r['variant']}": r['id'] for r in all_records}
    selected_mmv_display = st.selectbox("Select MMV to Map (Manual)", list(mmv_options.keys()), key="mmv_map_sel")
    if not selected_mmv_display: return

    selected_mmv_id = mmv_options[selected_mmv_display]
    current_mmv = get_mmv_record(selected_mmv_id)
    
    tab_labels = [i.upper() for i in INSURERS]
    tabs = st.tabs(tab_labels)
    
    def get_mapping_dict(insurer_key):
        val = current_mmv.get(insurer_key)
        if val is None: val = current_mmv.get(insurer_key.lower())
        
        if isinstance(val, dict): return val
        if isinstance(val, str) and val.strip():
            try: return json.loads(val)
            except: return {}
        return {}

    def get_mapping_str(insurer_key):
        val = current_mmv.get(insurer_key)
        if val is None: val = current_mmv.get(insurer_key.lower())
        
        if isinstance(val, str): return val
        return ""

    with tabs[INSURERS.index('icici')]: 
        st.markdown("##### ICICI Mapping")
        ic_data = get_mapping_dict('icici')
        c1, c2 = st.columns(2)
        mk_id = c1.text_input("ICICI Make ID", value=ic_data.get('makeId', ''), key=f"ic_mk_{selected_mmv_id}")
        md_id = c2.text_input("ICICI Model ID", value=ic_data.get('modelId', ''), key=f"ic_md_{selected_mmv_id}")
        
        if product_id == 2:
            c_seg = c1.text_input("Car Segment", value=ic_data.get('carsegment', ''), key=f"ic_seg_{selected_mmv_id}")
            c_seat = c2.text_input("Seating Cap", value=ic_data.get('seatingCapacity', ''), key=f"ic_seat_{selected_mmv_id}")
            ic_pl = {"makeId": mk_id, "modelId": md_id, "carsegment": c_seg, "seatingCapacity": c_seat}
        else:
            ic_pl = {"makeId": mk_id, "modelId": md_id}

        ic_json = json.dumps(ic_pl, indent=2) if (mk_id or md_id) else ""
        
        st.markdown("JSON Payload")
        st.code(ic_json if ic_json else "{}", language="json")
        if st.button("Save ICICI", key=f"btn_ic_{selected_mmv_id}"):
            update_insurer_mapping_mmv(selected_mmv_id, 'icici', ic_json)
            st.rerun()

    with tabs[INSURERS.index('digit')]: 
        st.markdown("##### Digit Mapping")
        d_val = get_mapping_str('digit')
        d_code = st.text_input("Digit Vehicle Code", value=d_val, key=f"dig_code_{selected_mmv_id}")
        if st.button("Save Digit", key=f"btn_dig_{selected_mmv_id}"):
            update_insurer_mapping_mmv(selected_mmv_id, 'digit', d_code)
            st.rerun()

    with tabs[INSURERS.index('reliance')]: 
        st.markdown("##### Reliance Mapping")
        rel_data = get_mapping_dict('reliance')
        r1, r2 = st.columns(2)
        r_mid = r1.text_input("Make ID", value=rel_data.get('makeId', ''), key=f"rel_mid_{selected_mmv_id}")
        r_modid = r1.text_input("Model ID", value=rel_data.get('modelId', ''), key=f"rel_modid_{selected_mmv_id}")
        r_mname = r1.text_input("Make Name", value=rel_data.get('makeName', ''), key=f"rel_mname_{selected_mmv_id}")
        r_moname = r1.text_input("Model Name", value=rel_data.get('modelName', ''), key=f"rel_moname_{selected_mmv_id}")
        r_cc = r2.text_input("CC", value=rel_data.get('cc', ''), key=f"rel_cc_{selected_mmv_id}")
        r_fuel = r2.text_input("Fuel Type", value=rel_data.get('fuelType', ''), key=f"rel_fuel_{selected_mmv_id}")
        r_seat = r2.text_input("Seating", value=rel_data.get('seatingCapacity', ''), key=f"rel_seat_{selected_mmv_id}")
        r_carry = r2.text_input("Carrying", value=rel_data.get('caryingCapacity', ''), key=f"rel_carry_{selected_mmv_id}")

        if any([r_mid, r_modid]):
            rel_pl = {"cc": r_cc, "makeId": r_mid, "modelId": r_modid, "fuelType": r_fuel, "makeName": r_mname, "modelName": r_moname, "caryingCapacity": r_carry, "seatingCapacity": r_seat}
            rel_json = json.dumps(rel_pl, indent=2)
        else: rel_json = ""
        
        st.markdown("JSON Payload")
        st.code(rel_json if rel_json else "{}", language="json")
        if st.button("Save Reliance", key=f"btn_rel_{selected_mmv_id}"):
            update_insurer_mapping_mmv(selected_mmv_id, 'reliance', rel_json)
            st.rerun()

    with tabs[INSURERS.index('hdfc')]: 
        st.markdown("##### HDFC Mapping")
        hdfc_data = get_mapping_dict('hdfc')
        v1 = hdfc_data.get('v1', {})
        v2 = hdfc_data.get('v2', {})
        h1, h2 = st.columns(2)
        h_mc = h1.text_input("Make Code (v1)", value=v1.get('makeCode', ''), key=f"hdfc_mc_{selected_mmv_id}")
        h_moc = h1.text_input("Model Code (v1)", value=v1.get('modelCode', ''), key=f"hdfc_moc_{selected_mmv_id}")
        h_mname = h2.text_input("Make Name", value=v2.get('MAKE', ''), key=f"hdfc_mn_{selected_mmv_id}")
        h_moname = h2.text_input("Model Name", value=v2.get('MODEL', ''), key=f"hdfc_mon_{selected_mmv_id}")
        h_var = h2.text_input("Variant Name", value=v2.get('VARIANT', ''), key=f"hdfc_var_{selected_mmv_id}")
        h_cc = h1.text_input("Cubic Capacity", value=v2.get('CUBIC_CAPACITY', ''), key=f"hdfc_cc_{selected_mmv_id}")
        h_seat = h1.text_input("Seating", value=v2.get('SEATING_CAPACITY', ''), key=f"hdfc_seat_{selected_mmv_id}")
        h_carry = h1.text_input("Carrying", value=v2.get('CARRYING_CAPACITY', ''), key=f"hdfc_carry_{selected_mmv_id}")
        h_fuel = h2.text_input("Fuel Type", value=v2.get('FUEL_TYPE', ''), key=f"hdfc_fuel_{selected_mmv_id}")
        h_wht = h2.text_input("Weight", value=v2.get('WEIGHT', ''), key=f"hdfc_wht_{selected_mmv_id}")
        h_whl = h2.text_input("Wheels", value=v2.get('WHEELS', ''), key=f"hdfc_whl_{selected_mmv_id}")
        h_mc2 = h2.text_input("Model Code (v2)", value=v2.get('MODEL_CODE', ''), key=f"hdfc_mc2_{selected_mmv_id}")

        if h_mc or h_moc:
            hdfc_pl = {"v1": {"makeCode": h_mc, "modelCode": h_moc}, "v2": {"MAKE": h_mname, "MODEL": h_moname, "VARIANT": h_var, "FUEL_TYPE": h_fuel, "CUBIC_CAPACITY": h_cc, "SEATING_CAPACITY": h_seat, "CARRYING_CAPACITY": h_carry, "WEIGHT": h_wht, "WHEELS": h_whl, "MODEL_CODE": h_mc2}}
            hdfc_json = json.dumps(hdfc_pl, indent=2)
        else: hdfc_json = ""
        
        st.markdown("JSON Payload")
        st.code(hdfc_json if hdfc_json else "{}", language="json")
        if st.button("Save HDFC", key=f"btn_hdfc_{selected_mmv_id}"):
            update_insurer_mapping_mmv(selected_mmv_id, 'hdfc', hdfc_json)
            st.rerun()

    with tabs[INSURERS.index('bajaj')]:
        st.markdown("##### Bajaj Mapping")
        b_data = get_mapping_dict('bajaj')
        b1, b2 = st.columns(2)
        b_vc = b1.text_input("Vehicle Code", value=b_data.get('VEHICLECODE', ''), key=f"baj_vc_{selected_mmv_id}")
        
        def_vt = "Private Car" if product_id == 2 else "Two Wheeler"
        b_vt = b1.text_input("Vehicle Type", value=b_data.get('VEHICLETYPE', def_vt), key=f"baj_vt_{selected_mmv_id}")
        
        b_mkc = b1.text_input("Make Code", value=b_data.get('VEHICLEMAKECODE', ''), key=f"baj_mkc_{selected_mmv_id}")
        b_mk = b1.text_input("Make Name", value=b_data.get('VEHICLEMAKE', ''), key=f"baj_mk_{selected_mmv_id}")
        b_mdc = b2.text_input("Model Code", value=b_data.get('VEHICLEMODELCODE', ''), key=f"baj_mdc_{selected_mmv_id}")
        b_md = b2.text_input("Model Name", value=b_data.get('VEHICLEMODEL', ''), key=f"baj_md_{selected_mmv_id}")
        b_stc = b2.text_input("Subtype Code", value=b_data.get('VEICLESUBTYPECODE', ''), key=f"baj_stc_{selected_mmv_id}")
        b_st = b2.text_input("Subtype Name", value=b_data.get('VEHICLESUBTYPE', ''), key=f"baj_st_{selected_mmv_id}")
        b_fuel = b1.text_input("Fuel (P/D)", value=b_data.get('FUEL', ''), key=f"baj_fl_{selected_mmv_id}")
        b_cc = b1.text_input("CC", value=b_data.get('CUBICCAPACITY', ''), key=f"baj_cc_{selected_mmv_id}")
        b_cry = b2.text_input("Carrying", value=b_data.get('CARRYINGCAPACITY', ''), key=f"baj_cry_{selected_mmv_id}")

        if b_vc:
            baj_pl = {"VEHICLECODE": b_vc, "VEHICLETYPE": b_vt, "VEHICLEMAKECODE": b_mkc, "VEHICLEMAKE": b_mk, "VEHICLEMODELCODE": b_mdc, "VEHICLEMODEL": b_md, "VEHICLESUBTYPECODE": b_stc, "VEHICLESUBTYPE": b_st, "FUEL": b_fuel, "CUBICCAPACITY": b_cc, "CARRYINGCAPACITY": b_cry}
            baj_json = json.dumps(baj_pl, indent=2)
        else: baj_json = ""
        
        st.markdown("JSON Payload")
        st.code(baj_json if baj_json else "{}", language="json")
        if st.button("Save Bajaj", key=f"btn_baj_{selected_mmv_id}"):
            update_insurer_mapping_mmv(selected_mmv_id, 'bajaj', baj_json)
            st.rerun()

    with tabs[INSURERS.index('tata')]:
        st.markdown("##### Tata Mapping")
        t_data = get_mapping_dict('tata')
        t1, t2 = st.columns(2)
        t_mk = t1.text_input("Make Name", value=t_data.get('VEHICLE_MAKE', ''), key=f"tata_mk_{selected_mmv_id}")
        t_mk_no = t1.text_input("Make No", value=t_data.get('VEHICLE_MAKE_NO', ''), key=f"tata_mkno_{selected_mmv_id}")
        t_md = t2.text_input("Model Name", value=t_data.get('VEHICLE_MODEL', ''), key=f"tata_md_{selected_mmv_id}")
        t_md_no = t2.text_input("Model No", value=t_data.get('VEHICLE_MODEL_NO', ''), key=f"tata_mdno_{selected_mmv_id}")
        t_var = t1.text_input("Variant", value=t_data.get('VEHICLE_VARIANT', ''), key=f"tata_var_{selected_mmv_id}")
        t_var_no = t1.text_input("Variant No", value=t_data.get('VEHICLE_VARIANT_NO', ''), key=f"tata_varno_{selected_mmv_id}")
        t_seat = t2.text_input("Seating", value=t_data.get('SEATING_CAPACITY', ''), key=f"tata_seat_{selected_mmv_id}")

        if t_mk_no or t_md_no:
            tata_pl = {"VEHICLE_MAKE": t_mk, "VEHICLE_MODEL": t_md, "VEHICLE_MAKE_NO": t_mk_no, "VEHICLE_VARIANT": t_var, "SEATING_CAPACITY": t_seat, "VEHICLE_MODEL_NO": t_md_no, "VEHICLE_VARIANT_NO": t_var_no}
            tata_json = json.dumps(tata_pl, indent=2)
        else: tata_json = ""
        
        st.markdown("JSON Payload")
        st.code(tata_json if tata_json else "{}", language="json")
        if st.button("Save Tata", key=f"btn_tata_{selected_mmv_id}"):
            update_insurer_mapping_mmv(selected_mmv_id, 'tata', tata_json)
            st.rerun()

    with tabs[INSURERS.index('sbi')]:
        st.markdown("##### SBI Mapping")
        sbi_data = get_mapping_dict('sbi')
        s1, s2 = st.columns(2)
        s_mid = s1.text_input("Make ID", value=sbi_data.get('MAKE_ID', ''), key=f"sbi_mid_{selected_mmv_id}")
        s_moid = s1.text_input("Model ID", value=sbi_data.get('MODEL_ID', ''), key=f"sbi_moid_{selected_mmv_id}")
        s_vid = s1.text_input("Variant ID", value=sbi_data.get('VARIANT_ID', ''), key=f"sbi_vid_{selected_mmv_id}")
        s_vname = s1.text_input("Variant Name", value=sbi_data.get('VARIANT_NAME', ''), key=f"sbi_vname_{selected_mmv_id}")
        s_cc = s2.text_input("CC", value=sbi_data.get('CC', ''), key=f"sbi_cc_{selected_mmv_id}")
        s_seat = s2.text_input("Seating", value=sbi_data.get('SEATING', ''), key=f"sbi_seat_{selected_mmv_id}")
        s_carry = s2.text_input("Carrying", value=sbi_data.get('CARRYING', ''), key=f"sbi_carry_{selected_mmv_id}")
        s_fuel = s2.text_input("Fuel Type Code", value=sbi_data.get('FUEL_TYPE', ''), key=f"sbi_fuel_{selected_mmv_id}")
        s_whl = s2.text_input("Wheels", value=sbi_data.get('WHEELS', ''), key=f"sbi_whl_{selected_mmv_id}")
        
        s_body = s1.text_input("Body Style", value=sbi_data.get('BODYSTYLE', ''), key=f"sbi_body_{selected_mmv_id}") if product_id == 2 else ""

        if s_mid:
            sbi_pl = {"CC": s_cc, "WHEELS": s_whl, "MAKE_ID": s_mid, "SEATING": s_seat, "CARRYING": s_carry, "MODEL_ID": s_moid, "FUEL_TYPE": s_fuel, "VARIANT_ID": s_vid, "VARIANT_NAME": s_vname}
            if s_body: sbi_pl["BODYSTYLE"] = s_body
            sbi_json = json.dumps(sbi_pl, indent=2)
        else: sbi_json = ""
        
        st.markdown("JSON Payload")
        st.code(sbi_json if sbi_json else "{}", language="json")
        if st.button("Save SBI", key=f"btn_sbi_{selected_mmv_id}"):
            update_insurer_mapping_mmv(selected_mmv_id, 'sbi', sbi_json)
            st.rerun()

    with tabs[INSURERS.index('future')]:
        st.markdown("##### Future Generali Mapping")
        f_data = get_mapping_dict('future')
        f1, f2 = st.columns(2)
        f_vc = f1.text_input("Vehicle Code", value=f_data.get('vehicleCode', ''), key=f"fut_vc_{selected_mmv_id}")
        f_mk = f1.text_input("Make", value=f_data.get('make', ''), key=f"fut_mk_{selected_mmv_id}")
        f_md = f1.text_input("Model", value=f_data.get('model', ''), key=f"fut_md_{selected_mmv_id}")
        f_bt = f1.text_input("Body Type", value=f_data.get('bodyType', ''), key=f"fut_bt_{selected_mmv_id}")
        f_fc = f2.text_input("Fuel Code", value=f_data.get('fuelCode', ''), key=f"fut_fc_{selected_mmv_id}")
        f_cc = f2.text_input("CC", value=f_data.get('cc', ''), key=f"fut_cc_{selected_mmv_id}")
        f_seat = f2.text_input("Seating", value=f_data.get('seatingCapacity', ''), key=f"fut_seat_{selected_mmv_id}")
        f_carry = f2.text_input("Carrying", value=f_data.get('carryingCapacity', ''), key=f"fut_carry_{selected_mmv_id}")

        if f_vc:
            fut_pl = {"cc": f_cc, "make": f_mk, "model": f_md, "bodyType": f_bt, "fuelCode": f_fc, "vehicleCode": f_vc, "seatingCapacity": f_seat, "carryingCapacity": f_carry}
            fut_json = json.dumps(fut_pl, indent=2)
        else: fut_json = ""
        
        st.markdown("JSON Payload")
        st.code(fut_json if fut_json else "{}", language="json")
        if st.button("Save Future", key=f"btn_fut_{selected_mmv_id}"):
            update_insurer_mapping_mmv(selected_mmv_id, 'future', fut_json)
            st.rerun()

    with tabs[INSURERS.index('iffco')]:
        st.markdown("##### IFFCO Mapping")
        i_data = get_mapping_dict('iffco')
        i1, i2 = st.columns(2)
        i_mc = i1.text_input("Make Code", value=i_data.get('makeCode', ''), key=f"iff_mc_{selected_mmv_id}")
        i_cc = i1.text_input("CC", value=i_data.get('CC', ''), key=f"iff_cc_{selected_mmv_id}")
        i_seat = i2.text_input("Seating", value=i_data.get('seatingCapacity', ''), key=f"iff_seat_{selected_mmv_id}")
        i_fy = i2.text_input("Mfg From Year", value=i_data.get('manufactureFromYear', ''), key=f"iff_fy_{selected_mmv_id}")
        i_ty = i2.text_input("Mfg To Year", value=i_data.get('manufactureToYear', ''), key=f"iff_ty_{selected_mmv_id}")

        if i_mc:
            iff_pl = {"CC": i_cc, "makeCode": i_mc, "seatingCapacity": i_seat, "manufactureToYear": i_ty, "manufactureFromYear": i_fy}
            iff_json = json.dumps(iff_pl, indent=2)
        else: iff_json = ""
        
        st.markdown("JSON Payload")
        st.code(iff_json if iff_json else "{}", language="json")
        if st.button("Save IFFCO", key=f"btn_iff_{selected_mmv_id}"):
            update_insurer_mapping_mmv(selected_mmv_id, 'iffco', iff_json)
            st.rerun()

    with tabs[INSURERS.index('chola')]:
        st.markdown("##### Chola Mapping")
        c_data = get_mapping_dict('chola')
        c1, c2 = st.columns(2)
        c_vmc = c1.text_input("Vehicle Model Code", value=c_data.get('VEHICLE_MODEL_CODE', ''), key=f"ch_vmc_{selected_mmv_id}")
        c_mfr = c1.text_input("Manufacturer", value=c_data.get('MANUFACTURER', ''), key=f"ch_mfr_{selected_mmv_id}")
        c_mod = c2.text_input("Vehicle Model Name", value=c_data.get('VEHICLE_MODEL', ''), key=f"ch_mod_{selected_mmv_id}")
        c_cc = c2.text_input("CC", value=c_data.get('CC', ''), key=f"ch_cc_{selected_mmv_id}")

        if c_vmc:
            cho_pl = {"CC": c_cc, "MANUFACTURER": c_mfr, "VEHICLE_MODEL": c_mod, "VEHICLE_MODEL_CODE": c_vmc}
            cho_json = json.dumps(cho_pl, indent=2)
        else: cho_json = ""
        
        st.markdown("JSON Payload")
        st.code(cho_json if cho_json else "{}", language="json")
        if st.button("Save Chola", key=f"btn_cho_{selected_mmv_id}"):
            update_insurer_mapping_mmv(selected_mmv_id, 'chola', cho_json)
            st.rerun()

    with tabs[INSURERS.index('kotak')]:
        st.markdown("##### Kotak Mapping")
        k_data = get_mapping_dict('kotak')
        k1, k2 = st.columns(2)
        k_mc = k1.text_input("Make Code", value=k_data.get('makeCode', ''), key=f"kot_mc_{selected_mmv_id}")
        k_moc = k1.text_input("Model Code", value=k_data.get('modelCode', ''), key=f"kot_moc_{selected_mmv_id}")
        k_vc = k1.text_input("Variant Code", value=k_data.get('variantCode', ''), key=f"kot_vc_{selected_mmv_id}")
        k_mod = k2.text_input("Model Name", value=k_data.get('model', ''), key=f"kot_mod_{selected_mmv_id}")
        k_var = k2.text_input("Variant Name", value=k_data.get('variant', ''), key=f"kot_var_{selected_mmv_id}")
        k_cl = k2.text_input("Cluster", value=k_data.get('modelCluster', ''), key=f"kot_cl_{selected_mmv_id}")
        k_seg = k2.text_input("Segment", value=k_data.get('modelSegment', ''), key=f"kot_seg_{selected_mmv_id}")
        k_seat = k2.text_input("Seating", value=k_data.get('seatingCapacity', ''), key=f"kot_seat_{selected_mmv_id}")

        if k_mc:
            kot_pl = {"model": k_mod, "variant": k_var, "makeCode": k_mc, "modelCode": k_moc, "variantCode": k_vc, "modelCluster": k_cl, "modelSegment": k_seg, "seatingCapacity": k_seat}
            kot_json = json.dumps(kot_pl, indent=2)
        else: kot_json = ""
        
        st.markdown("JSON Payload")
        st.code(kot_json if kot_json else "{}", language="json")
        if st.button("Save Kotak", key=f"btn_kot_{selected_mmv_id}"):
            update_insurer_mapping_mmv(selected_mmv_id, 'kotak', kot_json)
            st.rerun()

    with tabs[INSURERS.index('acko')]:
        st.markdown("##### Acko Mapping")
        a_data = get_mapping_dict('acko')
        a1, a2 = st.columns(2)
        a_vid = a1.text_input("Variant ID", value=a_data.get('variant_id', ''), key=f"ack_vid_{selected_mmv_id}")
        a_mk = a2.text_input("Make", value=a_data.get('make', ''), key=f"ack_mk_{selected_mmv_id}")
        a_md = a1.text_input("Model", value=a_data.get('model', ''), key=f"ack_md_{selected_mmv_id}")
        a_var = a2.text_input("Variant", value=a_data.get('variant', ''), key=f"ack_var_{selected_mmv_id}")
        a_fuel = a1.text_input("Fuel Type", value=a_data.get('fuel_type', ''), key=f"ack_fl_{selected_mmv_id}")
        
        a_trans = a2.text_input("Transmission", value=a_data.get('transmissionType', ''), key=f"ack_tr_{selected_mmv_id}") if product_id == 2 else ""

        if a_vid:
            ack_pl = {"make": a_mk, "model": a_md, "variant": a_var, "fuel_type": a_fuel, "variant_id": a_vid}
            if a_trans: ack_pl["transmissionType"] = a_trans
            ack_json = json.dumps(ack_pl, indent=2)
        else: ack_json = ""
        
        st.markdown("JSON Payload")
        st.code(ack_json if ack_json else "{}", language="json")
        if st.button("Save Acko", key=f"btn_ack_{selected_mmv_id}"):
            update_insurer_mapping_mmv(selected_mmv_id, 'acko', ack_json)
            st.rerun()

    with tabs[INSURERS.index('magma')]:
        st.markdown("##### Magma Mapping")
        m_data = get_mapping_dict('magma')
        m1, m2 = st.columns(2)
        m_mfc = m1.text_input("Mfr Code", value=m_data.get('MANUFACTURERCODE', ''), key=f"mag_mfc_{selected_mmv_id}")
        m_mdc = m1.text_input("Model Code", value=m_data.get('VEHICLEMODELCODE', ''), key=f"mag_mdc_{selected_mmv_id}")
        m_btc = m1.text_input("Body Type Code", value=m_data.get('BODYTYPECODE', ''), key=f"mag_btc_{selected_mmv_id}")
        m_btd = m2.text_input("Body Desc", value=m_data.get('VEHICLEBODYTYPEDESCRIPTION', ''), key=f"mag_btd_{selected_mmv_id}")
        m_mfr = m2.text_input("Manufacturer", value=m_data.get('MANUFACTURER', ''), key=f"mag_mfr_{selected_mmv_id}")
        m_mod = m2.text_input("Model", value=m_data.get('VEHICLEMODEL', ''), key=f"mag_mod_{selected_mmv_id}")
        m_fl = m1.text_input("Fuel", value=m_data.get('TXT_FUEL', ''), key=f"mag_fl_{selected_mmv_id}")
        m_cc = m2.text_input("CC", value=m_data.get('CUBICCAPACITY', ''), key=f"mag_cc_{selected_mmv_id}")
        m_st = m1.text_input("Seating", value=m_data.get('SEATINGCAPACITY', ''), key=f"mag_st_{selected_mmv_id}")
        m_cr = m2.text_input("Carrying", value=m_data.get('CARRYINGCAPACITY', ''), key=f"mag_cr_{selected_mmv_id}")
        m_seg = m1.text_input("Segment", value=m_data.get('TXT_SEGMENTTYPE', ''), key=f"mag_seg_{selected_mmv_id}")
        m_tac = m2.text_input("TAC Make Code", value=m_data.get('TXT_TACMAKECODE', ''), key=f"mag_tac_{selected_mmv_id}")

        if m_mfc:
            mag_pl = {"TXT_FUEL": m_fl, "BODYTYPECODE": m_btc, "MANUFACTURER": m_mfr, "VEHICLEMODEL": m_mod, "CUBICCAPACITY": m_cc, "SEATINGCAPACITY": m_st, "TXT_SEGMENTTYPE": m_seg, "TXT_TACMAKECODE": m_tac, "CARRYINGCAPACITY": m_cr, "MANUFACTURERCODE": m_mfc, "VEHICLEMODELCODE": m_mdc, "VEHICLEBODYTYPEDESCRIPTION": m_btd}
            mag_json = json.dumps(mag_pl, indent=2)
        else: mag_json = ""
        
        st.markdown("JSON Payload")
        st.code(mag_json if mag_json else "{}", language="json")
        if st.button("Save Magma", key=f"btn_mag_{selected_mmv_id}"):
            update_insurer_mapping_mmv(selected_mmv_id, 'magma', mag_json)
            st.rerun()

    with tabs[INSURERS.index('zuno')]:
        st.markdown("##### Zuno Mapping")
        if product_id == 2:
            z_data = get_mapping_dict('zuno')
            z1, z2 = st.columns(2)
            z_mk = z1.text_input("Make", value=z_data.get('make', ''), key=f"zu_mk_{selected_mmv_id}")
            z_md = z1.text_input("Model", value=z_data.get('model', ''), key=f"zu_md_{selected_mmv_id}")
            z_vr = z2.text_input("Variant", value=z_data.get('variant', ''), key=f"zu_vr_{selected_mmv_id}")
            z_mc = z2.text_input("Master Code", value=z_data.get('masterCode', ''), key=f"zu_mc_{selected_mmv_id}")
            z_ex = z1.text_input("Ex-Showroom", value=z_data.get('exShowroomPrice', ''), key=f"zu_ex_{selected_mmv_id}")
            
            if z_mc:
                zuno_pl = {"make": z_mk, "model": z_md, "variant": z_vr, "masterCode": z_mc, "exShowroomPrice": z_ex}
                zuno_json = json.dumps(zuno_pl, indent=2)
            else: zuno_json = ""
            
            st.markdown("JSON Payload")
            st.code(zuno_json if zuno_json else "{}", language="json")
            if st.button("Save Zuno", key=f"btn_zuno_{selected_mmv_id}"):
                update_insurer_mapping_mmv(selected_mmv_id, 'zuno', zuno_json)
                st.rerun()
        else:
            z_val = get_mapping_str('zuno')
            z_code = st.text_input("Zuno Vehicle Code", value=z_val, key=f"zuno_code_{selected_mmv_id}")
            if st.button("Save Zuno", key=f"btn_zuno_{selected_mmv_id}"):
                update_insurer_mapping_mmv(selected_mmv_id, 'zuno', z_code)
                st.rerun()

    with tabs[INSURERS.index('royalSundaram')]:
        st.markdown("##### Royal Sundaram Mapping")
        r_val = get_mapping_str('royalSundaram')
        r_code = st.text_input("Royal Vehicle Code", value=r_val, key=f"royal_code_{selected_mmv_id}")
        if st.button("Save Royal", key=f"btn_royal_{selected_mmv_id}"):
            update_insurer_mapping_mmv(selected_mmv_id, 'royalSundaram', r_code)
            st.rerun()

    with tabs[INSURERS.index('united')]:
        st.markdown("##### United India Mapping")
        u_data = get_mapping_dict('united')
        u_val = json.dumps(u_data, indent=2) if u_data else ""
        
        st.markdown("JSON Payload")
        u_json = st.text_area("Raw Data", value=u_val, height=250, label_visibility="collapsed", key=f"united_json_{selected_mmv_id}")
        if st.button("Save United", key=f"btn_united_{selected_mmv_id}"):
            update_insurer_mapping_mmv(selected_mmv_id, 'united', u_json)
            st.rerun()

    specific_motor_insurers = ['icici', 'digit', 'reliance', 'hdfc', 'bajaj', 'tata', 'sbi', 'future', 'iffco', 'chola', 'kotak', 'acko', 'magma', 'zuno', 'royalSundaram', 'united']
    
    for i in range(len(INSURERS)):
        ins_name = INSURERS[i]
        if ins_name not in specific_motor_insurers:
             with tabs[i]:
                st.info(f"Generic JSON Editor for {ins_name.upper()}")
                curr_val = current_mmv.get(ins_name)
                val_str = json.dumps(curr_val) if isinstance(curr_val, dict) else str(curr_val)
                
                st.markdown("JSON Payload")
                new_val = st.text_area(f"{ins_name.upper()} JSON", value=val_str, label_visibility="collapsed", key=f"gen_mmv_{ins_name}_{selected_mmv_id}")
                if st.button(f"Save {ins_name.upper()}", key=f"btn_{ins_name}_{selected_mmv_id}"):
                      update_insurer_mapping_mmv(selected_mmv_id, ins_name, new_val)
                      st.rerun()

def display_mmv_registry(product_id, key_suffix="main"):
    st.markdown("### Master Data Registry")
    all_records = get_all_mmv_records(product_id)
    data = []
    for r in all_records:
        record = {
            'Product ID': r.get('product_id', 1), 'Make': r['make'], 'Model': r['model'], 'Variant': r['variant'],
            'CC': r.get('cc', r.get('CC')), 'Fuel': r.get('fueltype', r.get('fuel', '')), 'Seating': r.get('seating_capacity', r.get('seatingcapacity')),
            'Body Type': r.get('body_type', r.get('bodyType')), 'Ensuredit ID': r.get('ensuredit_id', r.get('ensureditId')),
            'Carrying Capacity': r.get('carrying_capacity', r.get('caryingcapacity'))
        }
        for ins in INSURERS:
            val = r.get(ins, '')
            if isinstance(val, dict): val = json.dumps(val) 
            record[ins.upper()] = val
        data.append(record)

    df = pd.DataFrame(data)
    st.dataframe(df, width="stretch", hide_index=True)

    st.markdown("### Raw Data Inspector")
    inspect_opts = {f"{r['make']} {r['model']} - {r['variant']}": r['id'] for r in all_records}
    sel_inspect_label = st.selectbox("Select Record to Inspect", list(inspect_opts.keys()), key=f"inspector_select_{product_id}_{key_suffix}")
    if sel_inspect_label:
        record_id = inspect_opts[sel_inspect_label]
        record = get_mmv_record(record_id)
        if record: st.json(record, expanded=False)

# ------------------------------------------------------------------
#  SECTION C: PINCODE LOGIC & UI
# ------------------------------------------------------------------

@st.cache_data(ttl=600, show_spinner=False)
def get_all_pincode_records():
    if USE_DATABASE:
        # --- FIX: Sort by the new 'id' column ---
        # ORDER BY id ASC ensures records appear in the order they were created.
        # 1, 2, 3... (Oldest to Newest)
        rows = run_query("SELECT * FROM pincode_master ORDER BY id ASC", fetch_all=True)
        return rows if rows else []
    return []

def get_pincode_record(pincode):
    if USE_DATABASE: return run_query("SELECT * FROM pincode_master WHERE pincode = %s", (pincode,), fetch_one=True)
    return None

def update_pincode_record(data):
    if USE_DATABASE:
        sql = "UPDATE pincode_master SET district=%s, city=%s, state=%s WHERE pincode=%s"
        params = (data['district'], data['city'], data['state'], data['pincode'])
        res = run_query(sql, params, commit=True)
        if res: st.cache_data.clear()
        return res
    return False

def add_pincode_record(data):
    if USE_DATABASE:
        exists = run_query("SELECT pincode FROM pincode_master WHERE pincode=%s", (data['pincode'],), fetch_one=True)
        if exists: return False, "Pincode already exists."
        sql = "INSERT INTO pincode_master (pincode, district, city, state) VALUES (%s, %s, %s, %s)"
        params = (data['pincode'], data['district'], data['city'], data['state'])
        success = run_query(sql, params, commit=True)
        if success: st.cache_data.clear()
        return (True, "Success") if success else (False, "Database Insert Failed")
    return False, "No Database"

def update_pincode_insurer_mapping(pincode, insurer, mapping_val):
    if USE_DATABASE:
        # Ensure we map the insurer key to lowercase DB column if needed (though strict key passing is better)
        # Postgres columns are lowercase. If insurer is 'hdfcLife', SQL should be 'hdfclife' or "hdfcLife"
        # Using lower() for column name safety
        sql = f"UPDATE pincode_master SET {insurer.lower()} = %s WHERE pincode = %s"
        run_query(sql, (mapping_val, pincode), commit=True)
        st.cache_data.clear()
        st.success(f"Mapping for {insurer} updated successfully!")

def display_pincode_form(is_edit_mode, record=None):
    import re  # Importing regex module for validation

    form_title = "Update Pincode" if is_edit_mode else "Add New Pincode"
    with st.container():
        st.subheader(form_title)
        c1, c2 = st.columns(2)
        
        def_pin = record.get('pincode', '') if record else ""
        pincode = c1.text_input("Pincode (PK)", value=str(def_pin), disabled=is_edit_mode, key=f"pin_pk_{def_pin}")
        
        def_dist = record.get('district', '') if record else ""
        district = c2.text_input("District", value=def_dist, key=f"pin_dist_{def_pin}")
        
        c3, c4 = st.columns(2)
        def_city = record.get('city', '') if record else ""
        city = c3.text_input("City", value=def_city, key=f"pin_city_{def_pin}")
        
        def_state = record.get('state', '') if record else ""
        state = c4.text_input("State", value=def_state, key=f"pin_state_{def_pin}")
        
        # Reactive Logic Check
        is_major_change = False
        if is_edit_mode and record:
            if str(district) != str(def_dist) or str(city) != str(def_city) or str(state) != str(def_state):
                is_major_change = True

        btn_txt = "Update Pincode" if is_edit_mode else "Save New Pincode"
        
        # --- HELPER: VALIDATION LOGIC ---
        def validate_pincode_form(pin, dist, ct, st_val):
            # 1. Pincode Validation: Exactly 6 digits
            if not (pin and pin.strip().isdigit() and len(pin.strip()) == 6):
                st.error("❌ **Validation Error:** Pincode must be exactly 6 digits.")
                return False
            
            # 2. Text-Only Validation: Regex allows a-z, A-Z, and spaces only
            text_pattern = r"^[a-zA-Z\s]+$"
            
            if not bool(re.match(text_pattern, str(dist).strip())):
                st.error("❌ **Validation Error:** District must contain only Alphabets and Spaces.")
                return False
                
            if not bool(re.match(text_pattern, str(ct).strip())):
                st.error("❌ **Validation Error:** City must contain only Alphabets and Spaces.")
                return False
                
            if not bool(re.match(text_pattern, str(st_val).strip())):
                st.error("❌ **Validation Error:** State must contain only Alphabets and Spaces.")
                return False
                
            return True
        # --------------------------------
        
        if is_edit_mode and is_major_change:
             st.warning("You are modifying core location data.")
             if st.button("Confirm Update", key=f"pin_upd_{pincode}"):
                 if not validate_pincode_form(pincode, district, city, state):
                     return

                 data = {"pincode": pincode, "district": district, "city": city, "state": state}
                 update_pincode_record(data)
                 st.toast("Pincode Updated", icon="✅")
                 st.session_state.clear()
                 st.rerun()
        else:
            if st.button(btn_txt, type="primary", key=f"pin_main_{pincode}"):
                if not validate_pincode_form(pincode, district, city, state):
                    return

                data = {"pincode": pincode, "district": district, "city": city, "state": state}
                if is_edit_mode:
                    update_pincode_record(data)
                    st.toast("Pincode Updated", icon="✅")
                    st.rerun()
                else:
                    success, msg = add_pincode_record(data)
                    if success: 
                        st.toast("Pincode Created", icon="🎉") 
                        st.rerun()
                    else: 
                        st.error(msg)

def display_pincode_registry(key_suffix="pin"):
    st.markdown("### Master Data Registry")
    rows = get_all_pincode_records()
    if not rows: st.info("No Pincode Data Found"); return
    df = pd.DataFrame(rows)
    st.dataframe(df, width="stretch", hide_index=True)
    
    st.markdown("### Raw Inspector")
    # Updated display format as requested: Pincode State
    pin_opts = {f"{r['pincode']} {r.get('state','')}" : r['pincode'] for r in rows}
    sel = st.selectbox("Inspect Record", list(pin_opts.keys()), key=f"pin_insp_{key_suffix}")
    if sel: st.json(get_pincode_record(pin_opts[sel]))

def display_pincode_mapping_workspace():
    st.header("Pincode Insurer Mapping Workspace")
    
    with st.expander("📂 Bulk Upload Pincode Mappings (CSV)", expanded=False):
        b_c1, b_c2 = st.columns([1, 2])
        target_insurer = b_c1.selectbox("Select Insurer", INSURERS, key="pin_bulk_ins")
        overwrite = b_c1.checkbox("Force Overwrite?", key="pin_ovr")
        uploaded_file = b_c2.file_uploader("Upload CSV (Columns: pincode, json_payload or data)", type=["csv"], key="pin_csv")
        if uploaded_file and st.button("Start Bulk Upload"):
            df = pd.read_csv(uploaded_file)
            process_bulk_mapping_upload('pincode_master', 'pincode', target_insurer, df, overwrite)

    st.markdown("---")
    
    all_pins = get_all_pincode_records()
    pin_opts = {f"{r['pincode']} {r.get('state','')}" : r['pincode'] for r in all_pins}
    
    sel_pin_display = st.selectbox("Select Pincode to Map", list(pin_opts.keys()))
    if not sel_pin_display: return
    pincode = pin_opts[sel_pin_display]
    current_pin = get_pincode_record(pincode)
    
    def get_val(key):
        val = current_pin.get(key)
        if val is None:
            val = current_pin.get(key.lower())
        if val is None and key.lower() == 'royalsundaram':
            val = current_pin.get('royal')
            
        if isinstance(val, dict): return val
        if isinstance(val, str) and val.strip():
            try: return json.loads(val)
            except: return {}
        return {}

    tabs = st.tabs([i.upper() for i in INSURERS])
    
    with tabs[INSURERS.index('digit')]:
        d = get_val('digit')
        c1, c2 = st.columns(2)
        code = c1.text_input("Code", value=d.get('code',''), key=f"pin_dg_c_{pincode}")
        st_nm = c1.text_input("State", value=d.get('state',''), key=f"pin_dg_st_{pincode}")
        dist = c2.text_input("District", value=d.get('district',''), key=f"pin_dg_dt_{pincode}")
        j = json.dumps({"code": code, "state": st_nm, "district": dist})
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save Digit", key=f"pin_s_dg_{pincode}"): update_pincode_insurer_mapping(pincode, 'digit', j)

    with tabs[INSURERS.index('chola')]:
        d = get_val('chola')
        c1, c2 = st.columns(2)
        code = c1.text_input("Code", value=d.get('code',''), key=f"pin_ch_c_{pincode}")
        st_nm = c1.text_input("State", value=d.get('state',''), key=f"pin_ch_st_{pincode}")
        dist = c2.text_input("District", value=d.get('district',''), key=f"pin_ch_dt_{pincode}")
        j = json.dumps({"code": code, "state": st_nm, "district": dist})
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save Chola", key=f"pin_s_ch_{pincode}"): update_pincode_insurer_mapping(pincode, 'chola', j)

    with tabs[INSURERS.index('iffco')]:
        d = get_val('iffco')
        c1, c2 = st.columns(2)
        zone = c1.text_input("Zone", value=d.get('zone',''), key=f"pin_if_z_{pincode}")
        cc = c1.text_input("City Code", value=d.get('cityCode',''), key=f"pin_if_cc_{pincode}")
        sc = c2.text_input("State Code", value=d.get('stateCode',''), key=f"pin_if_sc_{pincode}")
        j = json.dumps({"zone": zone, "cityCode": cc, "stateCode": sc})
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save Iffco", key=f"pin_s_if_{pincode}"): update_pincode_insurer_mapping(pincode, 'iffco', j)

    with tabs[INSURERS.index('icici')]:
        d = get_val('icici')
        c1, c2 = st.columns(2)
        sc = c1.text_input("State Code", value=d.get('STATE_CODE',''), key=f"pin_ic_sc_{pincode}")
        cdc = c2.text_input("City Dist Code", value=d.get('CITY_DISTRICT_CODE',''), key=f"pin_ic_cdc_{pincode}")
        j = json.dumps({"STATE_CODE": sc, "CITY_DISTRICT_CODE": cdc})
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save ICICI", key=f"pin_s_ici_{pincode}"): update_pincode_insurer_mapping(pincode, 'icici', j)

    with tabs[INSURERS.index('sbi')]:
        d = get_val('sbi')
        c1, c2 = st.columns(2)
        cit = c1.text_input("City Code", value=d.get('CITY',''), key=f"pin_sb_c_{pincode}")
        sta = c1.text_input("State Code", value=d.get('STATE',''), key=f"pin_sb_s_{pincode}")
        dis = c2.text_input("District Code", value=d.get('DISTRICT',''), key=f"pin_sb_d_{pincode}")
        j = json.dumps({"CITY": cit, "STATE": sta, "DISTRICT": dis})
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save SBI", key=f"pin_s_sbi_{pincode}"): update_pincode_insurer_mapping(pincode, 'sbi', j)

    with tabs[INSURERS.index('reliance')]:
        d = get_val('reliance')
        c1, c2 = st.columns(2)
        cid = c1.text_input("City ID", value=d.get('cityId',''), key=f"pin_rel_cid_{pincode}")
        sid = c1.text_input("State ID", value=d.get('stateId',''), key=f"pin_rel_sid_{pincode}")
        cnm = c1.text_input("City Name", value=d.get('cityName',''), key=f"pin_rel_cnm_{pincode}")
        snm = c2.text_input("State Name", value=d.get('stateName',''), key=f"pin_rel_snm_{pincode}")
        did = c2.text_input("Dist ID", value=d.get('districtId',''), key=f"pin_rel_did_{pincode}")
        dnm = c2.text_input("Dist Name", value=d.get('districtName',''), key=f"pin_rel_dnm_{pincode}")
        j = json.dumps({"cityId": cid, "stateId": sid, "cityName": cnm, "stateName": snm, "districtId": did, "districtName": dnm})
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save Reliance", key=f"pin_s_rel_{pincode}"): update_pincode_insurer_mapping(pincode, 'reliance', j)

    with tabs[INSURERS.index('care')]:
        d = get_val('care')
        c1, c2 = st.columns(2)
        acd = c1.text_input("Area Cd", value=d.get('areaCd',''), key=f"pin_cr_ac_{pincode}")
        ccd = c1.text_input("City Cd", value=d.get('cityCd',''), key=f"pin_cr_cc_{pincode}")
        zcd = c1.text_input("Zone Cd", value=d.get('zoneCd',''), key=f"pin_cr_zc_{pincode}")
        scd = c2.text_input("State Cd", value=d.get('stateCd',''), key=f"pin_cr_sc_{pincode}")
        cocd = c2.text_input("Country Cd", value=d.get('countryCd',''), key=f"pin_cr_coc_{pincode}")
        j = json.dumps({"areaCd": acd, "cityCd": ccd, "zoneCd": zcd, "stateCd": scd, "countryCd": cocd})
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save Care", key=f"pin_s_cr_{pincode}"): update_pincode_insurer_mapping(pincode, 'care', j)

    with tabs[INSURERS.index('cigna')]:
        d = get_val('cigna')
        c1, c2 = st.columns(2)
        guid = c1.text_input("GUID", value=d.get('guId',''), key=f"pin_cg_gid_{pincode}")
        zone = c1.text_input("Zone", value=d.get('zone',''), key=f"pin_cg_z_{pincode}")
        cc = c1.text_input("City Code", value=d.get('cityCode',''), key=f"pin_cg_cc_{pincode}")
        sc = c2.text_input("State Code", value=d.get('stateCode',''), key=f"pin_cg_sc_{pincode}")
        vid = c2.text_input("Version ID", value=d.get('versionId',''), key=f"pin_cg_vi_{pincode}")
        cdesc = c2.text_input("City Desc", value=d.get('cityDescription',''), key=f"pin_cg_cd_{pincode}")
        j = json.dumps({"guId": guid, "zone": zone, "cityCode": cc, "stateCode": sc, "versionId": vid, "cityDescription": cdesc})
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save Cigna", key=f"pin_s_cg_{pincode}"): update_pincode_insurer_mapping(pincode, 'cigna', j)

    with tabs[INSURERS.index('hdfc')]:
        d = get_val('hdfc')
        v1 = d.get('v1', {})
        c1, c2 = st.columns(2)
        pc = c1.text_input("Pin Code", value=v1.get('pinCode',''), key=f"pin_hd_pc_{pincode}")
        cc = c1.text_input("City Code", value=v1.get('cityCode',''), key=f"pin_hd_cc_{pincode}")
        cn = c1.text_input("City Name", value=v1.get('cityName',''), key=f"pin_hd_cn_{pincode}")
        sc = c2.text_input("State Code", value=v1.get('stateCode',''), key=f"pin_hd_sc_{pincode}")
        sn = c2.text_input("State Name", value=v1.get('stateName',''), key=f"pin_hd_sn_{pincode}")
        j = json.dumps({"v1": {"pinCode": pc, "cityCode": cc, "cityName": cn, "stateCode": sc, "stateName": sn}})
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save HDFC", key=f"pin_s_hd_{pincode}"): update_pincode_insurer_mapping(pincode, 'hdfc', j)

    with tabs[INSURERS.index('magma')]:
        d = get_val('magma')
        c1, c2 = st.columns(2)
        ts = c1.text_input("Txt State", value=d.get('TXT_STATE',''), key=f"pin_mg_ts_{pincode}")
        ns = c1.text_input("Num State Cd", value=d.get('NUM_STATE_CD',''), key=f"pin_mg_ns_{pincode}")
        tcd = c1.text_input("Txt CityDist", value=d.get('TXT_CITYDISTRICT',''), key=f"pin_mg_tcd_{pincode}")
        ncd = c2.text_input("Num CityDist Cd", value=d.get('NUM_CITYDISTRICT_CD',''), key=f"pin_mg_ncd_{pincode}")
        tpl = c2.text_input("Txt Pincode Loc", value=d.get('TXT_PINCODE_LOCALITY',''), key=f"pin_mg_tpl_{pincode}")
        j = json.dumps({"TXT_STATE": ts, "NUM_STATE_CD": ns, "TXT_CITYDISTRICT": tcd, "NUM_CITYDISTRICT_CD": ncd, "TXT_PINCODE_LOCALITY": tpl})
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save Magma", key=f"pin_s_mg_{pincode}"): update_pincode_insurer_mapping(pincode, 'magma', j)

    with tabs[INSURERS.index('careCashless')]:
        d = get_val('careCashless')
        c1, c2 = st.columns(2)
        cid = c1.text_input("City ID", value=d.get('cityId',''), key=f"pin_crc_ci_{pincode}")
        sid = c1.text_input("State ID", value=d.get('stateId',''), key=f"pin_crc_si_{pincode}")
        cn = c2.text_input("City Name", value=d.get('cityName',''), key=f"pin_crc_cn_{pincode}")
        sn = c2.text_input("State Name", value=d.get('stateName',''), key=f"pin_crc_sn_{pincode}")
        j = json.dumps({"cityId": cid, "stateId": sid, "cityName": cn, "stateName": sn})
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save CareCashless", key=f"pin_s_crc_{pincode}"): update_pincode_insurer_mapping(pincode, 'careCashless', j)

    with tabs[INSURERS.index('nivaBupa')]:
        d = get_val('nivaBupa')
        c1, c2 = st.columns(2)
        cc = c1.text_input("City Code", value=d.get('cityCode',''), key=f"pin_nb_cc_{pincode}")
        cn = c1.text_input("City Name", value=d.get('cityName',''), key=f"pin_nb_cn_{pincode}")
        cz = c1.text_input("City Zone", value=d.get('cityZone',''), key=f"pin_nb_cz_{pincode}")
        sc = c2.text_input("State Code", value=d.get('stateCode',''), key=f"pin_nb_sc_{pincode}")
        sn = c2.text_input("State Name", value=d.get('stateName',''), key=f"pin_nb_sn_{pincode}")
        j = json.dumps({"cityCode": cc, "cityName": cn, "cityZone": cz, "stateCode": sc, "stateName": sn})
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save NivaBupa", key=f"pin_s_nb_{pincode}"): update_pincode_insurer_mapping(pincode, 'nivaBupa', j)

    with tabs[INSURERS.index('cholaPA')]:
        d = get_val('cholaPA')
        st.caption("Use JSON editor below for complex Area Details array")
        
        st.markdown("JSON Payload")
        j_raw = st.text_area("Raw Data", value=json.dumps(d, indent=2), label_visibility="collapsed", key=f"pin_cpa_raw_{pincode}")
        if st.button("Save CholaPA", key=f"pin_s_cpa_{pincode}"): update_pincode_insurer_mapping(pincode, 'cholaPA', j_raw)


    with tabs[INSURERS.index('iciciHealth')]:
        d = get_val('iciciHealth')
        c1, c2 = st.columns(2)
        ts = c1.text_input("Txt State", value=d.get('TXT_STATE',''), key=f"pin_ich_ts_{pincode}")
        ns = c1.text_input("Num State Cd", value=d.get('NUM_STATE_CD',''), key=f"pin_ich_ns_{pincode}")
        tcd = c1.text_input("Txt CityDist", value=d.get('TXT_CITYDISTRICT',''), key=f"pin_ich_tcd_{pincode}")
        ncd = c2.text_input("Num CityDist Cd", value=d.get('NUM_CITYDISTRICT_CD',''), key=f"pin_ich_ncd_{pincode}")
        tpl = c2.text_input("Txt Pincode Loc", value=d.get('TXT_PINCODE_LOCALITY',''), key=f"pin_ich_tpl_{pincode}")
        j = json.dumps({"TXT_STATE": ts, "NUM_STATE_CD": ns, "TXT_CITYDISTRICT": tcd, "NUM_CITYDISTRICT_CD": ncd, "TXT_PINCODE_LOCALITY": tpl})
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save ICICI Health", key=f"pin_s_ich_{pincode}"): update_pincode_insurer_mapping(pincode, 'iciciHealth', j)

    with tabs[INSURERS.index('royalSundaram')]:
        d = get_val('royalSundaram')
        c1, c2 = st.columns(2)
        cn = c1.text_input("City Name", value=d.get('cityName',''), key=f"pin_rs_cn_{pincode}")
        j = json.dumps({"cityName": cn})
        
        st.markdown("JSON Payload")
        st.code(j, language="json")
        if st.button("Save Royal", key=f"pin_s_rs_{pincode}"): update_pincode_insurer_mapping(pincode, 'royalSundaram', j)

    other_ins = ['hdfcLife', 'tataAIA', 'tata', 'hdfcHealth', 'oic', 'tataMhg', 'united', 'shriram']
    for ins in other_ins:
        if ins in INSURERS:
            with tabs[INSURERS.index(ins)]:
                st.info(f"Generic JSON Editor for {ins.upper()}")
                curr_val = get_val(ins)
                val_str = json.dumps(curr_val) if isinstance(curr_val, dict) else str(curr_val)
                
                st.markdown("JSON Payload")
                new_val = st.text_area("Raw Data", value=val_str, label_visibility="collapsed", key=f"gen_pin_{ins}_{pincode}")
                if st.button(f"Save {ins.upper()}", key=f"s_pin_{ins}_{pincode}"): update_pincode_insurer_mapping(pincode, ins, new_val)
# ------------------------------------------------------------------
#  MAIN APPLICATION LAYOUT (ROUTER)
# ------------------------------------------------------------------

with st.sidebar:
    st.header("Master Selector")
    # Add a unique key to the radio button to help state tracking
    master_selection = st.radio(
        "Choose Master:",
        options=["2W Master", "4W Master", "RTO Master", "Pincode Master"],
        index=0,
        key="master_radio_selection"
    )

# --- GHOSTING FIX: CLEAR UI ON SWITCH ---
if "last_master" not in st.session_state:
    st.session_state["last_master"] = master_selection

if st.session_state["last_master"] != master_selection:
    st.session_state["last_master"] = master_selection
    # This forces a full rerun, which clears all ephemeral UI elements
    st.rerun()

# Create a single placeholder for the main content area
page_container = st.empty()

with page_container.container():
    if master_selection == "RTO Master":
        st.title("🏢 RTO Master Data Management")
        tab_rto_2, tab_rto_1, tab_rto_3 = st.tabs(["📝 Insurer Mapping", "🛠️ RTO Workspace", "📊 Database View"])
        
        with tab_rto_1:
            op_mode = st.radio("Action:", ["Add New RTO", "Update Existing RTO"], horizontal=True)
            sel_rec = None
            if op_mode == "Update Existing RTO":
                with st.spinner("Loading RTO Records..."):
                    all_rto = get_all_rto_records()
                rto_map = {}
                for r in all_rto:
                    d_str = r.get('search_string', r.get('searchString', 'Unknown'))
                    rto_map[f"{r['id']} - {d_str}"] = r['id']
                sel_k = st.selectbox("Search RTO", list(rto_map.keys()))
                if sel_k: sel_rec = get_rto_record(rto_map[sel_k])
            display_rto_form(op_mode == "Update Existing RTO", sel_rec)
            
            st.markdown("---")
            display_rto_registry(key_suffix="rto_wksp")
            
        with tab_rto_2:
            display_rto_mapping_workspace()
            st.markdown("---")
            display_rto_registry(key_suffix="rto_map")
            
        with tab_rto_3:
            display_rto_registry(key_suffix="rto_db")

    elif master_selection in ["2W Master", "4W Master"]:
        current_product_id = 1 if master_selection == "2W Master" else 2
        icon = "🏍️" if current_product_id == 1 else "🚙"
        st.title(f"{icon} {master_selection} Data Management")
        
        tab_mmv_2, tab_mmv_1, tab_mmv_3 = st.tabs(["📝 Insurer Mapping", "🛠️ MMV Workspace", "📊 Database View"])
        
        with tab_mmv_1:
            op_mode = st.radio("Action:", ["Add New MMV", "Update Existing MMV"], horizontal=True, key="mmv_op")
            sel_rec = None
            if op_mode == "Update Existing MMV":
                with st.spinner("Loading Vehicle Records..."):
                    all_recs = get_all_mmv_records(current_product_id)
                mmv_map = {f"{r['make']} {r['model']} {r['variant']}": r['id'] for r in all_recs}
                sel_k = st.selectbox("Search Vehicle", list(mmv_map.keys()))
                if sel_k: sel_rec = get_mmv_record(mmv_map[sel_k])
            display_mmv_form(current_product_id, op_mode == "Update Existing MMV", sel_rec)
            
            st.markdown("---")
            display_mmv_registry(current_product_id, key_suffix="mmv_wksp")

        with tab_mmv_2:
            display_insurer_mapping_form_mmv(current_product_id)
            st.markdown("---")
            display_mmv_registry(current_product_id, key_suffix="mmv_map")
            
        with tab_mmv_3:
            display_mmv_registry(current_product_id, key_suffix="mmv_db")

    elif master_selection == "Pincode Master":
        st.title("📍 Pincode Master Data Management")
        
        tab_pin_2, tab_pin_1, tab_pin_3 = st.tabs(["📝 Insurer Mapping", "🛠️ Pincode Workspace", "📊 Database View"])
        
        with tab_pin_1:
            op_mode = st.radio("Action:", ["Add New Pincode", "Update Existing Pincode"], horizontal=True)
            sel_rec = None
            if op_mode == "Update Existing Pincode":
                with st.spinner("Loading Pincode Records..."):
                    all_pins = get_all_pincode_records()
                # Using Dictionary for faster lookup
                # Concat of Pincode State
                pin_map = {f"{p['pincode']} {p.get('state','')}" : p['pincode'] for p in all_pins}
                sel_k = st.selectbox("Search Pincode", list(pin_map.keys()))
                if sel_k: sel_rec = get_pincode_record(pin_map[sel_k])
            display_pincode_form(op_mode == "Update Existing Pincode", sel_rec)
            
            st.markdown("---")
            display_pincode_registry(key_suffix="pin_wksp")

        with tab_pin_2:
            display_pincode_mapping_workspace()
            st.markdown("---")
            display_pincode_registry(key_suffix="pin_map")

        with tab_pin_3:
            display_pincode_registry(key_suffix="pin_db")