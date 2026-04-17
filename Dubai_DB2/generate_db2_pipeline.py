import pandas as pd
import json
import ast
import re
import numpy as np
import os
from collections import defaultdict

def to_dict_safe(x):
    if isinstance(x, dict): return x
    if x is None or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and x.strip() == ""): return {}
    if isinstance(x, str):
        s = x.strip()
        if s.lower() in {"nan", "none", "null", "{}"}: return {}
        try:
            obj = json.loads(s)
            if isinstance(obj, dict): return obj
        except: pass
        try:
            obj = ast.literal_eval(s)
            if isinstance(obj, dict): return obj
        except: pass
    return {}

def categorize_units(x):
    data = to_dict_safe(x)
    result = {}
    comm_kw = ['GYM', 'HOTEL', 'KIOSK', 'OFFICE', 'SHOP', 'STUDIO']
    for k, v in data.items():
        ks = str(k).strip().upper()
        try: val = float(v)
        except: val = 0
        if 'SINGLE ROOM' in ks or ks == 'SINGLE':
            result['< 1 B/R'] = result.get('< 1 B/R', 0) + val; continue
        if 'PENTHOUSE' in ks:
            result['PENTHOUSE'] = result.get('PENTHOUSE', 0) + val; continue
        if any(w in ks for w in comm_kw):
            result['Commercial'] = result.get('Commercial', 0) + val; continue
        m = re.search(r'(\d+)\s*(?:B\s*/?\s*R?|BR|BHK)\b', ks, re.IGNORECASE)
        num = int(m.group(1)) if m else None
        if num is not None:
            rk = {1:'1 B/R', 2:'2 B/R', 3:'3 B/R', 4:'4 B/R', 5:'5 B/R'}.get(num, '> 5 B/R' if num > 5 else 'Other')
            result[rk] = result.get(rk, 0) + val
        else:
            result['Other'] = result.get('Other', 0) + val
    return {k: int(v) if v == int(v) else v for k, v in result.items()}

def calculate_cumulative_area(x):
    data = to_dict_safe(x)
    total = 0.0
    for k, v in data.items():
        try: total += float(k) * float(v)
        except: continue
    return round(total, 2)

def normalize_project_id(x):
    if pd.isna(x): return None
    s = str(x).strip()
    try:
        f = float(s)
        if f.is_integer(): return str(int(f))
    except: pass
    s = re.sub(r"\.0$", "", s).strip()
    return s if s else None

def map_room_key_to_bucket(room_key):
    s = str(room_key).strip().upper()
    if "SINGLE ROOM" in s or s == "SINGLE": return "lt_1_br"
    if "PENTHOUSE" in s: return "PENTHOUSE"
    commercial_keywords = ['GYM', 'HOTEL', 'KIOSK', 'OFFICE', 'SHOP', 'STUDIO']
    if any(kw in s for kw in commercial_keywords): return "Commercial"
    m = re.search(r'(\d+)\s*B\s*/?\s*R\b|(\d+)\s*BR\b', s)
    if m:
        num = int(m.group(1) or m.group(2))
        if num == 1: return "1_br"
        if num == 2: return "2_br"
        if num == 3: return "3_br"
        if num == 4: return "4_br"
        if num == 5: return "5_br"
        if num > 5:  return "gt_5_br"
    return "Other"

def normalize_rooms_area_dict(x):
    data = to_dict_safe(x)
    out = defaultdict(lambda: defaultdict(int))
    for outer_key, inner in data.items():
        bucket = map_room_key_to_bucket(outer_key)
        for area_k, cnt_v in to_dict_safe(inner).items():
            try:
                area_clean = f"{float(area_k):.2f}"
                count_clean = int(float(cnt_v))
                out[bucket][area_clean] += count_clean
            except: continue
    return {k: dict(v) for k, v in out.items()}

def sum_area_times_count(inner_dict):
    d = to_dict_safe(inner_dict)
    total = 0.0
    for k, v in d.items():
        try: total += float(k) * float(v)
        except: continue
    return round(total, 2)

def extract_carpet_areas_per_bucket(x):
    outer = to_dict_safe(x)
    out = {}
    for bucket, inner in outer.items():
        safe_bucket = str(bucket).strip().replace(' ', '_').replace('/', '_').replace('<', 'lt').replace('>', 'gt')
        col_name = f"carpet_area_{safe_bucket}"
        out[col_name] = sum_area_times_count(inner)
    return out

# --- Pipeline Configuration ---
projects_file = r"D:\Dubai\Dubai_Updated_data\Raw_Data\Projects.csv"
units_file = r"D:\Dubai\Dubai_Updated_data\Project_Unit_Summary\Units_with_actual_area_sqft.csv"
buildings_file = r"D:\Dubai\Dubai_Updated_data\Raw_Data\Buildings.csv"
buildings_raw_file = r"D:\Dubai\Dubai_Updated_data\Raw_Data\Buildings.csv"
latlong_file = r"C:\Users\Admin\Downloads\Dubai_LatLong_Final_WithSearch.xlsx"
df_dev_file = r"D:\Dubai\Dubai_Updated_data\Raw_Data\Developers.csv"
output_file = r"G:\.shortcut-targets-by-id\1oGd6xPdp686p0qW-tzZyy5quOpi82hLA\DB1+DB2\Dubai\Dubai_DB2.xlsx"

def main():
    print("1. Loading Projects...")
    df = pd.read_csv(projects_file, encoding='utf-8', encoding_errors='replace')
    df = df.drop_duplicates(subset=['project_id'], keep='first')
    df['_pid_norm'] = df['project_id'].apply(normalize_project_id)

    # Note: User provided df_dev_file in prompt. Load it if needed.
    if os.path.exists(df_dev_file):
        df_dev = pd.read_csv(df_dev_file)
        # Assuming you may want to use developers data, although the notebook does not merge it explicitly.
    
    print("2. Aggregating Units (chunked)...")
    units_columns = ['project_id', 'rooms_en', 'actual_area_sqft', 'property_sub_type_en', 'project_name_en', 'project_name_ar']
    project_data = defaultdict(lambda: {'rooms': defaultdict(int), 'areas': defaultdict(int), 'subtypes': defaultdict(int), 'name_en': None, 'name_ar': None, 'unit_count': 0, 'room_area_pairs': []})
    
    chunk_size = 500_000
    for chunk in pd.read_csv(units_file, chunksize=chunk_size, encoding='utf-8', encoding_errors='replace', low_memory=False):
        avail = [c for c in units_columns if c in chunk.columns]
        chunk = chunk[avail]
        chunk['_pid_norm'] = chunk['project_id'].apply(normalize_project_id)
        for _, row in chunk.iterrows():
            pid = row['_pid_norm']
            if pd.isna(pid): continue
            project_data[pid]['unit_count'] += 1
            
            room = row.get('rooms_en')
            rk = 'NA' if pd.isna(room) or str(room).strip() == '' else str(room).strip()
            project_data[pid]['rooms'][rk] += 1
            
            area = row.get('actual_area_sqft')
            ak = 'NA' if pd.isna(area) or str(area).strip() == '' else f"{float(area):.2f}" if area else 'NA'
            project_data[pid]['areas'][ak] += 1
            
            subtype = row.get('property_sub_type_en')
            sk = 'NA' if pd.isna(subtype) or str(subtype).strip() == '' else str(subtype).strip()
            project_data[pid]['subtypes'][sk] += 1
            
            for col, tgt in [('project_name_en','name_en'), ('project_name_ar','name_ar')]:
                v = row.get(col)
                if pd.notna(v) and project_data[pid][tgt] is None: project_data[pid][tgt] = str(v).strip()
                    
            project_data[pid]['room_area_pairs'].append((row.get('rooms_en'), row.get('actual_area_sqft')))

    print("3. Building aggregated table...")
    agg_rows = []
    for pid, d in project_data.items():
        nested = defaultdict(lambda: defaultdict(int))
        for r, a in d['room_area_pairs']:
            rk = "NA" if pd.isna(r) or str(r).strip() == "" else str(r).strip()
            ak = "NULL" if pd.isna(a) or str(a).strip() == "" else f"{float(a):.2f}" if a else "NULL"
            nested[rk][ak] += 1
        agg_rows.append({
            '_pid_norm': pid,
            'units_rooms_en': dict(d['rooms']),
            'units_actual_area': dict(d['areas']),
            'units_property_sub_type_en': dict(d['subtypes']),
            'units_project_name_en': d['name_en'],
            'units_project_name_ar': d['name_ar'],
            'unit_count': d['unit_count'],
            'rooms_area_dict_raw': {k: dict(v) for k, v in nested.items()}
        })

    df_agg = pd.DataFrame(agg_rows)
    print("4. Merging...")
    df = df.merge(df_agg, on='_pid_norm', how='left')
    df['unit_count'] = df['unit_count'].fillna(0).astype(int)
    for c in ['units_rooms_en', 'units_actual_area', 'units_property_sub_type_en', 'rooms_area_dict_raw']:
        df[c] = df[c].apply(lambda x: x if isinstance(x, dict) else {})
    df['units_project_name_en'] = df['units_project_name_en'].fillna('')
    df['units_project_name_ar'] = df['units_project_name_ar'].fillna('')

    print("5. Date / quarter columns...")
    for col in ['project_start_date', 'project_end_date']:
        df[col] = pd.to_datetime(df[col], format='%d-%m-%Y', errors='coerce')
    df['Start_Year'] = df['project_start_date'].dt.year.astype('Int64')
    df['End_Year']   = df['project_end_date'].dt.year.astype('Int64')
    df['Start_Quarter'] = ('Q' + df['project_start_date'].dt.quarter.astype('Int64').astype(str) + ' ' + df['project_start_date'].dt.year.astype('Int64').astype(str)).where(df['project_start_date'].notna(), '')
    df['End_Quarter'] = ('Q' + df['project_end_date'].dt.quarter.astype('Int64').astype(str) + ' ' + df['project_end_date'].dt.year.astype('Int64').astype(str)).where(df['project_end_date'].notna(), '')
    df['Start_Quarter_Year'] = df['project_start_date'].dt.to_period('Q')
    df['End_Quarter_Year']   = df['project_end_date'].dt.to_period('Q')

    print("6. Categorizing & expanding bedroom types...")
    df['categorized_units'] = df['units_rooms_en'].apply(categorize_units)
    cats = ['< 1 B/R', '1 B/R', '2 B/R', '3 B/R', '4 B/R', '5 B/R', '> 5 B/R', 'PENTHOUSE', 'Commercial', 'Other']
    expanded = df['categorized_units'].apply(pd.Series).reindex(columns=cats).fillna(0).astype(int)
    df = pd.concat([df, expanded], axis=1)

    print("7. Cumulative actual area...")
    df['units_actual_area_Cumulative'] = df['units_actual_area'].apply(calculate_cumulative_area)

    print("8. Creating JSON dicts...")
    df['rooms_area_dict'] = df['rooms_area_dict_raw'].apply(lambda d: json.dumps(d, ensure_ascii=False) if d else '{}')
    df['rooms_area_dict_categorized'] = df['rooms_area_dict_raw'].apply(normalize_rooms_area_dict)
    df['rooms_area_dict_categorized'] = df['rooms_area_dict_categorized'].apply(lambda d: json.dumps(d, ensure_ascii=False) if d else '{}')

    print("9. Carpet area per bucket...")
    carpet_expanded = df['rooms_area_dict_categorized'].apply(extract_carpet_areas_per_bucket).apply(pd.Series).fillna(0)
    df = pd.concat([df, carpet_expanded], axis=1)
    df.drop(columns=['_pid_norm', 'rooms_area_dict_raw'], inplace=True, errors='ignore')

    # --- Property Sub Types ---
    print("10. Property Sub Types...")
    flat_set   = {"FLAT", "UNIT", "BUILDING"}
    shop_set   = {"SHOP", "SHOW ROOMS", "STORE"}
    office_set = {"CLINIC", "OFFICE", "WAREHOUSE", "WORKSHOP"}

    def summarize_4cols(x):
        d = to_dict_safe(x)
        out = {"Flat": 0, "Shop": 0, "Office": 0, "Other": 0}
        for k, v in d.items():
            key = str(k).strip().upper()
            try: cnt = int(float(v))
            except: cnt = 0
            if key in flat_set: out["Flat"] += cnt
            elif key in shop_set: out["Shop"] += cnt
            elif key in office_set: out["Office"] += cnt
            else: out["Other"] += cnt
        return out
    
    four_cols = df["units_property_sub_type_en"].apply(summarize_4cols).apply(pd.Series)
    df = pd.concat([df, four_cols], axis=1)

    # Add property_type_flag
    def property_flag(row):
        # Access values directly using row['column_name'] to get scalar values
        flat = row.get("Flat", 0)
        shop = row.get("Shop", 0)
        office = row.get("Office", 0)
        other = row.get("Other", 0)
        
        # Convert to scalar if they're Series (they shouldn't be, but let's be safe)
        if hasattr(flat, 'iloc'):
            flat = flat.iloc[0] if len(flat) > 0 else 0
        if hasattr(shop, 'iloc'):
            shop = shop.iloc[0] if len(shop) > 0 else 0
        if hasattr(office, 'iloc'):
            office = office.iloc[0] if len(office) > 0 else 0
        if hasattr(other, 'iloc'):
            other = other.iloc[0] if len(other) > 0 else 0
        
        # Convert to numbers
        try:
            flat = int(flat) if flat is not None else 0
            shop = int(shop) if shop is not None else 0
            office = int(office) if office is not None else 0
            other = int(other) if other is not None else 0
        except (ValueError, TypeError):
            flat, shop, office, other = 0, 0, 0, 0
        
        if flat > 0 and shop == 0 and office == 0 and other == 0:
            return "Residential"
        if flat > 0 and (shop > 0 or office > 0 or other > 0):
            return "Residential + Commercial"
        if flat == 0 and (shop > 0 or office > 0 or other > 0):
            return "Commercial"
        return "NA"
    df["property_type_flag"] = df.apply(property_flag, axis=1)

    # Subtype actual area dict lossless
    # Since we don't do the full groupby on raw units, we'll emulate it from the raw area data
    df_units = pd.read_csv(units_file, low_memory=False)
    df_units['_pid'] = df_units['project_id'].apply(normalize_project_id)
    df['_pid'] = df['project_id'].apply(normalize_project_id)
    
    def subtype_area_dict_lossless(group):
        area_sum = defaultdict(float)
        null_count = defaultdict(int)
        for subtype, area in zip(group["property_sub_type_en"], group["actual_area_sqft"]):
            sub_key = "NA" if pd.isna(subtype) or str(subtype).strip() == "" else str(subtype).strip()
            if pd.isna(area) or str(area).strip() == "":
                null_count[sub_key] += 1
                continue
            try:
                area_val = float(area)
                area_sum[sub_key] += area_val
            except: null_count[sub_key] += 1
        out = {k: round(v, 2) for k, v in area_sum.items()}
        if null_count: out["__NULL_COUNT__"] = dict(null_count)
        return out
    
    df_units_small = df_units.loc[df_units["_pid"].notna(), ["_pid", "property_sub_type_en", "actual_area_sqft"]]
    df_subtype_area = df_units_small.groupby("_pid", dropna=False).apply(subtype_area_dict_lossless).reset_index(name="subtype_actual_area_dict")
    df_subtype_area["subtype_actual_area_dict"] = df_subtype_area["subtype_actual_area_dict"].apply(lambda d: json.dumps(d, ensure_ascii=False))
    df = df.merge(df_subtype_area, on="_pid", how="left")
    df["subtype_actual_area_dict"] = df["subtype_actual_area_dict"].fillna("{}")

    def summarize_4cols_carpet_area(x):
        d = to_dict_safe(x)
        out = {"carpet_area_Flat": 0.0, "carpet_area_Shop": 0.0, "carpet_area_Office": 0.0, "carpet_area_Other": 0.0}
        for k, v in d.items():
            key_u = str(k).strip().upper()
            if key_u in {"__NULL_COUNT__", "NULL", "NA"}: continue
            try: area_val = float(v)
            except: area_val = 0.0
            if key_u in flat_set: out["carpet_area_Flat"] += area_val
            elif key_u in shop_set: out["carpet_area_Shop"] += area_val
            elif key_u in office_set: out["carpet_area_Office"] += area_val
            else: out["carpet_area_Other"] += area_val
        for c in out: out[c] = round(out[c], 2)
        return out

    carpet_cols2 = df["subtype_actual_area_dict"].apply(summarize_4cols_carpet_area).apply(pd.Series)
    df = pd.concat([df, carpet_cols2], axis=1)

    # --- Buildings Enrichment ---
    print("11. Buildings logic...")
    if os.path.exists(buildings_file):
        df_buidling = pd.read_csv(buildings_file)
        df["project_id_merge"] = pd.to_numeric(df["project_id"], errors="coerce").astype("Int64")
        df_buidling["project_id_merge"] = pd.to_numeric(df_buidling["project_id"], errors="coerce").astype("Int64")
        df_buidling["floors"] = pd.to_numeric(df_buidling["floors"], errors="coerce")
        df_buidling["floor_int"] = df_buidling["floors"].fillna(0).round().astype(int)
        floors_by_project = df_buidling.groupby("project_id_merge")["floor_int"].apply(list).reset_index(name="floor_list")
        df = df.merge(floors_by_project, on="project_id_merge", how="left")
        df["floor_list"] = df["floor_list"].apply(lambda x: x if isinstance(x, list) else [])
        df.drop(columns=["project_id_merge"], inplace=True)

    # # --- Lat/Long Merge ---
    # print("12. Lat/Long Merge...")
    # if os.path.exists(latlong_file):
    #     df_latlong = pd.read_excel(latlong_file)
    #     latlong_cols = ["project_id", "Latitude", "Longitude", "Geocode_Status", "Status", "Search_Map_Text"]
    #     df_latlong = df_latlong[[c for c in latlong_cols if c in df_latlong.columns]]
    #     df = df.merge(df_latlong, on="project_id", how="left")

    # --- Building Data JSON ---
    print("13. Building Data JSON...")
    if os.path.exists(buildings_raw_file):
        buildings_df = pd.read_csv(buildings_raw_file)
        clean_project_dict = {}
        building_floors_dict = {}
        
        buildings_df['shops'] = buildings_df.get('shops', pd.Series([0]*len(buildings_df))).fillna(0).astype(int)
        buildings_df['flats'] = buildings_df.get('flats', pd.Series([0]*len(buildings_df))).fillna(0).astype(int)
        buildings_df['offices'] = buildings_df.get('offices', pd.Series([0]*len(buildings_df))).fillna(0).astype(int)
        buildings_df['building_number'] = buildings_df['building_number'].fillna('unknown').astype(str)
        buildings_df['floors'] = pd.to_numeric(buildings_df.get('floors', pd.Series([0]*len(buildings_df))), errors='coerce').fillna(0).astype(int)
        
        for _, r in buildings_df.iterrows():
            proj = r['project_id'] if not pd.isna(r['project_id']) else "unknown_project"
            bnum = str(r['building_number']).strip()
            
            if proj not in clean_project_dict:
                clean_project_dict[proj] = {}
                building_floors_dict[proj] = {}
                
            clean_project_dict[proj][bnum] = {'shops': r['shops'], 'flats': r['flats'], 'offices': r['offices']}
            building_floors_dict[proj][bnum] = r['floors']
            
        df['building_data'] = df['project_id'].map(clean_project_dict).apply(lambda x: json.dumps(x) if isinstance(x, dict) else '{}')
        df['_building_floors_dict'] = df['project_id'].map(building_floors_dict).apply(lambda x: x if isinstance(x, dict) else {})

    # --- Formatting Additional Columns ---
    print("14. Additional derived columns...")
    
    def transform_rooms_area_dict_keep_keys(rooms_area_dict_str):
        data = to_dict_safe(rooms_area_dict_str)
        if not isinstance(data, dict): return {}
        transformed = {}
        for room_type, areas in data.items():
            if not isinstance(areas, dict): continue
            transformed[room_type] = {}
            for area, count in areas.items():
                area_str = str(area)
                try: c = int(count)
                except: c = int(float(count)) if pd.notna(count) else 0
                transformed[room_type][area_str] = [c, 0]
        return transformed

    def build_carpet_wise_total_sold_units(row):
        project_name = row.get("units_project_name_en")
        if pd.isna(project_name) or str(project_name).strip() == "": return ""
        rooms_raw = row.get("rooms_area_dict")
        t_rooms = transform_rooms_area_dict_keep_keys(rooms_raw)
        return json.dumps({str(project_name): [[t_rooms]]}, ensure_ascii=False)
    
    df["carpet_wise_total_sold_units"] = df.apply(build_carpet_wise_total_sold_units, axis=1)

    def build_bhk_wise_total_sold(units_rooms_val):
        data = to_dict_safe(units_rooms_val)
        out = {}
        for k, v in data.items():
            try: total = int(float(v))
            except: total = 0
            out[str(k)] = {"total": total, "sold": 0, "unsold": total}
        return json.dumps(out, ensure_ascii=False)
    
    df["bhk_wise_total_sold"] = df["units_rooms_en"].apply(build_bhk_wise_total_sold)

    def to_ddmmyyyy(x):
        if pd.isna(x): return None
        dt = pd.to_datetime(x, errors="coerce")
        if pd.isna(dt): return None
        return dt.strftime("%d/%m/%Y")
        
    df["_start_fmt"] = df["project_start_date"].apply(to_ddmmyyyy)
    df["commencement_date"] = df.apply(lambda r: json.dumps({str(r["units_project_name_en"]): [r["_start_fmt"]]}, ensure_ascii=False) if pd.notna(r["_start_fmt"]) and pd.notna(r["units_project_name_en"]) else None, axis=1)
    
    df["_end_fmt"] = df["project_end_date"].apply(to_ddmmyyyy)
    df["final_proposed_date_of_compeletion"] = df.apply(lambda r: json.dumps({str(r["units_project_name_en"]): [r["_end_fmt"]]}, ensure_ascii=False) if pd.notna(r["_end_fmt"]) and pd.notna(r["units_project_name_en"]) else None, axis=1)

    def safe_float(x):
        try: return float(str(x).strip())
        except: return np.nan
        
    def project_bhk_info(row):
        project = str(row.get("units_project_name_en")).strip()
        if not project or project.lower() == "nan": return None
        rooms_area = to_dict_safe(row.get("rooms_area_dict"))
        if not rooms_area: return None
        snapshots = []
        for room_type, area_cnts in rooms_area.items():
            rt = " ".join(str(room_type).strip().split())
            if not rt or rt.lower() == "nan": continue
            total, wsum = 0, 0.0
            if isinstance(area_cnts, dict):
                for k, v in area_cnts.items():
                    area = safe_float(k)
                    try: cnt = int(v)
                    except: cnt = 0
                    if not np.isnan(area) and cnt > 0:
                        total += cnt
                        wsum += area * cnt
            avg_area = (wsum / total) if total > 0 else None
            snapshots.append({rt: {"total": int(total), "sold": 0, "avg_area": (float(avg_area) if avg_area is not None else None)}})
        if not snapshots: return None
        return str({project: snapshots})
    
    df["project_wise_bhk_info"] = df.apply(project_bhk_info, axis=1)

    def fmt_area(a):
        try: x = float(str(a).strip())
        except: return None
        return f"{x:.2f}".rstrip("0").rstrip(".")

    def dict_repr_with_doubled_single_quotes(py_obj):
        return str(py_obj).replace("'", "''")

    def build_bhk_wise_ca(row):
        project = str(row.get("units_project_name_en")).strip()
        if not project or project.lower() == "nan": return None
        rooms_area = to_dict_safe(row.get("rooms_area_dict"))
        if not rooms_area: return json.dumps({project: "[]"}, ensure_ascii=False)
        snapshots = []
        snap = {}
        for room_type, area_cnts in rooms_area.items():
            key = " ".join(str(room_type).strip().split()).lower()
            if not key or key == "nan": continue
            if isinstance(area_cnts, dict):
                areas = []
                for k in area_cnts.keys():
                    s = fmt_area(k)
                    if s and s not in areas: areas.append(s)
                if areas: snap[key] = areas
        if snap: snapshots.append(snap)
        if not snapshots: return json.dumps({project: "[]"}, ensure_ascii=False)
        return json.dumps({project: dict_repr_with_doubled_single_quotes(snapshots)}, ensure_ascii=False)
    
    df["bhk_wise_ca"] = df.apply(build_bhk_wise_ca, axis=1)

    def quarter_sort_key(q):
        try:
            q = str(q).strip()
            part, year = q.split("-")
            return (int(year), int(part.replace("Q", "")))
        except: return (9999, 9)

    agg = df.groupby(["units_project_name_en", "Start_Quarter"], dropna=False)["unit_count"].sum().reset_index()
    agg["_qsort"] = agg["Start_Quarter"].apply(quarter_sort_key)
    agg = agg.sort_values(["units_project_name_en", "_qsort"])
    pj_map = {}
    for p, g in agg.groupby("units_project_name_en"):
        p_str = str(p).strip()
        if not p_str or p_str.lower() == "nan": continue
        pairs = []
        for _, r in g.iterrows():
            q = str(r["Start_Quarter"]).strip()
            if q and q.lower() != "nan": pairs.append([q, int(r["unit_count"])])
        pj_map[p_str] = json.dumps({p_str: pairs}, ensure_ascii=False)
    df["project_wise_commencement_quarter_and_total_units"] = df["units_project_name_en"].map(pj_map)

    def min_max_from_area_dict(val):
        area_dict = to_dict_safe(val)
        result = {}
        for room, areas in area_dict.items():
            room_name = " ".join(str(room).strip().split())
            if not isinstance(areas, dict): continue
            values = []
            for k in areas.keys():
                try: values.append(float(k))
                except: pass
            if values:
                result[room_name] = [round(min(values), 2), round(max(values), 2)]
        return result
    
    df["bhk_wise_min_max"] = df["rooms_area_dict"].apply(lambda x: json.dumps(min_max_from_area_dict(x), ensure_ascii=False))

    # --- Fixing Missing Columns ---
    print("14b. Fixing Missing Columns...")
    # 1. Building_count (from building_data)
    def get_building_count(x):
        if not isinstance(x, str) or not x: return 0
        try:
            obj = json.loads(x)
            if isinstance(obj, (dict, list)): return len(obj)
        except: pass
        return 0
    df["Building_count"] = df["building_data"].apply(get_building_count)
    
    # 2. project_tower_completion_date (Nested JSON format)
    def build_tower_completion_date(row):
        proj_name = row.get("units_project_name_en")
        floors_dict = row.get("_building_floors_dict", {})
        if not isinstance(floors_dict, dict): floors_dict = {}
        comp_date_raw = row.get("completion_date", None)
        
        if pd.isna(proj_name) or not proj_name: return "{}"
        proj_str = str(proj_name).strip()
        
        dt = pd.to_datetime(comp_date_raw, errors="coerce")
        comp_date = dt.strftime("%d-%m-%Y") if pd.notna(dt) else None
        
        if not comp_date or not floors_dict:
            return json.dumps({proj_str: {proj_str: {}}}, ensure_ascii=False)
            
        inner_dict = {str(bnum): comp_date for bnum in floors_dict.keys()}
        return json.dumps({proj_str: {proj_str: inner_dict}}, ensure_ascii=False)
        
    df["project_tower_completion_date"] = df.apply(build_tower_completion_date, axis=1)
    
    # 3. number_of_sanctioned_floors (Nested JSON format)
    def build_sanctioned_floors(row):
        proj_name = row.get("units_project_name_en")
        floors_dict = row.get("_building_floors_dict", {})
        if not isinstance(floors_dict, dict): floors_dict = {}
        
        if pd.isna(proj_name) or not proj_name: return "{}"
        proj_str = str(proj_name).strip()
        
        if not floors_dict:
            return json.dumps({proj_str: {}}, ensure_ascii=False)
            
        inner_dict = {str(k): int(v) for k, v in floors_dict.items()}
        return json.dumps({proj_str: inner_dict}, ensure_ascii=False)
        
    df["number_of_sanctioned_floors"] = df.apply(build_sanctioned_floors, axis=1)
    
    # 4. organization_individual & number_of_developers (from df_dev)
    if os.path.exists(df_dev_file):
        df_dev = pd.read_csv(df_dev_file)
        # Assuming legal_status_en holds the organization/individual info
        if "legal_status_en" in df_dev.columns and "developer_id" in df_dev.columns:
            dev_map = dict(zip(df_dev["developer_id"].astype(str), df_dev["legal_status_en"]))
            # Map taking the first developer_id if multiple exist
            df["organization_individual"] = df["developer_id"].astype(str).apply(lambda x: dev_map.get(str(x).split(',')[0].strip()) if pd.notna(x) else None)
        else:
            df["organization_individual"] = None
    else:
        df["organization_individual"] = None
        
    df["number_of_developers"] = df["developer_id"].astype(str).apply(lambda x: len(x.split(',')) if x and str(x).lower() != 'nan' else 0)

    # --- Column Mapping ---
    print("15. Final mapping to DB2...")

    # Create duplicate columns explicitly
    df['registered_project_name'] = df['units_project_name_en']
    df['project_name_ar'] = df['project_name']
    df['project_name'] = df['units_project_name_en']
    
    mapping = [
        ("project_number", "index"),
        (None, "registered_project_name"),
        (None, "project_name"),
        (None, "project_name_ar"),
        ("area_name_en", "location_name"),
        ("city_name", "city_name"),
        ("Latitude", "project_latitude"),
        ("Longitude", "project_longitude"),
        ("area_latitude", "location_latitude"),
        ("area_longitude", "location_longitude"),
        (None, "plot_number"),
        ("project_id", "project_registration_id"),
        ("Geocode_Status", "is_coordinate_manually_done"),
        ("unit_count", "total_units"),
        ("total_sold", "booked_units"),
        ("commencement_date", "commencement_date"),
        (None, "building_wise_total_booked_units"),
        ("final_proposed_date_of_compeletion", "final_proposed_date_of_completion"),
        ("project_wise_bhk_info", "project_bhk_summary"),
        ("project_wise_commencement_quarter_and_total_units", "project_commencement_quarter_units"),
        ("organization_individual", "organization_individual_name"),
        ("no_of_developers", "number_of_developers"),
        (None, "pincode"),
        (None, "registered_project_count"),
        (None, "remark"),
        (None, "total_fsi"),
        (None, "total_plot_area_sq_m"),
        ("bhk_wise_min_max", "bhk_wise_min_max_area"),
        ("bhk_wise_ca", "bhk_wise_carpet_area"),
        ("property_type_flag", "project_type"),
        (None, "bhk_wise_total_booked_units"),
        ("bhk_wise_total_sold", "carpet_wise_total_booked_units"),
        ("Building_count", "total_building_count"),
        ("project_tower_completion_date", "project_tower_completion_date"),
        ("number_of_sanctioned_floors", "number_of_sanctioned_floors"),
        ("amenity_profile", "amenity_profile"),
        ("age_of_project", "age_of_project"),
        ("construction_status", "construction_status"),
        ("building_grade", "building_grade"),
        ("zoning_type", "zoning_type"),
        ("encumbrance_status", "encumbrance_status"),
        ("country_name", "country_name"),
        ("state_name", "state_name"),
        ("sub_locality", "sub_locality"),
        ("micro_market", "micro_market"),
        ("frontage", "frontage"),
        ("approval_status", "approval_status"),
        ("data_source", "data_source"),
        ("source_accessibility", "source_accessibility"),
        ("source_accessibility_way", "source_accessibility_way"),
        ("sourcing_cost", "sourcing_cost"),
        ("sourcing_time", "sourcing_time"),
        (None, "rera_location_v1"),
        (None, "Old Rera Location"),
        (None, "Super Modified Project Name"),
        (None, "Rera Location - v2"),
    ]
    
    expected_columns = [exp for (_, exp) in mapping]
    rename_dict = {curr: exp for curr, exp in mapping if curr is not None}

    df["city_name"] ="Dubai"
    df["state_name"] = "Dubai"
    df["country_name"] = "United Arab Emirates"
    df["source_accessibility"] = "Easy"
    df["source_accessibility_way"] = "Mining"
    
    existing_rename = {k: v for k, v in rename_dict.items() if k in df.columns}
    df.rename(columns=existing_rename, inplace=True)
    
    output_df = pd.DataFrame(index=df.index)
    for col in expected_columns:
        if col in df.columns:
            output_df[col] = df[col]
        else:
            output_df[col] = None
            
    output_df.to_excel(output_file, index=False)
    print(f"Successfully created {output_file}")
    print(f"Total columns in output: {len(expected_columns)}")

if __name__ == '__main__':
    main()
