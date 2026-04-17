import pandas as pd
import os

print("Starting the pipeline...")

df_transaction = pd.read_csv(r"D:\Dubai\Dubai_Updated_data\Raw_Data\Transactions.csv")
print(f"Transactions.csv loaded successfully. Shape: {df_transaction.shape}")

# Driving Year and Quarter Column from instance_date column

# Step 1: Convert to datetime (invalid → NaT)
df_transaction['instance_date'] = pd.to_datetime(
    df_transaction['instance_date'],
    format='%d-%m-%Y',
    errors='coerce'
)

# Step 2: Count invalid dates
invalid_count = df_transaction['instance_date'].isna().sum()
print(f"Number of invalid dates found: {invalid_count}")

# Step 3: Show problematic original values (important fix)
if invalid_count > 0:
    print("\nProblematic raw values:")
    print(df_transaction.loc[df_transaction['instance_date'].isna(), 'instance_date'].head())

# Step 4: Extract year & quarter using nullable integer (fix for float issue)
df_transaction['year'] = df_transaction['instance_date'].dt.year.astype('Int64')
df_transaction['quarter_num'] = df_transaction['instance_date'].dt.quarter.astype('Int64')

# Step 5: Create clean quarter column (avoid float like Q4.0-2006.0)
df_transaction['quarter'] = (
    'Q' + df_transaction['quarter_num'].astype(str) + '-' + df_transaction['year'].astype(str)
)

# Optional: Replace invalid rows with None instead of "Q<NA>-<NA>"
df_transaction.loc[df_transaction['instance_date'].isna(), 'quarter'] = None

# Step 6: Final check
print(df_transaction[['instance_date', 'year', 'quarter']].head())


flat_types = {
    "Flat", "Villa", "Stacked Townhouses", "Unit"
}

shop_types = {
    "Shop", "Store", "Show Rooms"
}

office_types = {
    "Office", "Clinic", "Workshop", "Warehouse"
}

others = {
    "Hotel Apartment",
    "Hotel Rooms",
    "Gymnasium",
    "Sized Partition",
    "Hotel",
    "Building"
}

def categorize_property(sub_type):
    if sub_type in flat_types:
        return "Flat"
    elif sub_type in shop_types:
        return "Shop"
    elif sub_type in office_types:
        return "Office"
    else:
        return "Others"

print(f"Property Type Distribution started....")
df_transaction['property_type'] = df_transaction['property_sub_type_en'].apply(categorize_property)
print(f"Property Type Distribution completed")

# ----------------------------------------------------------------------
# 1. Define the column mapping (Current_name -> Expected_name)
# ----------------------------------------------------------------------
mapping = [
    ("proj_id", "project_id"),
    ("project_number", "index"),
    ("project_name_en", "project_name"),
    ("village_mr", "village_name_marathi"),
    ("loc_id", "location_id"),
    ("area_name_en", "location_name"),
    ("area_name_ar", "location_name_ar"),
    ("igr_village", "village_name"),
    ("year", "year"),
    ("quarter", "quarter"),
    ("city_id", "city_id"),
    ("city", "city_name"),
    ("transaction_id", "document_number"),
    ("sro_code", "sub_registrar_office_code"),
    ("sro_name", "sub_registrar_office_name"),
    ("document_no", "document_number"),
    ("procedure_name_en", "transaction_type"),
    ("agreement_price", "agreement_price"),
    ("market_value", "guideline_value"),
    ("property_description", "property_description"),
    ("instance_date", "transaction_date"),
    ("floor_no", "floor_number"),
    ("unit_no", "unit_number"),
    ("procedure_area", "net_carpet_area_sq_m"),
    ("balcony_sqmt", "balcony_sq_m"),
    ("terrace_sqmt", "terrace_sq_m"),
    ("seller_name", "seller_name"),
    ("purchaser_name", "buyer_name"),
    ("property_category", "transaction_category"),
    ("internaldocumentnumber", "internal_document_number"),
    ("micrno", "micr_number"),
    ("bank_type", "bank_type"),
    ("party_code", "party_code"),
    ("dateofexecution", "date_of_agreement_execution"),
    ("stampdutypaid", "stamp_duty_paid"),
    ("registrationfees", "registration_fee"),
    (None, "project_latitude"),
    (None, "project_longitude"),
    (None, "location_latitude"),
    (None, "location_longitude"),
    ("property_sub_type_en", "property_type_raw"),
    ("rooms_en", "unit_configuration"),
    ("rooms_ar", "unit_configuration_ar"),
    ("buyer_pincode", "buyer_pincode"),
    ("trans_group_en", "transaction_category"),
    ("locality_of_buyer", "buyer_locality"),
    ("district", "buyer_district"),
    ("statename", "buyer_state"),
    (None, "is_llm_processed"),
    (None, "is_manual_processed"),
    ("building_name_en", "tower_name"),
    ("building_name_ar", "tower_name_ar"),
    ("gross_carpet_sqft", "gross_carpet_area_sq_ft"),
    ("rate_on_gca_sqft", "price_per_sq_ft_gross_carpet"),
    ("is_duplicate", "is_duplicate"),
    ("primary_sale_or_secondary_sale", "sale_type"),
    ("project_type", "project_type"),
    (None, "country_name"),
    (None, "state_name"),
    (None, "micro_market"),
    (None, "sub_locality"),
    (None, "pincode"),
    (None, "parking_count"),
    (None, "facing_direction"),
    (None, "view_type"),
    (None, "furnishing_status"),
    (None, "condition_status"),
    (None, "source_accessibility"),
    (None, "source_accessibility_way"),
    (None, "sourcing_cost"),
    (None, "sourcing_time"),
    (None, "data_type"),
    (None, "data_source"),
]

rename_dict = {curr: exp for curr, exp in mapping if curr is not None}
columns_to_create = [exp for curr, exp in mapping if curr is None]

# ----------------------------------------------------------------------
# 2. Create a copy and rename columns
# ----------------------------------------------------------------------
input_path = r"D:\Dubai\Dubai_Updated_data\Transection_data\DB1_Columns_Allignment\sample_transaction.xlsx"
# df = df_transaction.copy()

print(f"Original DataFrame shape: {df_transaction.shape}")
print(f"Rows: {df_transaction.shape[0]:,}, Columns: {df_transaction.shape[1]}")

# ----------------------------------------------------------------------
# 3. Rename columns
# ----------------------------------------------------------------------
df_transaction.rename(columns=rename_dict, inplace=True)

# ----------------------------------------------------------------------
# 4. Create missing columns
# ----------------------------------------------------------------------
for col in columns_to_create:
    if col not in df_transaction.columns:
        df_transaction[col] = None
        print(f"Created missing column: {col}")

# Add city_name, state_name, country_name to df (the renamed dataframe)
df_transaction['city_name'] = "Dubai"
df_transaction['state_name'] = "Dubai"
df_transaction['country_name'] = "United Arab Emirates"

# ----------------------------------------------------------------------
# 5. Save the processed dataframe (df) to CSV
# ----------------------------------------------------------------------
output_dir = os.path.dirname(input_path)
output_path = os.path.join(output_dir, "DB1.csv")

print("\nSaving full dataset into single CSV file...")
df_transaction.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"\n✅ CSV file created successfully: {output_path}")
print(f"Total rows saved: {len(df_transaction):,}")

# ----------------------------------------------------------------------
# 6. Reorder columns for the processed dataframe
# ----------------------------------------------------------------------
print("\nReordering columns...")
columns_first = [
    'index',
    'project_name',
    'project_name_ar',
    'quarter',
    'year',
    'location_name',
    'location_name_ar',
    'city_name',
    'property_type',
    'project_latitude',
    'project_longitude',
    'location_latitude',
    'location_longitude'
]

# Get existing columns from your desired list
existing_first = [col for col in columns_first if col in df_transaction.columns]

# Reorder columns
df_reordered = df_transaction[existing_first + [col for col in df_transaction.columns if col not in existing_first]]

# Save reordered CSV
reordered_path = r'D:\Dubai\Dubai_Updated_data\Transection_data\DB1_Columns_Allignment\DB1_Reord_test.csv'
save_to_drive = r'G:\.shortcut-targets-by-id\1oGd6xPdp686p0qW-tzZyy5quOpi82hLA\DB1+DB2\Dubai\Dubai_DB1.csv'

# df_reordered.to_csv(reordered_path, index=False, encoding='utf-8-sig')
# print(f"✅ Reordered CSV saved: {reordered_path}")

df_reordered.to_csv(save_to_drive, index=False, encoding='utf-8-sig')
print(f"✅ Saved to Drive (Google Drive) : {save_to_drive}")
