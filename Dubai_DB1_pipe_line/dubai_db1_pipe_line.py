import pandas as pd
import os

r"""
Dubai DB1 transaction pipeline column documentation
===================================================

Source file:
    D:\Dubai\Dubai_Updated_data\Raw_Data\Transactions.csv

This pipeline creates the DB1 transaction output by:
    1. Reading raw Dubai transaction records.
    2. Deriving date and property classification columns.
    3. Renaming raw columns to the expected DB1 column names.
    4. Creating missing DB1 schema columns with default values.
    5. Reordering selected important columns at the front of the output.

Derived columns and applied logic
---------------------------------
instance_date:
    Source: raw instance_date.
    Logic: converted from string to pandas datetime using format '%d-%m-%Y'.
    Invalid dates are converted to NaT because errors='coerce' is used.

year:
    Source: instance_date after datetime conversion.
    Logic: extracts the calendar year using instance_date.dt.year.
    Stored as pandas nullable integer Int64 so missing dates remain <NA>
    instead of becoming float values.

quarter_num:
    Source: instance_date after datetime conversion.
    Logic: extracts quarter number 1, 2, 3, or 4 using instance_date.dt.quarter.
    Stored as nullable integer Int64.
    Note: this is an intermediate helper column and is not included in the
    rename mapping.

quarter:
    Source: quarter_num and year.
    Logic: concatenates values into 'Q<quarter_num>-<year>', for example
    'Q4-2006'. If instance_date is invalid or missing, quarter is set to None
    to avoid values like 'Q<NA>-<NA>'.

property_type:
    Source: property_sub_type_en.
    Logic:
        - Flat if property_sub_type_en is one of:
          Flat, Villa, Stacked Townhouses, Unit.
        - Shop if property_sub_type_en is one of:
          Shop, Store, Show Rooms.
        - Office if property_sub_type_en is one of:
          Office, Clinic, Workshop, Warehouse.
        - Others for every other value, including the values listed in the
          others set and any unexpected or missing subtype.

Renamed DB1 columns
-------------------
These columns are created by renaming raw source columns. No calculation is
applied except where the source column itself was already derived earlier.

    project_id                         <- proj_id
    index                              <- project_number
    project_name                       <- project_name_en
    village_name_marathi               <- village_mr
    location_id                        <- loc_id
    location_name                      <- area_name_en
    location_name_ar                   <- area_name_ar
    village_name                       <- igr_village
    year                               <- year, derived from instance_date
    quarter                            <- quarter, derived from instance_date
    city_id                            <- city_id
    city_name                          <- city, later overwritten as 'Dubai'
    document_number                    <- transaction_id and document_no
    sub_registrar_office_code          <- sro_code
    sub_registrar_office_name          <- sro_name
    transaction_type                   <- procedure_name_en
    agreement_price                    <- actual_worth
    guideline_value                    <- market_value
    property_description               <- property_description
    transaction_date                   <- instance_date
    floor_number                       <- floor_no
    unit_number                        <- unit_no
    net_carpet_area_sq_m               <- procedure_area
    balcony_sq_m                       <- balcony_sqmt
    terrace_sq_m                       <- terrace_sqmt
    seller_name                        <- seller_name
    buyer_name                         <- purchaser_name
    transaction_category               <- property_category and trans_group_en
    internal_document_number           <- internaldocumentnumber
    micr_number                        <- micrno
    bank_type                          <- bank_type
    party_code                         <- party_code
    date_of_agreement_execution        <- dateofexecution
    stamp_duty_paid                    <- stampdutypaid
    registration_fee                   <- registrationfees
    property_type_raw                  <- property_sub_type_en
    unit_configuration                 <- rooms_en
    unit_configuration_ar              <- rooms_ar
    buyer_pincode                      <- buyer_pincode
    buyer_locality                     <- locality_of_buyer
    buyer_district                     <- district
    buyer_state                        <- statename
    tower_name                         <- building_name_en
    tower_name_ar                      <- building_name_ar
    gross_carpet_area_sq_ft            <- gross_carpet_sqft
    price_per_sq_ft_gross_carpet       <- rate_on_gca_sqft
    is_duplicate                       <- is_duplicate
    sale_type                          <- primary_sale_or_secondary_sale
    project_type                       <- project_type

Important note about duplicate target names:
    document_number appears twice in the mapping:
        transaction_id -> document_number
        document_no    -> document_number
    transaction_category also appears twice:
        property_category -> transaction_category
        trans_group_en    -> transaction_category
    pandas rename will allow duplicate column names if both source columns are
    present. The pipeline does not currently choose one source over the other.

Columns created with null/default values
----------------------------------------
These expected DB1 columns do not come from the raw source in this script. They
are first created as None if missing:

    project_latitude
    project_longitude
    location_latitude
    location_longitude
    is_llm_processed
    is_manual_processed
    country_name
    state_name
    micro_market
    sub_locality
    pincode
    parking_count
    facing_direction
    view_type
    furnishing_status
    condition_status
    source_accessibility
    source_accessibility_way
    sourcing_cost
    sourcing_time
    data_type
    data_source

After creation, these columns are overwritten with fixed values:
    city_name                 = 'Dubai'
    state_name                = 'Dubai'
    country_name              = 'United Arab Emirates'
    is_llm_processed          = 'No'
    is_manual_processed       = 'No'
    source_accessibility_way  = 'Download'
    source_accessibility      = 'Easy'
    data_type                 = 'Registered Document'
    data_source               = 'DLD'

Columns that remain None unless populated elsewhere:
    project_latitude, project_longitude, location_latitude,
    location_longitude, micro_market, sub_locality, pincode, parking_count,
    facing_direction, view_type, furnishing_status, condition_status,
    sourcing_cost, sourcing_time.

Output order logic
------------------
The pipeline moves the following columns to the front if they exist:
    index, project_name, project_name_ar, quarter, year, location_name,
    location_name_ar, city_name, property_type, project_latitude,
    project_longitude, location_latitude, location_longitude.
All other columns keep their existing order after these priority columns.
"""

print("Starting the pipeline...")

df_transaction = pd.read_csv(r"G:\My Drive\Dubai\Raw Data\Transactions_with_latlong.csv")
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
    (None, "village_name_marathi"), # ("village_mr", "village_name_marathi"),
    ("loc_id", "location_id"),
    ("area_name_en", "location_name"),
    ("area_name_ar", "location_name_ar"),
    (None, "village_name"), # ("igr_village", "village_name"),
    ("year", "year"),
    ("quarter", "quarter"),
    ("city_id", "city_id"),
    ("city", "city_name"),
    ("transaction_id", "document_number"),
    (None, "sub_registrar_office_code"), # ("sro_code", "sub_registrar_office_code"),
    (None, "sub_registrar_office_name"), # ("sro_name", "sub_registrar_office_name"),
    ("document_no", "document_number"),
    ("procedure_name_en", "transaction_type"),
    ("actual_worth", "agreement_price"),
    (None, "guideline_value"), # ("market_value", "guideline_value"),
    (None, "property_description"), # ("property_description", "property_description"),
    ("instance_date", "transaction_date"),
    (None, "floor_number"), # ("floor_no", "floor_number"),
    (None, "unit_number"), # ("unit_no", "unit_number"),
    ("procedure_area", "net_carpet_area_sq_m"),
    (None, "balcony_sq_m"), # ("balcony_sqmt", "balcony_sq_m"),
    (None, "terrace_sq_m"), # ("terrace_sqmt", "terrace_sq_m"),
    (None, "seller_name"), # ("seller_name", "seller_name"),
    (None, "buyer_name"), # ("purchaser_name", "buyer_name"),
    ("property_category", "transaction_category"),
    (None, "internal_document_number"), # ("internaldocumentnumber", "internal_document_number"),
    (None, "micr_number"), ("micrno", "micr_number"),
    (None, "bank_type"), # ("bank_type", "bank_type"),
    (None, "party_code"), # ("party_code", "party_code"),
    (None, "date_of_agreement_execution"), # ("dateofexecution", "date_of_agreement_execution"),
    (None, "stamp_duty_paid"), # ("stampdutypaid", "stamp_duty_paid"),
    (None, "registration_fee"), # ("registrationfees", "registration_fee"),
    ("Latitude", "project_latitude"),
    ("Longitude", "project_longitude"),
    (None, "location_latitude"),
    (None, "location_longitude"),
    ("property_sub_type_en", "property_type_raw"),
    ("rooms_en", "unit_configuration"),
    ("rooms_ar", "unit_configuration_ar"),
    (None, "buyer_pincode"), # ("buyer_pincode", "buyer_pincode"),
    ("trans_group_en", "transaction_category"),
    (None, "buyer_locality"), # ("locality_of_buyer", "buyer_locality"),
    (None, "buyer_district"),# ("district", "buyer_district"),
    (None, "buyer_state"), # ("statename", "buyer_state"),
    (None, "is_llm_processed"),
    (None, "is_manual_processed"),
    ("building_name_en", "tower_name"),
    ("building_name_ar", "tower_name_ar"),
    (None, "gross_carpet_area_sq_ft"), # ("gross_carpet_sqft", "gross_carpet_area_sq_ft"),
    (None, "price_per_sq_ft_gross_carpet"), #("rate_on_gca_sqft", "price_per_sq_ft_gross_carpet")
    (None, "is_duplicate"), # ("is_duplicate", "is_duplicate"),
    (None, "sale_type"), # ("primary_sale_or_secondary_sale", "sale_type"),
    ("property_usage_en", "project_type"),
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
df_transaction['is_llm_processed'] = 'No'
df_transaction['is_manual_processed'] = 'No'
df_transaction['country_name'] = 'United Arab Emirates'
df_transaction['source_accessibility_way'] = 'Download'
df_transaction['source_accessibility'] = 'Easy'
df_transaction['data_type'] = 'Registered Document'
df_transaction['data_source'] = 'DLD'

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
reordered_path = r'D:\Dubai\Dubai_DB1.csv'
# save_to_drive = r'G:\.shortcut-targets-by-id\1oGd6xPdp686p0qW-tzZyy5quOpi82hLA\DB1+DB2\Dubai\Dubai_DB1.csv'

df_reordered.to_csv(reordered_path, index=False, encoding='utf-8-sig')
print(f"✅ Reordered CSV saved: {reordered_path}")

# df_reordered.to_csv(save_to_drive, index=False, encoding='utf-8-sig')
# print(f"✅ Saved to Drive (Google Drive) : {save_to_drive}")
