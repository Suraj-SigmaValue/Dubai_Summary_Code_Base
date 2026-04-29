# Dubai DB2 Project Data Processing Pipeline

## Introduction

This repository contains a Python-based data processing pipeline for preparing the Dubai project-level DB2 dataset. The script reads raw project, unit, building, and developer data, performs cleaning and aggregation, enriches project-level attributes, and exports a standardized DB2 Excel file.

The main objective of this pipeline is to convert raw Dubai project and unit-level datasets into a clean, structured, and analysis-ready DB2 format that can be used for real estate analytics, reporting, dashboards, AI workflows, and downstream database ingestion.

## Objective

The pipeline is designed to solve the problem of scattered and non-standardized real estate project data. Raw files such as `Projects.csv`, `Units_with_actual_area_sqft.csv`, `Buildings.csv`, and `Developers.csv` contain useful information, but the data is not directly suitable for final DB2 usage.

This script standardizes project identifiers, aggregates unit-level records, creates BHK-wise summaries, calculates carpet area summaries, enriches project records with building-level data, and finally maps all required fields into a fixed DB2 column structure.

## Input Files

The pipeline expects the following source files:

```text
D:\Dubai\Dubai_Updated_data\Raw_Data\Projects.csv
D:\Dubai\Dubai_Updated_data\Project_Unit_Summary\Units_with_actual_area_sqft.csv
D:\Dubai\Dubai_Updated_data\Raw_Data\Buildings.csv
D:\Dubai\Dubai_Updated_data\Raw_Data\Developers.csv
C:\Users\Admin\Downloads\Dubai_LatLong_Final_WithSearch.xlsx
```

The latitude-longitude merge section is currently commented in the script. It can be enabled later if project coordinate enrichment is required.

## Output File

The final standardized DB2 file is exported to:

```text
G:\.shortcut-targets-by-id\1oGd6xPdp686p0qW-tzZyy5quOpi82hLA\DB1+DB2\Dubai\Dubai_DB2.xlsx
```

## Required Python Libraries

Install the required libraries before running the script.

```bash
pip install pandas numpy openpyxl
```

The script also uses standard Python libraries such as `json`, `ast`, `re`, `os`, and `collections`.

## Pipeline Workflow

The script follows a stage-wise processing flow.

First, the project master data is loaded from `Projects.csv`. Duplicate projects are removed using `project_id`, and a normalized project identifier is created to ensure reliable merging across files.

Next, the unit-level file is processed in chunks. This is important because the units file can be large. The script aggregates unit counts, room types, carpet areas, property sub-types, project names, and room-area combinations at the project level.

After unit aggregation, the aggregated unit summary is merged back into the project master dataset. Missing aggregation fields are handled safely using empty dictionaries or blank strings.

The project start and end dates are converted into datetime format. Based on these dates, the script derives start year, end year, start quarter, end quarter, and quarter-year fields.

The script then categorizes unit configurations into standard buckets such as `< 1 B/R`, `1 B/R`, `2 B/R`, `3 B/R`, `4 B/R`, `5 B/R`, `> 5 B/R`, `PENTHOUSE`, `Commercial`, and `Other`.

It also calculates cumulative actual carpet area by multiplying area values with their respective unit counts.

The room-area dictionaries are converted into JSON format. These dictionaries preserve the relationship between unit type, carpet area, and count. This is useful for downstream reporting and BHK-wise analytics.

The script further calculates carpet area per bedroom bucket and creates separate summarized carpet area columns.

Property sub-types are grouped into four broad categories: `Flat`, `Shop`, `Office`, and `Other`. Based on these values, the script creates a `property_type_flag`, which classifies the project as `Residential`, `Residential + Commercial`, `Commercial`, or `NA`.

The pipeline also creates a lossless property subtype area dictionary from raw unit data. This keeps subtype-wise carpet area information while also preserving null-area counts.

Building-level data is then processed from `Buildings.csv`. The script extracts building-wise floor counts, building count, building data JSON, tower completion date, and number of sanctioned floors.

Additional derived columns are created, including commencement date, final proposed completion date, project-wise BHK summary, BHK-wise carpet area, BHK-wise min-max area, carpet-wise total/sold units, and commencement quarter-wise total units.

Finally, the script maps all processed fields into the required DB2 schema and exports the result as an Excel file.

## Key Functions

### `to_dict_safe(x)`

Safely converts input values into dictionaries. It handles actual dictionaries, JSON strings, Python dictionary strings, nulls, empty strings, and invalid values.

### `categorize_units(x)`

Converts raw room configurations into standard unit categories such as `1 B/R`, `2 B/R`, `Commercial`, `PENTHOUSE`, and `Other`.

### `calculate_cumulative_area(x)`

Calculates total carpet area by multiplying each area value with its corresponding count.

### `normalize_project_id(x)`

Standardizes project IDs by removing decimal formatting issues such as `123.0` and converting them into clean string IDs.

### `map_room_key_to_bucket(room_key)`

Maps raw room labels into normalized internal bucket names such as `1_br`, `2_br`, `lt_1_br`, `gt_5_br`, `Commercial`, and `PENTHOUSE`.

### `normalize_rooms_area_dict(x)`

Creates a clean nested dictionary where each unit bucket contains carpet areas and counts.

### `extract_carpet_areas_per_bucket(x)`

Calculates total carpet area for each room bucket and creates dynamic carpet area columns.

## Final DB2 Column Mapping

The final output is mapped into a fixed DB2 structure. Important output columns include:

```text
index
registered_project_name
project_name
project_name_ar
location_name
city_name
project_latitude
project_longitude
location_latitude
location_longitude
plot_number
project_registration_id
is_coordinate_manually_done
total_units
booked_units
commencement_date
building_wise_total_booked_units
final_proposed_date_of_completion
project_bhk_summary
project_commencement_quarter_units
organization_individual_name
number_of_developers
pincode
registered_project_count
remark
total_fsi
total_plot_area_sq_m
bhk_wise_min_max_area
bhk_wise_carpet_area
project_type
bhk_wise_total_booked_units
carpet_wise_total_booked_units
total_building_count
project_tower_completion_date
number_of_sanctioned_floors
amenity_profile
age_of_project
construction_status
building_grade
zoning_type
encumbrance_status
country_name
state_name
sub_locality
micro_market
frontage
approval_status
data_source
source_accessibility
source_accessibility_way
sourcing_cost
sourcing_time
rera_location_v1
Old Rera Location
Super Modified Project Name
Rera Location - v2
```

## How to Run

Open the terminal or command prompt in the folder where the script is saved.

Run the script using:

```bash
python your_script_name.py
```

Replace `your_script_name.py` with the actual Python file name.

## Important Notes

Before running the script, confirm that all input file paths are correct and accessible from your system.

The script uses chunk processing for the units file to handle large datasets efficiently.

The latitude-longitude merge logic is currently commented. Enable it only when the coordinate file is finalized and required.

The final output is written directly to Google Drive. Make sure the Google Drive path is available and synced before execution.

Some columns are intentionally created as blank or null because they are required in the DB2 schema but may not be available in the raw Dubai source files.

## Output Validation Checklist

After running the script, validate the output file for the following points:

```text
The output Excel file is created successfully.
Total columns match the expected DB2 schema.
Project names are populated correctly.
Total units are aggregated correctly from the units file.
BHK-wise summaries are created properly.
Building count is populated where building data is available.
Country, state, source accessibility, and source accessibility way are populated.
JSON-format columns are readable and not broken.
No required DB2 column is missing.
```

## Recommended Improvements

The current script is functionally complete, but the following improvements can make it more production-ready:

```text
Move all file paths into a separate config file.
Add logging instead of only print statements.
Add try-except error handling around each major stage.
Save a validation checklist Excel file after processing.
Add row-count reconciliation between raw and processed files.
Create a separate schema file for expected DB2 columns.
Enable lat-long merge only through a configuration flag.
Optimize row-wise loops further for very large unit datasets.
```

## Conclusion

This pipeline converts raw Dubai project and unit-level data into a standardized DB2-ready Excel file. It handles project ID normalization, unit aggregation, BHK categorization, carpet area calculation, building enrichment, developer enrichment, and final schema mapping.

The final output is suitable for real estate analytics, DB ingestion, dashboard reporting, and AI-based property intelligence workflows.
