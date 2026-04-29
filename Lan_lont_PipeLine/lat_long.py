import time
from pathlib import Path

import pandas as pd
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim


# -----------------------------
# Customizable file paths
# -----------------------------
DB1_PATH = Path(r"D:\Dubai\Dubai_DB1_test.csv")
DB2_PATH = Path(r"D:\Dubai\Dubai_DB2_test.xlsx")

BASE_DIR = Path(__file__).resolve().parent
TEMP_UNIQUE_PROJECTS_PATH = BASE_DIR / "temp_unique_projects.csv"
COORDINATES_RESULT_PATH = BASE_DIR / "coordinates_results.csv"

LAT_LONG_COLUMNS = [
    "project_latitude",
    "project_longitude",
    "location_latitude",
    "location_longitude",
]


def read_db_files(db1_path, db2_path):
    """Read DB1 CSV and DB2 Excel files."""
    db1 = pd.read_csv(db1_path)
    db2 = pd.read_excel(db2_path)
    return db1, db2


def normalize_index(value):
    """Normalize index values so CSV/Excel numeric formatting does not break matching."""
    if pd.isna(value):
        return None

    value_text = str(value).strip()
    try:
        value_float = float(value_text)
        if value_float.is_integer():
            return str(int(value_float))
    except Exception:
        pass

    return value_text


def ensure_lat_long_columns(df):
    """Create lat/long columns if they are missing."""
    for column in LAT_LONG_COLUMNS:
        if column not in df.columns:
            df[column] = None


def build_unique_project_file(db1, db2, output_path):
    """
    Pick index, project_name, and location_name from DB1 and DB2.

    The combined data is first made unique at full row level, then made unique
    by index because index is treated as the unique project key.
    """
    required_columns = ["index", "project_name", "location_name"]
    frames = []

    for source_name, df in [("DB1", db1), ("DB2", db2)]:
        missing = [column for column in required_columns if column not in df.columns]
        if missing:
            print(f"{source_name} skipped for unique project extraction. Missing columns: {missing}")
            continue

        temp = df[required_columns].copy()
        temp["source"] = source_name
        frames.append(temp)

    if not frames:
        raise ValueError("No DB file contains index, project_name, and location_name columns.")

    unique_projects = pd.concat(frames, ignore_index=True)
    unique_projects = unique_projects.dropna(subset=["index"])
    unique_projects["index_key"] = unique_projects["index"].apply(normalize_index)
    unique_projects["project_name"] = unique_projects["project_name"].fillna("").astype(str).str.strip()
    unique_projects["location_name"] = unique_projects["location_name"].fillna("").astype(str).str.strip()

    unique_projects = unique_projects.drop_duplicates(
        subset=["index_key", "project_name", "location_name"],
        keep="first",
    )
    unique_projects = unique_projects.drop_duplicates(subset=["index_key"], keep="first")
    unique_projects = unique_projects.drop(columns=["source"])

    unique_projects.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Unique project file saved: {output_path}")
    print(f"Total unique indexes/projects: {len(unique_projects):,}")

    return unique_projects


def make_geocoder():
    """Create geopy Nominatim geocoder with rate limiting."""
    geocoder = Nominatim(user_agent="dubai_db_lat_long_pipeline", timeout=20)
    return RateLimiter(
        geocoder.geocode,
        min_delay_seconds=1.2,
        max_retries=2,
        error_wait_seconds=5,
        swallow_exceptions=True,
    )


def geocode_first_match(geocode, queries):
    """Try search queries in order and return the first latitude/longitude found."""
    seen = set()

    for query in queries:
        query = str(query).strip()
        if not query or query in seen:
            continue

        seen.add(query)
        location = geocode(query)
        if location:
            return location.latitude, location.longitude, query

    return None, None, None


def get_coordinates_for_project(geocode, project_name, location_name):
    """
    Fetch project and location coordinates.

    Project search includes location_name to improve accuracy. Location search
    uses location_name with Dubai/UAE context.
    """
    project_name = "" if pd.isna(project_name) else str(project_name).strip()
    location_name = "" if pd.isna(location_name) else str(location_name).strip()

    project_queries = [
        f"{project_name}, {location_name}, Dubai, United Arab Emirates",
        f"{project_name} {location_name}, Dubai, UAE",
        f"{project_name}, Dubai, United Arab Emirates",
    ]
    location_queries = [
        f"{location_name}, Dubai, United Arab Emirates",
        f"{location_name}, Dubai, UAE",
    ]

    project_lat, project_lon, project_query = geocode_first_match(geocode, project_queries)
    location_lat, location_lon, location_query = geocode_first_match(geocode, location_queries)

    return {
        "project_latitude": project_lat,
        "project_longitude": project_lon,
        "location_latitude": location_lat,
        "location_longitude": location_lon,
        "project_search_query": project_query,
        "location_search_query": location_query,
    }


def fetch_lat_long(unique_projects, output_path):
    """Fetch coordinates for unique projects and save them to a result file."""
    existing_results = pd.DataFrame()
    completed_indexes = set()

    if output_path.exists():
        existing_results = pd.read_csv(output_path)
        if "index_key" in existing_results.columns:
            completed_indexes = set(existing_results["index_key"].dropna().astype(str))
        print(f"Existing coordinate file found. Skipping {len(completed_indexes):,} completed indexes.")

    geocode = make_geocoder()
    new_results = []
    total = len(unique_projects)

    for row_number, row in unique_projects.iterrows():
        index_key = str(row["index_key"])
        if index_key in completed_indexes:
            continue

        print(f"Fetching {row_number + 1:,}/{total:,}: {row.get('project_name')} | {row.get('location_name')}")
        coords = get_coordinates_for_project(
            geocode,
            row.get("project_name"),
            row.get("location_name"),
        )

        new_results.append({
            "index": row.get("index"),
            "index_key": index_key,
            "project_name": row.get("project_name"),
            "location_name": row.get("location_name"),
            **coords,
        })

        if len(new_results) % 10 == 0:
            save_coordinate_results(existing_results, new_results, output_path)

        time.sleep(0.2)

    results = save_coordinate_results(existing_results, new_results, output_path)
    print(f"Coordinate result file saved: {output_path}")
    return results


def save_coordinate_results(existing_results, new_results, output_path):
    """Save coordinate results and keep one row per index."""
    new_results_df = pd.DataFrame(new_results)
    results = pd.concat([existing_results, new_results_df], ignore_index=True)

    if not results.empty and "index_key" in results.columns:
        results = results.drop_duplicates(subset=["index_key"], keep="last")

    results.to_csv(output_path, index=False, encoding="utf-8-sig")
    return results


def assign_coordinates(df, coordinates):
    """Assign fetched lat/long values back to a DB dataframe by normalized index."""
    ensure_lat_long_columns(df)

    df["_index_key"] = df["index"].apply(normalize_index)
    coordinates = coordinates.copy()
    coordinates["index_key"] = coordinates["index_key"].astype(str)

    coord_map = coordinates.set_index("index_key")[LAT_LONG_COLUMNS].to_dict("index")

    for column in LAT_LONG_COLUMNS:
        df[column] = df["_index_key"].map(lambda key: coord_map.get(str(key), {}).get(column))

    df.drop(columns=["_index_key"], inplace=True)
    return df


def save_updated_files(db1, db2, db1_path, db2_path):
    """Save updated DB files after creating simple backups."""
    db1_backup = db1_path.with_name(f"{db1_path.stem}_before_lat_long{db1_path.suffix}")
    db2_backup = db2_path.with_name(f"{db2_path.stem}_before_lat_long{db2_path.suffix}")

    if not db1_backup.exists():
        pd.read_csv(db1_path).to_csv(db1_backup, index=False, encoding="utf-8-sig")
    if not db2_backup.exists():
        pd.read_excel(db2_path).to_excel(db2_backup, index=False)

    db1.to_csv(db1_path, index=False, encoding="utf-8-sig")
    db2.to_excel(db2_path, index=False)

    print(f"DB1 updated: {db1_path}")
    print(f"DB2 updated: {db2_path}")
    print(f"Backup DB1: {db1_backup}")
    print(f"Backup DB2: {db2_backup}")


def run_lat_long_pipeline(db1_path=DB1_PATH, db2_path=DB2_PATH):
    """Run the complete lightweight latitude/longitude workflow."""
    db1_path = Path(db1_path)
    db2_path = Path(db2_path)

    db1, db2 = read_db_files(db1_path, db2_path)
    unique_projects = build_unique_project_file(db1, db2, TEMP_UNIQUE_PROJECTS_PATH)
    coordinates = fetch_lat_long(unique_projects, COORDINATES_RESULT_PATH)

    db1 = assign_coordinates(db1, coordinates)
    db2 = assign_coordinates(db2, coordinates)
    save_updated_files(db1, db2, db1_path, db2_path)


if __name__ == "__main__":
    run_lat_long_pipeline()
