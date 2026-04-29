import pandas as pd
import warnings
from pathlib import Path


SCHEMA_CONFIG = {
    "DB1": {
        "file": "DB1_Schema.xlsx",
        "column_name": "DB1(Transaction DB)-Actual",
    },
    "DB2": {
        "file": "DB2_Schema.xlsx",
        "column_name": "DB2(Project_DB)-Actual",
    },
}


def detect_db_type(input_file):
    """Detect whether the uploaded file should be validated against DB1 or DB2."""
    file_name = Path(input_file).name.upper()

    if "DB1" in file_name:
        return "DB1"
    if "DB2" in file_name:
        return "DB2"

    return None


def read_input_file(input_file):
    """Read CSV or Excel input file based on file extension."""
    input_path = Path(input_file)
    suffix = input_path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(input_file)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(input_file, sheet_name=0)

    raise ValueError(f"Unsupported input file type: {suffix}")


def normalize_dtype_name(dtype):
    """Convert pandas dtype into a simple reporting name."""
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    if pd.api.types.is_float_dtype(dtype):
        return "DECIMAL"
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATE"
    return "TEXT"


def is_integer_like(series):
    """Return True when a numeric series contains only whole numbers."""
    numeric_values = pd.to_numeric(series.dropna(), errors="coerce")

    if numeric_values.empty:
        return None
    if numeric_values.isna().any():
        return False

    return bool((numeric_values % 1 == 0).all())


def is_date_like(series):
    """Return True when non-null values can be parsed as dates."""
    non_null_values = series.dropna()

    if non_null_values.empty:
        return None

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        parsed_values = pd.to_datetime(non_null_values, errors="coerce")
        parsed_dayfirst_values = pd.to_datetime(non_null_values, errors="coerce", dayfirst=True)

    return bool(max(parsed_values.notna().mean(), parsed_dayfirst_values.notna().mean()) >= 0.9)


def validate_series_dtype(series, expected_type):
    """
    Validate one uploaded column against the schema Data_Type.

    Empty columns are marked as Unknown because there is no value available to
    prove or disprove the expected data type.
    """
    expected = str(expected_type).strip().upper()
    actual = normalize_dtype_name(series.dtype)

    if series.count() == 0:
        return actual, "Unknown", "Column is present but all values are null"

    if expected == "INTEGER":
        integer_like = is_integer_like(series)
        if pd.api.types.is_integer_dtype(series) or integer_like is True:
            return actual, "Pass", ""
        return actual, "Fail", "Expected integer values"

    if expected == "DECIMAL":
        numeric_values = pd.to_numeric(series.dropna(), errors="coerce")
        if not numeric_values.empty and numeric_values.notna().all():
            return actual, "Pass", ""
        return actual, "Fail", "Expected decimal/numeric values"

    if expected in {"VARCHAR", "TEXT"}:
        if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
            return actual, "Pass", ""
        return actual, "Fail", "Expected text values"

    if expected == "DATE":
        if pd.api.types.is_datetime64_any_dtype(series) or is_date_like(series):
            return actual, "Pass", ""
        return actual, "Fail", "Expected date values"

    return actual, "Unknown", f"Unsupported schema data type: {expected}"


def build_schema_validation_report(input_file, df, schema_file=None):
    """Compare uploaded DB1/DB2 file columns and dtypes with the matching schema."""
    db_type = detect_db_type(input_file)

    if schema_file is None:
        if db_type is None:
            return pd.DataFrame([{
                "Validation Type": "Schema Detection",
                "Schema": "",
                "Schema Column": "",
                "Expected Data Type": "",
                "Uploaded Column": "",
                "Column Present": "Unknown",
                "Actual Data Type": "",
                "Data Type Result": "Skipped",
                "Issue": "Could not detect DB1 or DB2 from input file name",
            }])

        schema_path = Path(__file__).resolve().parent / SCHEMA_CONFIG[db_type]["file"]
        schema_column = SCHEMA_CONFIG[db_type]["column_name"]
    else:
        schema_path = Path(schema_file)
        db_type = db_type or schema_path.stem.split("_")[0].upper()
        schema_column = SCHEMA_CONFIG.get(db_type, {}).get("column_name")

    if not schema_path.exists():
        return pd.DataFrame([{
            "Validation Type": "Schema File",
            "Schema": db_type,
            "Schema Column": "",
            "Expected Data Type": "",
            "Uploaded Column": "",
            "Column Present": "Fail",
            "Actual Data Type": "",
            "Data Type Result": "Skipped",
            "Issue": f"Schema file not found: {schema_path}",
        }])

    schema_df = pd.read_excel(schema_path, sheet_name=0)

    if schema_column is None:
        actual_columns = [col for col in schema_df.columns if "ACTUAL" in str(col).upper()]
        schema_column = actual_columns[0] if actual_columns else None

    if schema_column not in schema_df.columns or "Data_Type" not in schema_df.columns:
        return pd.DataFrame([{
            "Validation Type": "Schema Columns",
            "Schema": db_type,
            "Schema Column": schema_column or "",
            "Expected Data Type": "",
            "Uploaded Column": "",
            "Column Present": "Fail",
            "Actual Data Type": "",
            "Data Type Result": "Skipped",
            "Issue": "Schema must contain actual column name and Data_Type columns",
        }])

    schema_columns = schema_df[[schema_column, "Data_Type"]].dropna(subset=[schema_column]).copy()
    schema_columns[schema_column] = schema_columns[schema_column].astype(str).str.strip()
    schema_columns["Data_Type"] = schema_columns["Data_Type"].astype(str).str.strip()

    uploaded_columns = set(df.columns)
    expected_columns = set(schema_columns[schema_column])
    validation_rows = []

    for _, schema_row in schema_columns.iterrows():
        expected_column = schema_row[schema_column]
        expected_type = schema_row["Data_Type"]

        if expected_column in uploaded_columns:
            actual_type, dtype_result, issue = validate_series_dtype(df[expected_column], expected_type)
            column_present = "Pass"
        else:
            actual_type = ""
            dtype_result = "Skipped"
            issue = "Column missing in uploaded file"
            column_present = "Fail"

        validation_rows.append({
            "Validation Type": "Expected Column",
            "Schema": db_type,
            "Schema Column": expected_column,
            "Expected Data Type": expected_type,
            "Uploaded Column": expected_column if expected_column in uploaded_columns else "",
            "Column Present": column_present,
            "Actual Data Type": actual_type,
            "Data Type Result": dtype_result,
            "Issue": issue,
        })

    for extra_column in sorted(uploaded_columns - expected_columns):
        validation_rows.append({
            "Validation Type": "Extra Uploaded Column",
            "Schema": db_type,
            "Schema Column": "",
            "Expected Data Type": "",
            "Uploaded Column": extra_column,
            "Column Present": "Extra",
            "Actual Data Type": normalize_dtype_name(df[extra_column].dtype),
            "Data Type Result": "Skipped",
            "Issue": "Column exists in uploaded file but not in schema",
        })

    return pd.DataFrame(validation_rows)


def analyze_excel_file(input_file, output_file=None, schema_file=None):
    """
    Analyze CSV/Excel file and generate detailed report.

    Parameters:
    input_file: path to input CSV/Excel file
    output_file: path for output report (optional, auto-generated if not provided)
    schema_file: optional schema file path. If not provided, DB1/DB2 schema is
                 selected automatically from the input file name.
    """
    if output_file is None:
        input_path = Path(input_file)
        output_file = input_path.stem + "_analysis_report.xlsx"

    print(f"Analyzing: {input_file}")

    try:
        df = read_input_file(input_file)
        print("File loaded successfully")
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    schema_validation_df = build_schema_validation_report(input_file, df, schema_file)
    report_data = []

    print(f"Total rows: {len(df)}")
    print(f"Total columns: {len(df.columns)}")
    print("-" * 50)

    for col in df.columns:
        col_data = df[col]
        total_count = len(col_data)
        non_null_count = col_data.count()
        null_count = total_count - non_null_count
        null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0
        dtype = str(col_data.dtype)
        unique_count = col_data.nunique()

        sample_values = col_data.dropna().head(3).tolist()
        sample_str = ", ".join(str(value) for value in sample_values) if sample_values else "No values"

        # min_val = ""
        # max_val = ""
        # mean_val = ""

        # if pd.api.types.is_numeric_dtype(col_data):
        #     min_val = col_data.min()
        #     max_val = col_data.max()
        #     mean_val = round(col_data.mean(), 2) if not pd.isna(col_data.mean()) else ""
        # elif pd.api.types.is_datetime64_any_dtype(col_data):
        #     min_val = col_data.min() if not col_data.isna().all() else ""
        #     max_val = col_data.max() if not col_data.isna().all() else ""

        report_data.append({
            "Column Name": col,
            "Data Type": dtype,
            "Total Values": total_count,
            "Non-Null Count": non_null_count,
            "Null Count": null_count,
            "Null Percentage": round(null_percentage, 2),
            "Unique Values": unique_count,
            # "Minimum Value": min_val,
            # "Maximum Value": max_val,
            # "Mean/Average": mean_val,
            "Sample Values (first 3)": sample_str,
        })

        print(f"Analyzed: {col} - {dtype} - {null_percentage:.1f}% null")

    report_df = pd.DataFrame(report_data)

    summary = {
        "Metric": [
            "Total Rows",
            "Total Columns",
            "Total Memory Usage (MB)",
            "Columns with Null Values",
            "Columns with 100% Null Values",
            "Numeric Columns",
            "Text/Object Columns",
            "Date/Time Columns",
            "Schema Expected Columns",
            "Schema Missing Columns",
            "Schema Extra Uploaded Columns",
            "Schema Data Type Failures",
        ],
        "Value": [
            len(df),
            len(df.columns),
            round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2),
            (report_df["Null Count"] > 0).sum(),
            (report_df["Null Percentage"] == 100).sum(),
            sum("int" in str(dtype) or "float" in str(dtype) for dtype in report_df["Data Type"]),
            sum("object" in str(dtype) or "string" in str(dtype) for dtype in report_df["Data Type"]),
            sum("datetime" in str(dtype) for dtype in report_df["Data Type"]),
            (schema_validation_df["Validation Type"] == "Expected Column").sum(),
            (schema_validation_df["Column Present"] == "Fail").sum(),
            (schema_validation_df["Validation Type"] == "Extra Uploaded Column").sum(),
            (schema_validation_df["Data Type Result"] == "Fail").sum(),
        ],
    }

    summary_df = pd.DataFrame(summary)

    row_sample = df.head(10).copy()
    row_sample.insert(0, "Row_Number", range(1, len(row_sample) + 1))

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        report_df.to_excel(writer, sheet_name="Column Analysis", index=False)
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        row_sample.to_excel(writer, sheet_name="First 10 Rows Sample", index=False)
        schema_validation_df.to_excel(writer, sheet_name="Schema Validation", index=False)

        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        max_length = max(max_length, len(str(cell.value)))
                    except Exception:
                        pass
                worksheet.column_dimensions[column_letter].width = min(max_length + 2, 50)

    print("-" * 50)
    print("Report generated successfully!")
    print(f"Report saved as: {output_file}")
    print("\nQuick Stats:")
    print(f"   - {summary['Value'][2]} MB memory usage")
    print(f"   - {summary['Value'][4]} columns are completely empty")
    print(f"   - {summary['Value'][3]} columns contain some null values")
    print(f"   - Schema validation rows: {len(schema_validation_df)}")


if __name__ == "__main__":
    input_excel = r"D:\Dubai\Dubai_DB1.csv"

    # analyze_excel_file(input_excel)

    # Custom examples:
    # analyze_excel_file(input_excel, "my_custom_report.xlsx")
    analyze_excel_file(input_excel, schema_file=r"D:\Dubai Data\CheckList\DB1_Schema.xlsx")
