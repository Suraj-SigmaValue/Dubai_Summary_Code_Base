# Dubai DB1 Transaction Pipeline

A Python-based data processing pipeline designed to clean, align, and organize Dubai real estate transaction data before saving it to Google Drive.

## Features

- **Date Processing**: Automatically derives Year and Quarter from transaction dates.
- **Property Categorization**: Groups property sub-types into standard categories (Flat, Shop, Office, Others).
- **Column Alignment**: Renames raw data columns to standardized internal names using a comprehensive mapping.
- **Schema Management**: Ensures all expected columns exist, even if missing from the source data.
- **Reordering**: Sorts columns for optimal readability and downstream consumption.
- **Cloud Integration**: Supports saving directly to a synchronized Google Drive path.

## Prerequisites

- Python 3.8+
- [Optional] Google Drive for Desktop (if saving to the `G:` drive path)

## Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configuration**:
   Update `settings.yaml` with your Google Cloud credentials if using automated upload features.

## Usage

Run the main pipeline script:

```bash
python dubai_db1_pipe_line.py
```

The script will:
1. Load `Transactions.csv` from the raw data directory.
2. Process and transform the data.
3. Save a local copy in the workspace.
4. Export the final reordered dataset to the configured Google Drive path.

## Project Structure

- `dubai_db1_pipe_line.py`: Main execution script.
- `settings.yaml`: Configuration for APIs and credentials.
- `requirements.txt`: Python package dependencies.
- `.gitignore`: Files and directories to be ignored by Git.
- `DB1_Columns_Allignment/`: Workspace for processed CSV and Excel files.

## Column Mapping

The pipeline maps raw source columns to standardized names such as:
- `proj_id` -> `project_id`
- `agreement_price` -> `agreement_price`
- `instance_date` -> `transaction_date`
- ... and many others.
