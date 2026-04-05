# ETL Pipeline

A simple, production-ready ETL (Extract, Transform, Load) pipeline built with Python for processing CSV data and loading it into multiple destinations.

## 📋 Overview

This ETL pipeline extracts data from CSV files, performs data cleaning and transformation, and loads the results into both CSV files and SQLite databases.

## 🎯 Features

- **Extract**: Read data from CSV files using pandas
- **Transform**: 
  - Clean missing values
  - Rename columns to lowercase
  - Filter rows based on conditions
  - Add calculated columns
- **Load**: 
  - Save to CSV files
  - Load into SQLite database
- **Error Handling**: Graceful error handling with detailed logging
- **Configurable**: All file paths and parameters are configurable
- **Logging**: Comprehensive logging for monitoring and debugging

## 🚀 Getting Started

### Prerequisites

```bash
pip install pandas
```

SQLite3 is included in Python's standard library.

### Installation

1. Clone or download this repository
2. Install dependencies:
   ```bash
   pip install pandas
   ```

### Usage

#### Basic Usage

```python
python etl_pipeline.py
```

#### Custom Configuration

```python
from etl_pipeline import main

main(
    input_file='data/sales.csv',
    output_file='data/sales_cleaned.csv',
    db_path='data/sales.db',
    key_column='order_id',
    table_name='sales_data'
)
```

## 📁 Project Structure

```
ETL pipeline/
├── etl_pipeline.py          # Main ETL pipeline script
├── README.md                # This file
├── input.csv                # Sample input file (create your own)
├── output.csv               # Generated output file
└── etl_data.db             # Generated SQLite database
```

## 🔧 Pipeline Stages

### 1. Extract

Reads data from a CSV file into a pandas DataFrame.

```python
df = extract('input.csv')
```

**Features**:
- File existence validation
- Error handling for corrupted files
- Row and column count logging

### 2. Transform

Cleans and transforms the data according to business rules.

```python
df_transformed = transform(df, key_column='id')
```

**Transformations**:
- **Column Renaming**: Converts all column names to lowercase
- **Null Filtering**: Removes rows with null values in specified key column
- **Missing Value Handling**: 
  - Numeric columns: Fill with 0
  - Text columns: Fill with 'Unknown'
- **Calculated Columns**: Adds a `row_id` column for tracking

### 3. Load

Saves the transformed data to multiple destinations.

```python
load_to_csv(df_transformed, 'output.csv')
load_to_database(df_transformed, 'etl_data.db', 'etl_data')
```

**Destinations**:
- **CSV File**: For easy sharing and analysis
- **SQLite Database**: For querying and integration

## 📊 Sample Input Data

Create an `input.csv` file with sample data:

```csv
ID,Name,Age,Salary,Department
1,John Doe,30,50000,Engineering
2,Jane Smith,,60000,Marketing
3,Bob Johnson,35,,Sales
4,,28,45000,Engineering
5,Alice Brown,32,55000,
```

## 📈 Sample Output

After running the pipeline:

**output.csv**:
```csv
id,name,age,salary,department,row_id
1,John Doe,30,50000,Engineering,1
2,Jane Smith,0,60000,Marketing,2
3,Bob Johnson,35,0,Sales,3
4,Unknown,28,45000,Engineering,4
5,Alice Brown,32,55000,Unknown,5
```

**SQLite Database**:
```sql
SELECT * FROM etl_data;
-- Returns the same data as output.csv
```

## 🔍 Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `input_file` | str | 'input.csv' | Path to input CSV file |
| `output_file` | str | 'output.csv' | Path to output CSV file |
| `db_path` | str | 'etl_data.db' | Path to SQLite database |
| `key_column` | str | None | Column to check for null values |
| `table_name` | str | 'etl_data' | Database table name |

## 📝 Logging

The pipeline provides detailed logging at each stage:

```
2026-04-05 23:48:00 - INFO - ==================================================
2026-04-05 23:48:00 - INFO - ETL Pipeline Started
2026-04-05 23:48:00 - INFO - ==================================================
2026-04-05 23:48:00 - INFO - Extracting data from input.csv
2026-04-05 23:48:00 - INFO - Extracted 100 rows and 5 columns
2026-04-05 23:48:00 - INFO - Starting data transformation
2026-04-05 23:48:00 - INFO - Renamed columns to lowercase
2026-04-05 23:48:00 - INFO - Removed rows with null values in 'id'
2026-04-05 23:48:00 - INFO - Filled remaining missing values
2026-04-05 23:48:00 - INFO - Added calculated column 'row_id'
2026-04-05 23:48:00 - INFO - Transformation complete: 5 rows removed, 95 rows remaining
2026-04-05 23:48:00 - INFO - Loading data to CSV: output.csv
2026-04-05 23:48:00 - INFO - Successfully saved 95 rows to output.csv
2026-04-05 23:48:00 - INFO - Loading data to SQLite database: etl_data.db
2026-04-05 23:48:00 - INFO - Successfully loaded 95 rows to table 'etl_data'
2026-04-05 23:48:00 - INFO - ==================================================
2026-04-05 23:48:00 - INFO - ETL Pipeline Completed Successfully
2026-04-05 23:48:00 - INFO - ==================================================
```

## 🛠️ Error Handling

The pipeline handles common errors gracefully:

- **File Not Found**: Clear error message if input file doesn't exist
- **Invalid CSV**: Handles corrupted or malformed CSV files
- **Database Errors**: Catches and logs database connection issues
- **Transform Errors**: Validates column existence before operations

## 🔄 Extending the Pipeline

### Add Custom Transformations

```python
def transform(df: pd.DataFrame, key_column: str = None) -> pd.DataFrame:
    # ... existing code ...
    
    # Add your custom transformation
    df['total_with_tax'] = df['amount'] * 1.1
    
    return df
```

### Add New Load Destinations

```python
def load_to_postgres(df: pd.DataFrame, connection_string: str) -> None:
    """Load data to PostgreSQL database"""
    from sqlalchemy import create_engine
    engine = create_engine(connection_string)
    df.to_sql('etl_data', engine, if_exists='replace', index=False)
```

## 📚 Use Cases

- **Data Migration**: Move data between systems
- **Data Cleaning**: Standardize and clean raw data
- **Reporting**: Prepare data for analysis and reporting
- **Data Integration**: Combine data from multiple sources
- **Batch Processing**: Process large datasets in batches

## 🎓 Best Practices

1. **Always validate input data** before transformation
2. **Log every stage** for debugging and monitoring
3. **Handle errors gracefully** to prevent pipeline failures
4. **Make configurations external** for flexibility
5. **Test with sample data** before production runs
6. **Document transformations** for maintainability

## 🚦 Next Steps

- Add support for multiple input files
- Implement incremental loading
- Add data quality checks
- Support for JSON and Excel formats
- Parallel processing for large datasets
- Add email notifications on completion/failure
- Implement scheduling with cron or Airflow

## 📄 License

This project is open source and available for educational and commercial use.

## 🤝 Contributing

Feel free to extend and modify the pipeline for your specific needs.
# ETL-pipeline
