# BigQuery Notebooks Tutorial: Loading Data
The below step will load the data from csv, json , txt to mimic loading from various data source.

##1. Load Books data.(csv)
1.1  Import required libraries and config
```python
# Import required libraries
import google.cloud.bigquery as bq
from google.cloud import storage
import pandas as pd
import json

# Initialize BigQuery client
client = bq.Client()
storage_client = storage.Client()

# Configuration
PROJECT_ID = client.project
DATASET_ID = 'EA_DEMO_RAW'
BUCKET_NAME = f'ea-demo-1raw'

print(f'The project currently set is :{PROJECT_ID}')
```
1.2 To load the data we can create a function as below:
```python
# Load  data from CSV
def load_data():
    """Load books data from CSV into T_BOOKS_RAW table"""

    table_id = f"{PROJECT_ID}.{DATASET_ID}.T_BOOKS_RAW"

    # Configure the load job
    job_config = bq.LoadJobConfig(
        source_format=bq.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header row
        autodetect=False,  # Don't auto-detect schema, use existing table schema
        write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,  # Replace existing data
        max_bad_records=0  # Fail if any bad records
    )

    # GCS URI for books CSV file
    uri = f"gs://{BUCKET_NAME}/books.csv"

    print(f" Loading books data from: {uri}")
    print(f" Target table: {table_id}")

    # Start the load job
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )

    # Wait for job to complete
    load_job.result()

    # Get the updated table info
    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows:,} rows into T_BOOKS_RAW")
    return load_job
```
1.3 let us call this function.
```python
books_job=load_data()
```

## Load Customer data. (Json)


## Load Sales data. (Json)
The sales.json contains the details of th
## Laod Weblog data.(txt)
