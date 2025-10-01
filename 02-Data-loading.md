# Section 2: BigQuery Notebooks Tutorial: Loading Data
The below step will load the data from csv, json , txt to mimic loading from various data source.

## 1. Load Books data.(csv)
create a Notebook and name it 
```python
Load_Data_Books
```
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

## 2.Load Customer data. (Json)
create a Notebook and name it 
```python
Load_Data_Customer
```
2.1   Import required libraries and config
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
2.2 To load the data we can create a function as below:
```python
# Load customers data from JSON
def load_customers_json():
    """Load customers data from JSON into T_CUSTOMERS_RAW table"""

    table_id = f"{PROJECT_ID}.{DATASET_ID}.T_CUSTOMERS_RAW"

    # Configure the load job
    job_config = bq.LoadJobConfig(
        source_format=bq.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False,  # Don't auto-detect schema, use existing table schema
        write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,  # Replace existing data
        max_bad_records=0  # Fail if any bad records
    )

    # GCS URI for customers JSON file
    uri = f"gs://{BUCKET_NAME}/customers.json"

    print(f"Loading customers data from: {uri}")
    print(f"Target table: {table_id}")

    # Start the load job
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )

    # Wait for job to complete
    load_job.result()

    # Get the updated table info
    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows:,} rows into T_CUSTOMERS_RAW")

    return load_job
```
2.3 let us call this function.
```python
customers_job=load_customers_json()
```

## 3. Load Sales data. (Json) (try it)
create a Notebook and name it 
```python
Load_Data_Sales
```
Just use the above example and try to load the sales data.
3.1 import libraries and config.

3.2 create the function , please note table_id , uri and any refrence to the table T_XXX_RAW needs to be changed.


## 4. Load Weblog data.(txt)
let us now parse the website logs
create a Notebook and name it 
```python
Load_Data_Weblogs
```


4.1 declare the libraries and config
```python
from google.cloud import bigquery
from google.cloud import storage
import re
from datetime import datetime

# Configuration
PROJECT_ID = bigquery.Client().project
DATASET_ID = 'EA_DEMO_RAW'
BUCKET_NAME = 'ea-demo-1raw'
FILE_NAME = 'web_logs.txt'
raw_table = f"{PROJECT_ID}.{DATASET_ID}.T_WEBLOGS_RAW"

# Initialize clients
bq_client = bigquery.Client(project=PROJECT_ID)
storage_client = storage.Client(project=PROJECT_ID)
```
4.2 Read file from bucket 
```python
# Read file from GCS
bucket = storage_client.bucket(BUCKET_NAME)
blob = bucket.blob(FILE_NAME)
log_data = blob.download_as_text()
```
4.3  set up regex config
```python
# Parse logs
pattern = r"\[(.*?)\] (\w+): User '(.*?)' (.*?) book '(.*?)'"
rows = [
    {
        "timestamp": match[0],
        "log_level": match[1],
        "customer_id": match[2],
        "action": match[3],
        "book_id": match[4]
    }
    for match in re.findall(pattern, log_data)
]

```
4.4 load the table 
```python
# Step 1: Overwrite the raw table
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
job = bq_client.load_table_from_json(rows, raw_table, job_config=job_config)
job.result()  # Wait for job to complete

print(f" Inserted {len(rows)} rows into {raw_table}.")
```
