# Section 2: Building Analysis Logic in BigQuery Notebook

Welcome to the analytical heart of our tutorial! In this section, we'll process unstructured log data, create new BigQuery tables programmatically, and run SQL queries to extract business insights.

## Prerequisites

Before starting this section, ensure you have:
- Completed Section 1 setup
- Your BigQuery Notebook open and ready
- All data files uploaded to Cloud Storage
- BigQuery tables (`books`, `customers`, `sales`) created successfully

## Step 1: Initialize Notebook & Import Libraries

Start by running this cell in your BigQuery Notebook to import necessary libraries and initialize the BigQuery client:

```python
# Import required libraries
import google.cloud.bigquery as bq
import pandas as pd
import re
from datetime import datetime
from google.cloud import storage
import io

# Initialize BigQuery client
client = bq.Client()
print("BigQuery client initialized successfully!")

# Set your project ID and dataset
PROJECT_ID = client.project  # This gets your current project
DATASET_ID = 'bookstore_analysis'
BUCKET_NAME = f'bookstore-analysis-{PROJECT_ID}'  # Adjust if your bucket name is different

print(f"Working with project: {PROJECT_ID}")
print(f"Dataset: {DATASET_ID}")
print(f"Bucket: {BUCKET_NAME}")
```

## Step 2: Process Log File from Cloud Storage

### 2.1 Read Log File from GCS

```python
# Initialize Cloud Storage client
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

# Read the web logs file from GCS
blob = bucket.blob('web_logs.txt')
log_content = blob.download_as_text()

print("Log file content preview:")
print(log_content[:500] + "..." if len(log_content) > 500 else log_content)
```

### 2.2 Create Log Parsing Function

```python
def parse_log_line(log_line):
    """
    Parse a single log line to extract timestamp, customer_id, and book_id.
    
    Expected format: [YYYY-MM-DD HH:MM:SS] INFO: User 'CUST_ID' viewed details for book 'BOOK_ID'
    
    Returns:
        dict: Parsed data or None if parsing fails
    """
    # Regular expression pattern to match the log format
    pattern = r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] INFO: User \'(\w+)\' viewed details for book \'(\w+)\''
    
    match = re.match(pattern, log_line.strip())
    
    if match:
        timestamp_str, customer_id, book_id = match.groups()
        
        # Convert timestamp string to datetime object
        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        
        return {
            'timestamp': timestamp,
            'customer_id': customer_id,
            'book_id': book_id
        }
    else:
        print(f"Failed to parse line: {log_line.strip()}")
        return None

# Test the parsing function with a sample line
test_line = "[2024-01-15 09:23:45] INFO: User 'CUST_001' viewed details for book 'BOOK_001'"
parsed_test = parse_log_line(test_line)
print("Test parsing result:", parsed_test)
```

### 2.3 Process All Log Lines

```python
# Process all log lines
log_lines = log_content.strip().split('\n')
page_views_data = []

print(f"Processing {len(log_lines)} log lines...")

for i, line in enumerate(log_lines):
    parsed_data = parse_log_line(line)
    if parsed_data:
        page_views_data.append(parsed_data)
    else:
        print(f"Skipped line {i+1}: Could not parse")

# Create DataFrame from parsed data
page_views_df = pd.DataFrame(page_views_data)

print(f"Successfully parsed {len(page_views_df)} page views")
print("\nPage views DataFrame preview:")
print(page_views_df.head(10))

# Display summary statistics
print(f"\nSummary:")
print(f"Total page views: {len(page_views_df)}")
print(f"Unique customers: {page_views_df['customer_id'].nunique()}")
print(f"Unique books viewed: {page_views_df['book_id'].nunique()}")
print(f"Date range: {page_views_df['timestamp'].min()} to {page_views_df['timestamp'].max()}")
```

## Step 3: Create BigQuery Table from Processed Data

### 3.1 Define Table Schema

```python
# Define the schema for our page_views table
from google.cloud.bigquery import SchemaField

page_views_schema = [
    SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    SchemaField("customer_id", "STRING", mode="REQUIRED"),
    SchemaField("book_id", "STRING", mode="REQUIRED"),
]

print("Table schema defined:")
for field in page_views_schema:
    print(f"  - {field.name}: {field.field_type} ({field.mode})")
```

### 3.2 Create Table and Load Data

```python
# Create table reference
dataset_ref = client.dataset(DATASET_ID)
table_ref = dataset_ref.table('page_views')

# Create table with schema
table = bq.Table(table_ref, schema=page_views_schema)

# Delete table if it exists (for rerunning the notebook)
try:
    client.delete_table(table_ref)
    print("Existing table deleted")
except:
    print("No existing table found")

# Create new table
table = client.create_table(table)
print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

# Load DataFrame into BigQuery table
job_config = bq.LoadJobConfig(
    schema=page_views_schema,
    write_disposition="WRITE_TRUNCATE"  # Overwrite existing data
)

job = client.load_table_from_dataframe(page_views_df, table_ref, job_config=job_config)
job.result()  # Wait for the job to complete

print(f"Successfully loaded {job.output_rows} rows into page_views table")

# Verify the table was created and populated
query = f"SELECT COUNT(*) as row_count FROM `{PROJECT_ID}.{DATASET_ID}.page_views`"
result = client.query(query).result()
for row in result:
    print(f"Verified: page_views table contains {row.row_count} rows")
```

## Step 4: Analytical Queries

Now let's run our three main analytical queries to extract business insights.

### 4.1 Query 1: Find Top 5 Best-Selling Books by Revenue

```python
# Query to find top 5 best-selling books by revenue
query_top_books = f"""
SELECT 
    b.title,
    b.authors,
    SUM(s.quantity * b.price) as total_revenue,
    SUM(s.quantity) as total_units_sold,
    ROUND(AVG(b.price), 2) as avg_price
FROM `{PROJECT_ID}.{DATASET_ID}.sales` s
JOIN `{PROJECT_ID}.{DATASET_ID}.books` b ON s.book_id = b.book_id
GROUP BY b.book_id, b.title, b.authors
ORDER BY total_revenue DESC
LIMIT 5
"""

print("Query 1: Top 5 Best-Selling Books by Revenue")
print("=" * 50)

# Execute query and load results into DataFrame
top_books_df = client.query(query_top_books).to_dataframe()
print(top_books_df.to_string(index=False))

# Store for later visualization
print(f"\nFound {len(top_books_df)} top-selling books")
```

### 4.2 Query 2: Find Top 5 Customers by Total Spending

```python
# Query to find top 5 customers by total spending
query_top_customers = f"""
SELECT 
    c.customer_name,
    c.join_date,
    COUNT(DISTINCT s.sale_id) as total_orders,
    SUM(s.quantity) as total_books_purchased,
    SUM(s.quantity * b.price) as total_spending
FROM `{PROJECT_ID}.{DATASET_ID}.sales` s
JOIN `{PROJECT_ID}.{DATASET_ID}.customers` c ON s.customer_id = c.customer_id
JOIN `{PROJECT_ID}.{DATASET_ID}.books` b ON s.book_id = b.book_id
GROUP BY c.customer_id, c.customer_name, c.join_date
ORDER BY total_spending DESC
LIMIT 5
"""

print("\n\nQuery 2: Top 5 Customers by Total Spending")
print("=" * 50)

# Execute query and load results into DataFrame
top_customers_df = client.query(query_top_customers).to_dataframe()
print(top_customers_df.to_string(index=False))

print(f"\nFound {len(top_customers_df)} top-spending customers")
```

### 4.3 Query 3: Find Top 5 Most Viewed Books

```python
# Query to find top 5 most viewed books from page views
query_top_viewed = f"""
SELECT 
    b.title,
    b.authors,
    COUNT(*) as total_views,
    COUNT(DISTINCT pv.customer_id) as unique_viewers,
    ROUND(AVG(b.price), 2) as book_price
FROM `{PROJECT_ID}.{DATASET_ID}.page_views` pv
JOIN `{PROJECT_ID}.{DATASET_ID}.books` b ON pv.book_id = b.book_id
GROUP BY b.book_id, b.title, b.authors, b.price
ORDER BY total_views DESC
LIMIT 5
"""

print("\n\nQuery 3: Top 5 Most Viewed Books")
print("=" * 50)

# Execute query and load results into DataFrame
top_viewed_df = client.query(query_top_viewed).to_dataframe()
print(top_viewed_df.to_string(index=False))

print(f"\nFound {len(top_viewed_df)} most-viewed books")
```

## Step 5: Additional Analysis - Correlation Between Views and Sales

Let's add one more interesting analysis to see if there's a correlation between page views and actual sales:

```python
# Bonus Query: Compare views vs sales for all books
query_views_vs_sales = f"""
WITH book_views AS (
    SELECT 
        b.book_id,
        b.title,
        COUNT(*) as total_views
    FROM `{PROJECT_ID}.{DATASET_ID}.page_views` pv
    JOIN `{PROJECT_ID}.{DATASET_ID}.books` b ON pv.book_id = b.book_id
    GROUP BY b.book_id, b.title
),
book_sales AS (
    SELECT 
        b.book_id,
        b.title,
        COALESCE(SUM(s.quantity), 0) as total_sales,
        COALESCE(SUM(s.quantity * b.price), 0) as total_revenue
    FROM `{PROJECT_ID}.{DATASET_ID}.books` b
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.sales` s ON b.book_id = s.book_id
    GROUP BY b.book_id, b.title
)
SELECT 
    bv.title,
    bv.total_views,
    bs.total_sales,
    bs.total_revenue,
    ROUND(SAFE_DIVIDE(bs.total_sales, bv.total_views) * 100, 2) as conversion_rate_percent
FROM book_views bv
JOIN book_sales bs ON bv.book_id = bs.book_id
ORDER BY conversion_rate_percent DESC
"""

print("\n\nBonus Analysis: Views vs Sales Conversion")
print("=" * 50)

views_vs_sales_df = client.query(query_views_vs_sales).to_dataframe()
print(views_vs_sales_df.to_string(index=False))

print(f"\nAnalyzed conversion rates for {len(views_vs_sales_df)} books")
```

## Step 6: Summary of Processed Data

Let's create a summary of what we've accomplished:

```python
# Summary statistics
print("\n" + "="*60)
print("DATA PROCESSING SUMMARY")
print("="*60)

# Original data summary
customers_count = client.query(f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{DATASET_ID}.customers`").to_dataframe()
books_count = client.query(f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{DATASET_ID}.books`").to_dataframe()
sales_count = client.query(f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{DATASET_ID}.sales`").to_dataframe()
views_count = client.query(f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{DATASET_ID}.page_views`").to_dataframe()

print(f"📊 Data Sources Processed:")
print(f"   • Customers: {customers_count.iloc[0]['count']} records")
print(f"   • Books: {books_count.iloc[0]['count']} records (from Google Books API)")
print(f"   • Sales transactions: {sales_count.iloc[0]['count']} records")
print(f"   • Page views: {views_count.iloc[0]['count']} records (parsed from logs)")

print(f"\n🎯 Key Findings:")
print(f"   • Top revenue book: {top_books_df.iloc[0]['title']} (${top_books_df.iloc[0]['total_revenue']:.2f})")
print(f"   • Top customer: {top_customers_df.iloc[0]['customer_name']} (${top_customers_df.iloc[0]['total_spending']:.2f})")
print(f"   • Most viewed book: {top_viewed_df.iloc[0]['title']} ({top_viewed_df.iloc[0]['total_views']} views)")

print(f"\n✅ Tables Created:")
print(f"   • {DATASET_ID}.customers (from JSON)")
print(f"   • {DATASET_ID}.books (from API → JSON)")
print(f"   • {DATASET_ID}.sales (from JSON)")
print(f"   • {DATASET_ID}.page_views (from processed log file)")
```

## What We've Accomplished

In this section, you have:

✅ **Processed unstructured data**: Parsed web server logs using regular expressions  
✅ **Created tables programmatically**: Used pandas and BigQuery client to create new tables  
✅ **Performed complex analytics**: Joined multiple tables to extract business insights  
✅ **Handled different data sources**: API data, structured JSON, and unstructured text logs  
✅ **Generated actionable insights**: Identified top books, customers, and engagement patterns  

The three DataFrames you now have (`top_books_df`, `top_customers_df`, `top_viewed_df`) contain the core insights for your fictional bookstore business.

**Next**: In Section 3, we'll visualize these results and create compelling charts to present our findings.

**Continue to**: [Section 3: Results and Visualization](./03-results-and-visualization.md)
