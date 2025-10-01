# Section 2: Building Analysis Logic in BigQuery Notebook

In this section, we'll run SQL queries to extract business insights.

## Prerequisites

Before starting this section, ensure you have:
- Completed Section 1,2 
- Your BigQuery Notebook open and ready
- BigQuery tables (`t_books_raw`, `t_customers_raw`, `t_sales_raw`,`t_weblogs_raw`) created successfully


## Step 1: Analytical Queries

Now let's run our three main analytical queries to extract business insights.
create a notebook and let us name it as 
```SQL
Load_Aggregate
```

### 4.1 Query 1: Find Top 5 Best-Selling Books by Revenue

```python
%%bigquery Q1
SELECT
    b.title AS Title,
    b.authors as Authors,
    SUM(SAFE_CAST(s.quantity AS INT64) * b.price) as Total_revenue,
    SUM(SAFE_CAST(s.quantity AS INT64)) as Total_units_sold,
    ROUND(AVG(b.price ), 2) as Avg_price
FROM `your project`.EA_DEMO.T_SALES_RAW s
JOIN `your project`.EA_DEMO.T_BOOKS_RAW b ON s.book_id = b.book_id
GROUP BY b.book_id, b.title, b.authors
ORDER BY total_revenue DESC
LIMIT 5
```

### 4.2 Query 2: Find Top 5 Customers by Total Spending

```python
%%bigquery Q2
SELECT
    c.customer_name as Name,
    c.join_date as Join_date,
    COUNT(DISTINCT s.sale_id) as Total_orders,
    SUM(SAFE_CAST(s.quantity AS INT64)) as Total_books_purchased,
    SUM(SAFE_CAST(s.quantity AS INT64)* b.price) as Total_spending
FROM `your project`.EA_DEMO.T_SALES_RAW s
JOIN `your project`.EA_DEMO.T_CUSTOMERS_RAW c ON s.customer_id = c.customer_id
JOIN `your project`.EA_DEMO.T_BOOKS_RAW b ON s.book_id = b.book_id
GROUP BY c.customer_id, c.customer_name, c.join_date
ORDER BY total_spending DESC
LIMIT 5
```

### 4.3 Query 3: Find Top 5 Most Viewed Books

```python
%%bigquery Q3
SELECT
    b.title As Title,
    b.authors As Authors,
    COUNT(*) as Total_views,
    COUNT(DISTINCT pv.customer_id) as Unique_viewers,
    ROUND(AVG(b.price), 2) as Book_price
FROM `your project.EA_DEMO.T_WEBLOGS_RAW` pv
JOIN `your project.EA_DEMO.T_BOOKS_RAW` b ON pv.book_id = b.book_id
GROUP BY b.book_id, b.title, b.authors, b.price
ORDER BY total_views DESC
LIMIT 5
```
### 4.3 Query 3: Find Top 5 Most Viewed Books
```python
 from google.cloud import bigquery

# Initialize client
client = bigquery.Client()

# Define dataset
dataset_id = 'EA_DEMO_ACCESS'
project_id = client.project

# Define full table names
table_bseller = f"{project_id}.{dataset_id}.T_T5_BSELLER"
table_cust = f"{project_id}.{dataset_id}.T_T5_CUST"
table_view = f"{project_id}.{dataset_id}.T_T5_VIEW"

# Load Q1 into T_T5_BSELLER
job1 = client.load_table_from_dataframe(Q1, table_bseller, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
job1.result()

# Load Q2 into T_T5_CUST
job2 = client.load_table_from_dataframe(Q2, table_cust, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
job2.result()

# Load Q3 into T_T5_VIEW
job3 = client.load_table_from_dataframe(Q3, table_view, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
job3.result()

print("All queries loaded into their respective tables.")
```

## What We've Accomplished

In this section, we have:
✅ **Handled different data sources in bigQuery**: API data, structured JSON, and unstructured text logs  
✅ **Processed unstructured data**: Parsed web server logs using regular expressions  
✅ **Created tables via gcloud**: Used BigQuery client gcloud command to create new tables  
✅ **Performed analytics**: Joined multiple tables to extract business insights  
✅ **Generated actionable insights**: Identified top books, customers, and engagement patterns  



