# Section 2: Building Analysis Logic in BigQuery Notebook

Welcome to the analytical heart of our tutorial! In this section, we'll process unstructured log data, create new BigQuery tables programmatically, and run SQL queries to extract business insights.

## Prerequisites

Before starting this section, ensure you have:
- Completed Section 1 setup
- Your BigQuery Notebook open and ready
- All data files uploaded to Cloud Storage
- BigQuery tables (`t_books_raw`, `t_customers_raw`, `t_sales_raw`,`t_weblogs_raw`) created successfully


## Step 1: Analytical Queries

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
