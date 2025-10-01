<div align="center"><img width="400" height="600" alt="image" src="https://github.com/user-attachments/assets/82b8fdd1-9212-410c-b892-5922dfb93647" /> </div>

# BigQuery Notebooks Tutorial: Demo on Sales Analysis EA Online Book Store

## Overview

This comprehensive tutorial guides you through building an end-to-end data analysis project using Google Cloud Platform (GCP) BigQuery Notebooks. You'll work with multiple data sources including a CSV, JSON files, and text logs to analyze sales and engagement data for a EA online bookstore.

## Project Goal

By the end of this tutorial, you will:
1. Identify the top 5 best-selling books by revenue
2. Identify the top 5 customers by total spending  
3. Analyze web logs to find the top 5 most viewed books

## Data Sources

- **CSV**: Google Books API for book metadata
- **JSON Files**: Customer and sales transaction data
- **Text Log File**: Web server logs with page view data

### Generated Data (Production-Scale)**
- 100 books, 500 customers, 2,000 sales, 5,000 log entries
- Located in `resources/` folder with realistic business patterns


## Structure

The tutorial is organized into three main sections:

### [Section 1: Setup and Introduction](./01-setup-and-introduction.md)
- GCP Garage Project walkthrough
- Runtime template creation
- BigQuery bucket creation and Copying file GCS Bucket.
- BigQuery dataset and table creation
- BigQuery Notebook creation

### [Section 2: Building Analysis Logic](./02-analysis-logic.md)
- Data processing in BigQuery Notebooks --Books
- Data processing in BigQuery Notebooks --Customers
- Data processing in BigQuery Notebooks --sales
- Log file parsing and table creation
- SQL queries for business insights


### [Section 3: Results and Visualization](./03-results-and-visualization.md)
- Result presentation and visualization
- Summary of accomplishments
- Next steps and recommendations


