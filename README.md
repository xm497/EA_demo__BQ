<div align="center"><img width="400" height="600" alt="image" src="https://github.com/user-attachments/assets/82b8fdd1-9212-410c-b892-5922dfb93647" /> </div>

# BigQuery Notebooks Tutorial: Demo on Sales Analysis EA Online Book Store

## Overview

This tutorial guides you through building an end-to-end data project using Google Cloud Platform (GCP) BigQuery Notebooks. You'll work with multiple data sources including a CSV, JSON files, and text logs to analyze sales and engagement data for a EA online bookstore.

## Project Goal

By the end of this tutorial, you will:
1. Identify the top **5 best-selling** books by revenue
2. Identify the top **5 customers** by total spending  
3. Analyze web logs to find the top **5 most viewed books**


## Learning Goal

- **CSV**: load data from Csv (Books)
- **JSON Files**: Mimicing Api result: Customer and sales transaction data
- **Text Log File**: Web server logs with page view data

### Data (synthetic)
- 100 books, 500 customers, 2,000 sales, 4746 log entries
- Located in `resources/` folder with realistic business patterns


## Structure

The tutorial is organized into three main sections:

### [Section 1: Setup and Introduction](./01-setup-and-introduction.md)
- GCP Garage Project walkthrough
- Runtime template creation
- BigQuery bucket creation and Copying file GCS Bucket.
- BigQuery dataset and table creation
- BigQuery Notebook creation

### [Section 2: Loading data and Analysis](./02-Data-loading.md)
- Data Loading in BigQuery Notebooks --Books
- Data Loading in BigQuery Notebooks --Customers
- Data Loading in BigQuery Notebooks --sales
- Data Loading in BigQuery Notebooks --Log file parsing 


### [Section 3: Anlysis Results](./03-Data-Analysis.md)
- SQL queries for business insights



