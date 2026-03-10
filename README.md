# SQL Data Warehouse and Analytics Project

## Overview

This project demonstrates the design and implementation of a modern **data warehouse and analytics solution** using **SQL Server** and the **Medallion Architecture** approach.

The warehouse is structured into three layers:

- **Bronze Layer**: stores raw data ingested directly from source CSV files into SQL Server
- **Silver Layer**: cleanses, standardizes, and transforms the raw data into a more reliable and analysis-ready format
- **Gold Layer**: delivers business-ready data modeled into a **star schema** for reporting and analytics

The project also includes **SQL-based analytical reporting** to generate insights into:

- Customer behavior
- Product performance
- Sales trends

---

## Project Objectives

The main goals of this project are to:

- Build a structured data warehouse using Medallion Architecture
- Develop ETL pipelines to move data across Bronze, Silver, and Gold layers
- Apply data cleansing and transformation logic to improve data quality
- Model analytical datasets using fact and dimension tables
- Produce SQL-based reports for business analysis and decision-making

---

## Architecture

### 1. Bronze Layer
The Bronze layer stores raw source data in its original form with minimal or no transformation.

**Purpose:**
- Preserve the original source data
- Support traceability and reproducibility
- Serve as the ingestion layer for downstream processing

**Source data includes:**
- CRM customer information
- CRM product information
- CRM sales details
- ERP customer data
- ERP location data
- ERP product category data

**Example Bronze tables:**
- `bronze.crm_cust_info`
- `bronze.crm_prd_info`
- `bronze.crm_sales_details`
- `bronze.erp_loc_a101`
- `bronze.erp_cust_az12`
- `bronze.erp_px_cat_g1v2`

---

### 2. Silver Layer
The Silver layer applies cleansing, standardization, normalization, and transformation logic to improve data usability and consistency.

**Purpose:**
- Fix data quality issues
- Standardize formats and values
- Prepare integrated datasets for dimensional modeling
- Add metadata for warehouse processing

**Typical transformations include:**
- Converting raw date fields into proper SQL date types
- Standardizing gender, marital status, and other categorical values
- Cleaning product and customer attributes
- Preparing product category mappings
- Adding warehouse metadata such as load timestamps

**Example Silver tables:**
- `silver.crm_cust_info`
- `silver.crm_prd_info`
- `silver.crm_sales_details`
- `silver.erp_loc_a101`
- `silver.erp_cust_az12`
- `silver.erp_px_cat_g1v2`

---

### 3. Gold Layer
The Gold layer contains business-ready analytical models designed for reporting and dashboarding.

This layer is modeled using a **star schema**, with central fact tables linked to descriptive dimension tables.

**Purpose:**
- Support analytical queries efficiently
- Provide a clean semantic layer for reporting
- Organize data into reusable business entities

**Gold objects in this project:**
- `gold.dim_customers`
- `gold.dim_products`
- `gold.fact_sales`

---

## Data Model

The Gold layer is designed as a **star schema**:

### Dimension Tables

#### `gold.dim_customers`
Contains customer-level descriptive attributes, including:
- Customer ID and number
- First name and last name
- Country
- Marital status
- Gender
- Birthdate
- Customer creation date

#### `gold.dim_products`
Contains product-level descriptive attributes, including:
- Product ID and number
- Product name
- Category and subcategory
- Maintenance type
- Cost
- Product line
- Start date

### Fact Table

#### `gold.fact_sales`
Contains transactional sales measures, including:
- Order number
- Product key
- Customer key
- Order date
- Shipping date
- Due date
- Sales amount
- Quantity
- Price

---

## ETL Workflow

The project follows a layered ETL process:

### Step 1: Load raw data into Bronze
Raw CSV data is loaded into Bronze tables without major transformation.

### Step 2: Transform Bronze data into Silver
Data is cleaned and standardized in Silver tables. This includes:
- Type conversions
- Value standardization
- Normalization of product and customer fields
- Data quality checks
- Metadata enrichment

### Step 3: Build Gold views for analytics
Gold views are created from Silver tables by:
- Joining customer, product, and category data
- Creating surrogate keys
- Filtering for active products
- Structuring the final star schema for reporting

---

## Analytics and Reporting

This project develops SQL-based reports to generate business insights in three main areas:

### Customer Behavior
Examples of analysis include:
- Customer segmentation based on spending and purchase history
- Distribution of customers by country
- Customer lifecycle analysis

A sample segmentation logic used in this project:
- **VIP**: customers with at least 12 months of history and spending greater than \$5,000
- **Regular**: customers with at least 12 months of history and spending of \$5,000 or less
- **New**: customers with less than 12 months of history

### Product Performance
Examples of analysis include:
- Sales contribution by product category
- Performance comparison across product lines
- Category-level sales drivers

### Sales Trends
Examples of analysis include:
- Overall sales volume and revenue tracking
- Sales distribution by geography
- Trend reporting by order activity and business dimensions

---

## Example Business Questions Answered

This warehouse supports analytical queries such as:

- Which product categories contribute the most to total sales?
- Which countries generate the highest share of revenue?
- How many customers fall into VIP, Regular, and New segments?
- Which products are actively sold in the business?
- How can customer and product dimensions be combined with fact sales for reporting?
