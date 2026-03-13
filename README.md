# SQL Data Warehouse and Analytics Project

## Overview

This project demonstrates the design and implementation of a modern **data warehouse and analytics solution** using **SQL Server** and the **Medallion Architecture** approach.

The warehouse is structured into three layers:

- **Bronze Layer**: stores raw data ingested directly from source CSV files into SQL Server
- **Silver Layer**: cleanses, standardizes, and transforms the raw data into a more reliable and analysis-ready format
- **Gold Layer**: delivers business-ready data modeled into a **star schema** using physical dimension and fact tables for reporting and analytics

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
- Materialize the Gold layer into physical tables for easier reporting, indexing, and performance tuning
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

This layer is modeled using a **star schema**, with a central fact table linked to descriptive dimension tables.
Unlike the earlier version of the project where the Gold layer was exposed through views, the Gold layer is now implemented as **physical tables**.

**Purpose:**
- Support analytical queries efficiently
- Provide a clean semantic layer for reporting
- Organize data into reusable business entities
- Allow indexing and constraint-based optimization on reporting tables

**Gold tables in this project:**
- `gold.dim_customers`
- `gold.dim_products`
- `gold.fact_sales`

**Key design characteristics:**
- Surrogate keys are used for dimensions (`customer_key`, `product_key`)
- The fact table stores foreign keys to the dimensions
- The Gold tables are loaded from Silver using the stored procedure `gold.load_gold`
- Primary keys, foreign keys, and supporting indexes are created to improve data integrity and query performance

---

## Data Model

The Gold layer is designed as a **star schema**:

### Dimension Tables

#### `gold.dim_customers`
Contains customer-level descriptive attributes, including:
- `customer_key` as the surrogate key
- Customer ID and customer number
- First name and last name
- Country
- Marital status
- Gender
- Birthdate
- Customer creation date
- Warehouse load timestamp

This table is populated by combining customer information from the Silver CRM and ERP datasets.
A unique index on `customer_id` supports business-key lookup during fact loading.

#### `gold.dim_products`
Contains product-level descriptive attributes, including:
- `product_key` as the surrogate key
- Product ID and product number
- Product name
- Category and subcategory
- Maintenance type
- Cost
- Product line
- Start date
- Warehouse load timestamp

Only active products are loaded into this table.
A unique index on `product_number` supports business-key lookup during fact loading.

### Fact Table

#### `gold.fact_sales`
Contains transactional sales measures, including:
- `sales_key` as the fact table primary key
- Order number
- Product key
- Customer key
- Order date
- Shipping date
- Due date
- Sales amount
- Quantity
- Price
- Warehouse load timestamp

This table links sales transactions to the customer and product dimensions through foreign keys.

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

### Step 3: Build and load Gold tables
The Gold layer is created as physical tables rather than views.
This step includes:
- Creating `gold.dim_customers`, `gold.dim_products`, and `gold.fact_sales`
- Defining surrogate keys for dimensions
- Defining primary key and foreign key relationships
- Creating supporting indexes on business keys
- Loading the Gold tables from the Silver layer through the stored procedure `gold.load_gold`

### Step 4: Refresh Gold data
The stored procedure `gold.load_gold` performs a full reload of the Gold layer by:
- Clearing the fact table
- Resetting and reloading the dimension tables
- Looking up dimension surrogate keys from business keys
- Loading the final fact table with customer and product references

---

## Indexing Strategy

The indexing strategy focuses primarily on the Gold layer, since it is the main analytical layer of the warehouse.

**Current Gold indexing includes:**
- Primary keys on:
  - `gold.dim_customers(customer_key)`
  - `gold.dim_products(product_key)`
  - `gold.fact_sales(sales_key)`
- Unique business-key indexes on:
  - `gold.dim_customers(customer_id)`
  - `gold.dim_products(product_number)`
- Foreign key relationships from `gold.fact_sales` to the dimension tables

This approach supports:
- Surrogate key lookup during ETL
- Better join performance between fact and dimension tables
- Easier future tuning for analytical queries

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

---

## Key Takeaway

The main evolution in this project is that the Gold layer has moved from a **view-based reporting layer** to a **materialized star schema implemented with physical tables**.
This makes the warehouse more realistic for production-style analytics because it allows:
- explicit fact and dimension design
- indexed reporting tables
- constraint-based integrity
- repeatable loading through a stored procedure
- clearer performance tuning for analytical workloads