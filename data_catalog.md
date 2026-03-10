# Gold Layer Data Catalog

This catalog documents the views in the **gold** schema.  
The gold layer represents the final analytical layer of the warehouse, structured for reporting and analysis using a star schema approach. Views are derived from cleaned and standardised data in the **silver** layer.

---

## 1. `gold.dim_customer`

### Overview
`gold.dim_customer` is the customer dimension view in the gold layer. It provides a cleaned and analysis ready customer master record by combining customer CRM data with ERP customer demographic and location data. It also creates a surrogate key (`customer_key`) for dimensional modelling.

Gender is derived using a fallback rule:
- use CRM gender if it is not `Unknown`
- otherwise use ERP gender
- if both are unavailable, return `n/a`

### Columns

| Column Name | Data Type | Description |
|---|---|---|
| `customer_key` | `BIGINT` | Surrogate key generated using `ROW_NUMBER()` over `cst_id`. |
| `customer_id` | `INT` | Natural customer identifier from `silver.crm_cust_info.cst_id`. |
| `customer_number` | `NVARCHAR(50)` | Customer business key from `silver.crm_cust_info.cst_key`. |
| `first_name` | `NVARCHAR(50)` | Customer first name, trimmed during silver layer cleaning. |
| `last_name` | `NVARCHAR(50)` | Customer last name, trimmed during silver layer cleaning. |
| `country` | `NVARCHAR(50)` | Customer country from ERP location data, with country codes normalised in the silver layer such as `DE` to `Germany`, `US` or `USA` to `United States`, and blanks replaced with `n/a`. |
| `marital_status` | `NVARCHAR(50)` | Standardised marital status from CRM data, such as `Married`, `Single`, or `Unknown`. |
| `new_gen` | `NVARCHAR(50)` | Derived gender field. Uses CRM gender when available and not `Unknown`; otherwise falls back to ERP gender; if both are unavailable returns `n/a`. |
| `birthdate` | `DATE` | Customer birth date from ERP customer data, with invalid dates set to `NULL` in the silver layer. |
| `create_date` | `DATE` | Original customer creation date from CRM source. |

---

## 2. `gold.dim_products`

### Overview
`gold.dim_products` is the product dimension view in the gold layer. It provides a cleaned and enriched product master by combining CRM product data with ERP category data. It also creates a surrogate key (`product_key`) for dimensional modelling.

Only active products are included, defined as products where `prd_end_dt IS NULL`.

### Columns

| Column Name | Data Type | Description |
|---|---|---|
| `product_key` | `BIGINT` | Surrogate key generated using `ROW_NUMBER()` ordered by `prd_start_dt` and `prd_key`. |
| `product_id` | `INT` | Natural product identifier from `silver.crm_prd_info.prd_id`. |
| `product_number` | `NVARCHAR(50)` | Product business key extracted from the source `prd_key` in the silver layer. |
| `product_name` | `NVARCHAR(50)` | Cleaned product name from CRM product data. |
| `category_id` | `NVARCHAR(50)` | Category identifier derived from the source product key in the silver layer. |
| `category` | `NVARCHAR(50)` | Product category from ERP category mapping table. |
| `subcategory` | `NVARCHAR(50)` | Product subcategory from ERP category mapping table. |
| `maintenance` | `NVARCHAR(50)` | Maintenance classification from ERP category mapping table. |
| `cost` | `INT` | Product cost from CRM product data. Null costs are replaced with `0` in the silver layer. |
| `product_line` | `NVARCHAR(50)` | Standardised product line description such as `Mountain`, `Road`, `Other Sales`, or `Touring`. |
| `start_date` | `DATE` | Product start date from CRM product history. |

---

## 3. `gold.fact_sales`

### Overview
`gold.fact_sales` is the sales fact view in the gold layer. It captures transactional sales data and links each record to the customer and product dimensions using surrogate keys.

The fact is built from cleaned sales data in `silver.crm_sales_details`, joined to `gold.dim_products` using product number and to `gold.dim_customer` using customer ID.

### Columns

| Column Name | Data Type | Description |
|---|---|---|
| `order_number` | `NVARCHAR(50)` | Sales order number from `silver.crm_sales_details.sls_ord_num`. |
| `product_key` | `BIGINT` | Foreign key to `gold.dim_products.product_key`. |
| `customer_key` | `BIGINT` | Foreign key to `gold.dim_customer.customer_key`. |
| `order_date` | `DATE` | Sales order date. Invalid source dates are converted to `NULL` in the silver layer. |
| `shipping_date` | `DATE` | Shipping date. Invalid source dates are converted to `NULL` in the silver layer. |
| `due_date` | `DATE` | Due date for the order. Invalid source dates are converted to `NULL` in the silver layer. |
| `sales_amount` | `INT` | Total sales value for the transaction. Recalculated in the silver layer when missing, non positive, or inconsistent with `quantity × abs(price)`. |
| `quantity` | `INT` | Quantity sold in the transaction. |
| `price` | `INT` | Unit price for the transaction. Recalculated in the silver layer when missing or non positive. |

---

## Notes on Data Type Logic

- For columns directly selected from silver tables, the data type comes from the silver table definitions.
- For surrogate keys created using `ROW_NUMBER()`, the data type is shown as `BIGINT`.
- For derived columns such as `new_gen`, the resulting type follows the character type used by the contributing source columns, so `NVARCHAR(50)` is appropriate.