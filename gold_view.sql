/*
Create Gold Layer Views
- This Gold layer represents the final dimension and fact tables, using the Star Schema
- Each View is derived from Silver layer tables, and may involve transformations, calculations, and joins to create the final structure suitable for reporting and analysis.
*/

-- Create the dim_customer view in the gold layer
IF OBJECT_ID('gold.dim_customer', 'V') IS NOT NULL
	DROP VIEW gold.dim_customer;

GO

CREATE VIEW gold.dim_customer AS
SELECT
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key, -- Surrogate key
	ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	-- Customer gender information is available in ci.cst_gndr and ca.gen, we are going to assume that the gender information in ci.cst_gndr is the accurate one
	CASE
		WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr -- ci.cst_gndr is the primary source for gender
		ELSE COALESCE(ca.gen, 'n/a') -- If ci.cst_gndr is 'Unknown', then we will use ca.gen as the secondary source
	END AS new_gen,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;

GO


-- Create the dim_products view in the gold layer
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;

GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Only include active products (those without an end date)

GO


-- Create the fact_sales view in the gold layer
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;

GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key, -- Foreign key to dim_products
    cu.customer_key AS customer_key, -- Foreign key to dim_customer
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customer cu
    ON sd.sls_cust_id = cu.customer_id;

GO

SELECT *
FROM gold.fact_sales
