/*
Create the fact_sales, dim_customers, and dim_products tables in the gold layer,
and a stored procedure to load data into these tables from the silver layer.
*/

-- Create the fact_sales table in the gold layer
IF OBJECT_ID('gold.fact_sales', 'U') IS NOT NULL
    DROP TABLE gold.fact_sales;

GO

CREATE TABLE gold.fact_sales (
    sales_key        INT IDENTITY(1,1) PRIMARY KEY,
    order_number     NVARCHAR(50) NOT NULL,
    product_key      INT, -- Foreign key to dim_products
    customer_key     INT, -- Foreign key to dim_customers
    order_date       DATE,
    shipping_date    DATE,
    due_date         DATE,
    sales_amount     INT,
    quantity         INT,
    price            INT,
    dwh_create_date  DATETIME2 DEFAULT GETDATE()
);

GO

-- Create the dim_customers table in the gold layer
IF OBJECT_ID('gold.dim_customers', 'U') IS NOT NULL
	DROP TABLE gold.dim_customers;

GO

CREATE TABLE gold.dim_customers (
    customer_key     INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate key
    customer_id      INT NOT NULL,
    customer_number  NVARCHAR(50),
    first_name       NVARCHAR(50),
    last_name        NVARCHAR(50),
    country          NVARCHAR(50),
    marital_status   NVARCHAR(50),
    gender           NVARCHAR(50),
    birthdate        DATE,
    create_date      DATE,
    dwh_create_date  DATETIME2 DEFAULT GETDATE()
);

CREATE UNIQUE INDEX IX_dim_customers_customer_id
ON gold.dim_customers(customer_id);

GO


-- Create the dim_products table in the gold layer
IF OBJECT_ID('gold.dim_products', 'U') IS NOT NULL
	DROP TABLE gold.dim_products;

GO

CREATE TABLE gold.dim_products (
    product_key      INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate key
    product_id       INT NOT NULL,
    product_number   NVARCHAR(50) NOT NULL,
    product_name     NVARCHAR(50),
    category_id      NVARCHAR(50),
    category         NVARCHAR(50),
    subcategory      NVARCHAR(50),
    maintenance      NVARCHAR(50),
    cost             INT,
    product_line     NVARCHAR(50),
    start_date       DATE,
    dwh_create_date  DATETIME2 DEFAULT GETDATE()
);

CREATE UNIQUE INDEX IX_dim_products_product_number
ON gold.dim_products(product_number);

GO



-- Add foreign key constraints to the fact_sales table
ALTER TABLE gold.fact_sales
ADD CONSTRAINT FK_fact_sales_product
FOREIGN KEY (product_key) REFERENCES gold.dim_products(product_key);
GO

ALTER TABLE gold.fact_sales
ADD CONSTRAINT FK_fact_sales_customer
FOREIGN KEY (customer_key) REFERENCES gold.dim_customers(customer_key);
GO



-- Create a stored procedure to load data into the gold layer tables
CREATE OR ALTER PROCEDURE gold.load_gold AS
BEGIN
    SET NOCOUNT ON; -- Removes "rows affected" messages

    TRUNCATE TABLE gold.fact_sales;

    -- Fully clear dim_customers, then reset the IDENTITY counter so that
    -- the next inserted row starts again from 1 during a full Gold reload
    DELETE FROM gold.dim_customers;
    DBCC CHECKIDENT ('gold.dim_customers', RESEED, 1);

    -- Fully clear dim_products, then reset the IDENTITY counter so that
    -- the next inserted row starts again from 1 during a full Gold reload
    DELETE FROM gold.dim_products;
    DBCC CHECKIDENT ('gold.dim_products', RESEED, 1);

    -- Load data into the dim_customers table
    INSERT INTO gold.dim_customers (
        customer_id,
        customer_number,
        first_name,
        last_name,
        country,
        marital_status,
        gender,
        birthdate,
        create_date,
        dwh_create_date
    )
    SELECT
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
    	END AS gender,
        ca.bdate AS birthdate,
        ci.cst_create_date AS create_date,
        GETDATE() AS dwh_create_date
    FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca
        ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 la
        ON ci.cst_key = la.cid;

    -- Load data into the dim_products table
    INSERT INTO gold.dim_products (
        product_id,
        product_number,
        product_name,
        category_id,
        category,
        subcategory,
        maintenance,
        cost,
        product_line,
        start_date,
        dwh_create_date
    )
    SELECT
        pn.prd_id       AS product_id,
        pn.prd_key      AS product_number,
        pn.prd_nm       AS product_name,
        pn.cat_id       AS category_id,
        pc.cat          AS category,
        pc.subcat       AS subcategory,
        pc.maintenance  AS maintenance,
        pn.prd_cost     AS cost,
        pn.prd_line     AS product_line,
        pn.prd_start_dt AS start_date,
        GETDATE()       AS dwh_create_date
    FROM silver.crm_prd_info pn
    LEFT JOIN silver.erp_px_cat_g1v2 pc
        ON pn.cat_id = pc.id
    WHERE pn.prd_end_dt IS NULL; -- Only include active products (those without an end date)

    -- Load data into the fact_sales table
    INSERT INTO gold.fact_sales (
        order_number,
        product_key,
        customer_key,
        order_date,
        shipping_date,
        due_date,
        sales_amount,
        quantity,
        price,
        dwh_create_date
    )
    SELECT
        sd.sls_ord_num  AS order_number,
        pr.product_key  AS product_key,
        cu.customer_key AS customer_key,
        sd.sls_order_dt AS order_date,
        sd.sls_ship_dt  AS shipping_date,
        sd.sls_due_dt   AS due_date,
        sd.sls_sales    AS sales_amount,
        sd.sls_quantity AS quantity,
        sd.sls_price    AS price,
        GETDATE()       AS dwh_create_date
    FROM silver.crm_sales_details sd
    LEFT JOIN gold.dim_products pr
        ON sd.sls_prd_key = pr.product_number
    LEFT JOIN gold.dim_customers cu
        ON sd.sls_cust_id = cu.customer_id;
END;
GO

-- Execute the stored procedure to load data into the gold layer tables
EXEC gold.load_gold;

SELECT TOP 10 * FROM gold.dim_customers;
SELECT TOP 10 * FROM gold.dim_products;
SELECT TOP 10 * FROM gold.fact_sales;