/*
Stored Procedure: silver.load_silver
This stored procedure performs data cleaning and transformation on the raw data in the bronze
schema and loads the cleaned data into the silver schema.
Apply various cleaning rules:
- trimming whitespace,
- standardizing categorical values,
- handling missing or invalid data, and
- ensuring referential integrity.
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

	-- Declare variables to track the start and end time of the data loading process
	DECLARE @start_time DATETIME, @end_time DATETIME;

	SET @start_time = GETDATE(); -- Capture the start time of the data loading process



	-- Perform data cleaning on bronze.crm_cust_info and load the cleaned data into silver.crm_cust_info
	TRUNCATE TABLE silver.crm_cust_info; -- Clear existing data in the silver table before loading new data
	
	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date,
		dwh_create_Date
	)
	SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname), -- Remove leading and trailing spaces from cst_firstname
		TRIM(cst_lastname), -- Remove leading and trailing spaces from cst_lastname
		CASE
			WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
			WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
			ELSE 'Unknown'
		END AS cst_marital_status,
		CASE
			WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
			WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
			ELSE 'Unknown'
		END AS cst_gndr,
		cst_create_date,
		GETDATE() AS dwh_create_Date
	FROM (
		-- Remove duplicate records, keeping the latest record based on cst_create_date
		SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
	) AS t
	WHERE rn = 1; -- Select only the latest record for each cst_id



	-- Perform data cleaning on bronze.crm_prd_info and load the cleaned data into silver.crm_prd_info
	TRUNCATE TABLE silver.crm_prd_info;
	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract the category ID
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Extract the product key
		TRIM(prd_nm) AS prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE
			WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		END AS prd_line, -- Map product lin codes to descriptive names
		CAST(prd_start_dt AS DATE) AS prd_start_dt, -- Convert from datetime to date
		LEAD(prd_start_dt, 1) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC) - 1 AS prd_end_dt -- Set prd_end_dt to one day before the next prd_start_dt for the same prd_key
	FROM bronze.crm_prd_info;



	-- Perform data cleaning on bronze.crm_sales_details and load the cleaned data into silver.crm_sales_details
	TRUNCATE TABLE silver.crm_sales_details;
	INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE
			WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE)
		END AS sls_order_dt, -- Convert from int to date, handling invalid formats
		CASE
			WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE)
		END AS sls_ship_dt, -- Convert from int to date, handling invalid formats
		CASE
			WHEN sls_due_dt = 0 OR LEN(sls_due_dt ) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE)
		END AS sls_due_dt, -- Convert from int to date, handling invalid formats
		CASE
			WHEN (sls_sales != sls_quantity * ABS(sls_price)) OR sls_sales <= 0 OR sls_sales IS NULL
			THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales, -- Recalculate sls_sales if it doesn't match sls_quantity * sls_price or if it's non-positive
		sls_quantity,
		CASE
			WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity, 0) -- Avoid division by zero
			ELSE sls_price
		END AS sls_price -- Recalculate sls_price if it's non-positive or NULL
	FROM bronze.crm_sales_details;



	-- Perform data cleaning on bronze.erp_cust_az12 and load the cleaned data into silver.erp_cust_az12
	TRUNCATE TABLE silver.erp_cust_az12
	INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
	)
	SELECT
		CASE
		    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
		END AS cid, -- Remove 'NAS' prefix from cid if it exists
		CASE
			WHEN bdate < '1900-01-01' OR bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate, -- Set bdate to NULL if it's outside a reasonable range
		CASE
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			ELSE NULL
		END AS gen -- Map gen codes to descriptive names, setting to NULL if it doesn't match expected values
	FROM bronze.erp_cust_az12



	-- Perform data cleaning on bronze.erp_loc_a101 and load the cleaned data into silver.erp_loc_a101
	TRUNCATE TABLE silver.erp_loc_a101
	INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
	)
	SELECT
		REPLACE(cid, '-', '') AS cid,
		CASE
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry -- Normalize and Handle missing or blank country codes
	FROM bronze.erp_loc_a101



	-- Perform data cleaning on bronze.erp_px_cat_g1v2 and load the cleaned data into silver.erp_px_cat_g1v2
	TRUNCATE TABLE silver.erp_px_cat_g1v2
	INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2



	-- Compute the duration of the data cleaning and loading
	SET @end_time = GETDATE(); -- Capture the end time of the data loading process
	PRINT 'Data loading completed in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(1000)) + ' seconds.'

END

EXEC silver.load_silver;


SELECT *
FROM silver.erp_px_cat_g1v2;

SELECT *
FROM silver.crm_prd_info