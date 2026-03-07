/*
Stored Procedure: Load Bronze Layer (Source -> Bronze)

This Stored Procedure loads data from external CSV files into bronze tables.
It follows the following actions:
- Truncating the Bronze tables, creating empty tables
- Use `BULK INSERT` to load data from the CSV files to the bronze tables.

*/

CREATE OR ALTER PROCEDURE bronze.LoadBronzeLayer AS
BEGIN

	-- Declare variables to take note of time taken for the entire process
	DECLARE @StartTime DATETIME, @EndTime DATETIME;
	
	-- Capture the start time
	SET @StartTime = GETDATE();
	
	
	
	-- Load data into bronze.crm_cust_info
	TRUNCATE TABLE bronze.crm_cust_info;
	
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Solo Projects\CRM_Data_Warehouse\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
	WITH (
		FORMAT = 'CSV',
		FIRSTROW = 2, -- The row number when the first data row starts (skipping the header)
		FIELDTERMINATOR = ',', -- The column delimiter for CSV files
		ROWTERMINATOR = '\n' -- The row terminator/delimiter for CSV files
	);
	
	
	
	-- Load data into bronze.crm_prd_info
	TRUNCATE TABLE bronze.crm_prd_info;
	
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Solo Projects\CRM_Data_Warehouse\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
	WITH (
		FORMAT = 'CSV',
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n'
	);
	
	
	
	-- Load data into bronze.crm_sales_details
	TRUNCATE TABLE bronze.crm_sales_details;
	
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Solo Projects\CRM_Data_Warehouse\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
	WITH (
		FORMAT = 'CSV',
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n'
	);
	
	
	
	-- Load data into bronze.erp_cust_az12
	TRUNCATE TABLE bronze.erp_cust_az12;
	
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Solo Projects\CRM_Data_Warehouse\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
	WITH (
		FORMAT = 'CSV',
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n'
	);
	
	
	
	-- Load data into bronze.erp_loc_a101
	TRUNCATE TABLE bronze.erp_loc_a101;
	
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Solo Projects\CRM_Data_Warehouse\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
	WITH (
		FORMAT = 'CSV',
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n'
	);
	
	
	
	-- Load data into bronze.erp_px_cat_g1v2
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Solo Projects\CRM_Data_Warehouse\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH (
		FORMAT = 'CSV',
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n'
	);
	
	
	
	-- Capture the end time
	SET @EndTime = GETDATE();
	PRINT 'Time taken to load data into bronze tables: ' + CAST(DATEDIFF(SECOND, @StartTime, @EndTime) AS VARCHAR(1000)) + ' seconds.';

END

EXECUTE bronze.LoadBronzeLayer;