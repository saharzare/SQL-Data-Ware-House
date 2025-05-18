/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

EXEC silver.load_silver
CREATE OR ALTER PROCEDURE silver.load_silver AS

BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
--using CTE is better so:
PRINT' >> Truncating table: [silver].[crm_cust_info] '
TRUNCATE TABLE [silver].[crm_cust_info];
PRINT ' >> Inserting data into: [silver].[crm_cust_info]';
WITH CTE AS (
    SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY [cst_id] ORDER BY [cst_create_date] DESC) AS flag 
    FROM [bronze].[crm_cust_info] 
    WHERE [cst_id] IS NOT NULL
)
INSERT INTO [silver].[crm_cust_info] (
    [cst_id],
    [cst_key],
    [cst_firstname],
    [cst_lastname],
    [cst_marital_status],
    [cst_gndr],
    [cst_create_date]
)
SELECT 
    [cst_id],
    [cst_key],
    TRIM([cst_firstname]) AS cst_firstname,
    TRIM([cst_lastname]) AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM([cst_marital_status])) = 'S' THEN 'Single'
        WHEN UPPER(TRIM([cst_marital_status])) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS [cst_marital_status],
    CASE 
        WHEN UPPER(TRIM([cst_gndr])) = 'F' THEN 'Female'
        WHEN UPPER(TRIM([cst_gndr])) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS [cst_gndr],
    [cst_create_date]
FROM CTE
WHERE flag = 1;

------------------------------------
--CHECK THE DATA CONSISTENCY OF SILVER AFTER INSERTING-customer table
SELECT * FROM [silver].[crm_cust_info]
WHERE TRIM([cst_firstname]) != [cst_firstname]

SELECT DISTINCT [cst_gndr] FROM  [silver].[crm_cust_info]

--------------------------------------------
SELECT * FROM [silver].[crm_cust_info]
SELECT * FROM [bronze].[crm_prd_info]
SELECT * FROM [bronze].[crm_sales_details]
-----------------------------------------
--check for spaces for silver tables before inserting-prod
select [prd_nm] from [bronze].[crm_prd_info]
where trim([prd_nm]) != [prd_nm]
--check for nulls & numbers
select [prd_cost] from [bronze].[crm_prd_info]
where [prd_cost] < 0 or  [prd_cost] is null
SELECT DISTINCT [prd_line] FROM [bronze].[crm_prd_info]
SELECT [prd_line] FROM [bronze].[crm_prd_info]
WHERE TRIM([prd_line]) != [prd_line]
--end date smaller than start date
SELECT * FROM [bronze].[crm_prd_info]
WHERE [prd_end_dt]> [prd_start_dt]

------------------------------------------

SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		
print' >> Truncating table: silver.crm_prd_info '
TRUNCATE TABLE  silver.crm_prd_info 
--INSERT CRM-PRD
print' >> Inserting table: silver.crm_prd_info '
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
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Extract product key
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, -- Map product line codes to descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
				AS DATE
			) AS prd_end_dt -- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info;
--WHERE SUBSTRING(prd_key,7,len(prd_key)) NOT IN (
--SELECT [sls_prd_key] FROM [bronze].[crm_sales_details])
----------------------------
print' >> check data quality'
--CHECK THE QUALITY OF SILVER DATA-CRM-PRO
SELECT [prd_id],COUNT(*) AS COUNTING FROM [silver].[crm_prd_info]
GROUP BY [prd_id]
HAVING COUNT(*) > 1
OR [prd_id] IS NULL
-----------------------------
--CHECK FOR SPACE
SELECT [prd_nm] FROM [silver].[crm_prd_info]
WHERE TRIM([prd_nm]) != [prd_nm]
--CHECK FOR NEGATIVE OR NULLS
SELECT prd_cost FROM [silver].[crm_prd_info]
WHERE prd_cost < 0 OR prd_cost IS NULL
--DATA CONSISTENCY AND STANDADIZATION
SELECT DISTINCT [prd_line] FROM [silver].[crm_prd_info]
--CHECK FOR DATE ORDER INVALID
SELECT * FROM[silver].[crm_prd_info]
WHERE [prd_end_dt] <[prd_start_dt]
--------------------------------------------------
SELECT * FROM [bronze].[crm_sales_details]
--FIRST CHECK THE INTEGRITY OF CUSTOMER & PRODUCT IN SALES
SELECT [sls_cust_id] FROM [bronze].[crm_sales_details]
SELECT * FROM [bronze].[crm_sales_details] WHERE  [sls_cust_id] NOT IN (SELECT [cst_id] FROM [bronze].[crm_cust_info])
SELECT * FROM [bronze].[crm_sales_details] WHERE [sls_prd_key] NOT IN (SELECT [prd_key] FROM [bronze].[crm_prd_info])
---------------
--CHECK FOR INVALID DATE
-- NEGATIVE AND ZERO DATE CAN NOT CAST TO DATE
SELECT [sls_ORDER_dt] FROM [bronze].[crm_sales_details]
WHERE [sls_ORDER_dt] < = 0
----------------------------------
--CAST DATE TO NULL FROM ZERO(remember dates can not be negative or zero or less than 8 for lenght, make thr zeros to null)
SELECT NULLIF([sls_ORDER_dt],0) AS [sls_due_dt] FROM [bronze].[crm_sales_details] 
WHERE [sls_ORDER_dt] <= 0 OR LEN(sls_ORDER_dt) != 8
SELECT NULLIF([sls_due_dt],0) AS [sls_due_dt] FROM [bronze].[crm_sales_details] 
--CHECK FOR OUTLIERS BY VALIDATING THE BOUNDRIES OF THE DATE RANGE
SELECT NULLIF([sls_order_dt],0) AS [sls_order_dt] FROM [bronze].[crm_sales_details]
WHERE [sls_order_dt]> 20500101 OR [sls_order_dt] <19000101
OR [sls_order_dt] <= 0
OR LEN([sls_order_dt]) != 8
--ORDER DATE SHOULD BE SMALLER THAN DUE AND SHIPING DATE
SELECT *  FROM [bronze].[crm_sales_details]
WHERE [sls_order_dt] > [sls_ship_dt]  OR [sls_order_dt] > [sls_due_dt]
-------------------------------------
-- LAST 3 COLUMNS ARE CONNECTED EACH OTHER
--BUESINESS RULE :SUM(SALES) = QUANTITY * PRICE & NOU NULL, NOT NEGATIVE AND ZERO
SELECT DISTINCT sls_sales,[sls_price],  sls_quantity FROM [bronze].[crm_sales_details]
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY [sls_price], sls_sales, sls_quantity DESC
-----------------------------------------
--create sales details
if OBJECT_ID('silver.crm_sales_details','U') is not null
drop table silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key NVARCHAR(50),
sls_cust_id int,sls_order_dt DATE,sls_ship_dt DATE,sls_due_dt DATE , sls_sales int,sls_quantity int,sls_price int,
dwh_creat_date datetime2 default getdate()

);
-----------------------------------------------
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		
print'Truncating table: [silver].[crm_sales_details]'
TRUNCATE TABLE [silver].[crm_sales_details]
--INSERTING INTO THE SILVER SALES TABLE
print'Inserting table: [silver].[crm_sales_details]'

INSERT INTO [silver].[crm_sales_details]
([sls_ord_num],[sls_prd_key],[sls_cust_id],[sls_order_dt],[sls_ship_dt],[sls_due_dt],[sls_sales],[sls_quantity],[sls_price])
SELECT [sls_ord_num],
[sls_prd_key],
[sls_cust_id],
CASE WHEN [sls_order_dt] = 0 OR LEN([sls_order_dt]) != 8 THEN NULL
ELSE CAST(CAST([sls_order_dt] AS NVARCHAR) AS DATE)
END AS [sls_order_dt],
CASE WHEN [sls_ship_dt] <=0 OR LEN([sls_ship_dt]) != 8 THEN NULL
ELSE CAST(CAST([sls_ship_dt] AS VARCHAR) AS DATE)
END AS [sls_ship_dt],
CASE WHEN [sls_due_dt] = 0 OR LEN([sls_due_dt]) != 8 THEN NULL
ELSE CAST(CAST([sls_due_dt] AS VARCHAR) AS DATE)
END sls_due_dt,
CASE WHEN [sls_sales] IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity *ABS(sls_price)
THEN  sls_quantity *ABS(sls_price)
ELSE [sls_sales]
END [sls_sales],
[sls_quantity], --OK
CASE WHEN [sls_price] IS NULL OR [sls_price] < 0 
THEN sls_sales / NULLIF(sls_quantity,0)
ELSE sls_price
END [sls_price]
FROM [bronze].[crm_sales_details]

-------------------
--CHECK THE QUALITY & VALIDITY OF DATA IN SILVER

SELECT * FROM silver.[crm_sales_details]
--CHECK THE INVALID DATE
SELECT * FROM [silver].[crm_sales_details]
WHERE [sls_order_dt] >   [sls_ship_dt]
OR [sls_order_dt] > [sls_due_dt]
SELECT * FROM [silver].[crm_sales_details]
WHERE sls_price IS NULL OR sls_price < 0
OR sls_sales IS NULL OR sls_sales < 0
OR sls_sales != sls_price * sls_quantity
-----------------------------------------
--create erb customer
if OBJECT_ID('silver.erp_cust_az12','U') is not null
drop table silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12(
cid nvarchar(50),bdate date,gen nvarchar(50),dwh_creat_date datetime2 default getdate()
);
---------------------------
SELECT * FROM [bronze].[erp_cust_az12]
SELECT * FROM [bronze].[crm_cust_info]
---------------
--CHECK THE RELATION OF CRM & ERP CUSTOMER TABLE
SELECT * FROM  [bronze].[crm_cust_info]
WHERE [cst_id] IN (SELECT cid FROM [bronze].[erp_cust_az12])
--OR
SELECT * FROM [bronze].[erp_cust_az12]
WHERE CID LIKE 'NAS%'
--
--DATA CHECK QUALITY FOR GENDER
SELECT * FROM [bronze].[erp_cust_az12]
WHERE TRIM([gen]) != gen

SELECT DISTINCT gen,
CASE WHEN UPPER(TRIM([gen] )) IN ('F','Female') THEN 'Female'
WHEN UPPER(TRIM([gen] )) IN ('M','Male') THEN 'Male'
ELSE 'n/a'
END gen
FROM [bronze].[erp_cust_az12]
----------------
--CHECK OUTLIER DATE
SELECT * FROM [bronze].[erp_cust_az12]
WHERE [bdate] < '1924-01-01' OR bdate > GETDATE()
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		
		SET @start_time = GETDATE();
---------------
print'Truncating table:  [silver].[erp_cust_az12]'
TRUNCATE TABLE  [silver].[erp_cust_az12]
print'Inserting table:  [silver].[erp_cust_az12]'

--INSERT DATA TO SILVER TABLE OF CUSTOMER
INSERT INTO [silver].[erp_cust_az12]( cid,bdate,gen)
SELECT
CASE WHEN [cid] LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
ELSE CID
END cid,
CASE WHEN [bdate] > GETDATE() THEN NULL
ELSE bdate 
END bdate ,
CASE WHEN UPPER(TRIM([gen] )) IN ('F','Female') THEN 'Female'
WHEN UPPER(TRIM([gen] )) IN ('M','Male') THEN 'Male'
ELSE 'n/a'
END gen
FROM [bronze].[erp_cust_az12]
---------------------
--CHECK THE QUALITY OF SILVER TABLE
--OUT OF RANGE OF BDATE
SELECT * FROM [silver].[erp_cust_az12]
WHERE bdate > GETDATE()
--DATA CONSISTENCY OF GENDER
SELECT DISTINCT gen FROM [silver].[erp_cust_az12]
---------------------------------
SELECT * FROM [bronze].[erp_loc_a101]
SELECT * FROM  [bronze].[crm_cust_info]
-- DATA CHECK QUALITY
SELECT DISTINCT cntry FROM [bronze].[erp_loc_a101]
SELECT * FROM [bronze].[erp_loc_a101]
WHERE cid IN (SELECT CST_KEY FROM [silver].[crm_cust_info])

------
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
print'Truncating table :[silver].[erp_loc_a101]'
TRUNCATE TABLE [silver].[erp_loc_a101]
--INSERT DATA TO SILVER
print'Inserting table :[silver].[erp_loc_a101]'

INSERT INTO [silver].[erp_loc_a101]
([cid],[cntry])
SELECT REPLACE([cid],'-','') AS cid,
CASE WHEN UPPER(TRIM([cntry])) IN ('USA','US') THEN 'United States'--HANDILNG MISSING VALUES AND NORMALIZATION
WHEN [cntry] IN ('DE','Germany') THEN 'Germany'
WHEN  [cntry] = '' OR [cntry] IS NULL THEN 'n/a'
ELSE UPPER(TRIM([cntry]))
END [cntry]
FROM [bronze].[erp_loc_a101]
------------------------------------
SELECT * FROM [bronze].[erp_px_cat_g1v2]
--CHECK QUALITY FOR UNWANTED SPACES
SELECT  * FROM [bronze].[erp_px_cat_g1v2]
WHERE TRIM(subcat) != subcat
OR
TRIM(SUBCAT) != SUBCAT
OR
TRIM(MAINTENANCE) != maintenance
----------------------
--DATA CONSISTENCY
SELECT DISTINCT cat FROM [bronze].[erp_px_cat_g1v2]
SELECT DISTINCT subcat FROM [bronze].[erp_px_cat_g1v2]
SELECT DISTINCT maintenance FROM [bronze].[erp_px_cat_g1v2]
----------------------
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
print'Truncating table :[silver].[erp_px_cat_g1v2]'
TRUNCATE TABLE [silver].[erp_px_cat_g1v2]
print'Inserting table :[silver].[erp_px_cat_g1v2]'
INSERT INTO [silver].[erp_px_cat_g1v2]([id],[cat],[subcat],[maintenance])
SELECT [id],[cat],[subcat],[maintenance]
FROM bronze.[erp_px_cat_g1v2]

SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH

END
