--check for nulls and duplicates in primary keys
--Quality check: primary key must be unique and no nulls
SELECT [cst_id], COUNT(*) AS COUNTINHG FROM [bronze].[crm_cust_info]

GROUP BY [cst_id]
having COUNT(*) > 1
OR
[cst_id] IS NULL
-----------------------------
--remove the duplicates
SELECT *
FROM  [bronze].[crm_cust_info]
WHERE [cst_id] = 29466
--------------------
--assign a unique number to each row
with cte as
(SELECT *, ROW_NUMBER() OVER ( PARTITION BY [cst_id] ORDER BY [cst_create_date] DESC)AS flag_last FROM [bronze].[crm_cust_info])
select * from cte
where  flag_last =1
------------------------------------------
--check for space in the starting of the names
SELECT [cst_firstname] FROM [bronze].[crm_cust_info]
WHERE [cst_firstname] != TRIM([cst_firstname])

SELECT [cst_lastname] FROM [bronze].[crm_cust_info]
WHERE [cst_lastname] != TRIM([cst_lastname])

select [cst_marital_status] from [bronze].[crm_cust_info]
where [cst_marital_status] != trim([cst_marital_status])

select [cst_gndr] from [bronze].[crm_cust_info]
where [cst_gndr] != trim([cst_gndr])
---------------------------------------
--data consitency
--remember that we work with meaningfuly data not acronym
SELECT DISTINCT [cst_gndr] FROM [bronze].[crm_cust_info]
SELECT DISTINCT [cst_marital_status] FROM [bronze].[crm_cust_info];


---------------------------------------
--insert transformed data into silver tables
SELECT 
[cst_id],
[cst_key],
TRIM([cst_firstname]),
TRIM([cst_lastname]),

CASE WHEN UPPER(TRIM([cst_marital_status])) = 'S' THEN 'Single'
WHEN UPPER(TRIM([cst_marital_status])) = 'M' THEN 'Marrid'
ELSE 'n/a'
END [cst_marital_status],

CASE WHEN UPPER(TRIM([cst_gndr])) = 'F' THEN 'Female'
WHEN UPPER(TRIM([cst_gndr])) = 'M' THEN 'Male'
ELSE 'n/a'
END [cst_gndr],

[cst_create_date]
FROM 
( SELECT *,ROW_NUMBER() OVER(PARTITION BY [cst_id] ORDER BY [cst_create_date]DESC) AS flag
FROM [bronze].[crm_cust_info]
WHERE cst_id IS NOT NULL) AS copyT
where flag = 1;
-------------------------------------------------------------------------------
--using CTE is better so:
TRUNCATE TABLE [silver].[crm_cust_info];
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

--INSERT CRM-PRD
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
