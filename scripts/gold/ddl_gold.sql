/*
=====================================================
DDL Scripts: Create Gold View
=====================================================
Create a view from gold layer  in the data warehouse.
The layer is for dimensions and fact with star schema.
each view adapted from a silver layer with transformation and cleansing process for ready_use business.
=====================================================
This layer can be quired directly for analythics and reporting.
=====================================================
*/
-- IF THERE IS DUPLICATED PRIMARY KEY
SELECT cst_id ,COUNT(*) FROM 
	(SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry		
	FROM [silver].[crm_cust_info] AS ci
	left join [silver].[erp_cust_az12] as ca
	on ca.cid=ci.cst_key
	left join silver.erp_loc_a101 as la
	on la.cid = ci.cst_key) AS T 	 
GROUP BY cst_id
HAVING COUNT(*) > 1

--CHECK THE TWO EQUAL COLUMNS HAVE THE SAME ROWS
SELECT cst_gndr,gen FROM [silver].[crm_cust_info] AS CC
LEFT JOIN SILVER.erp_cust_az12 AS EC
ON CC.cst_key = EC.cid
--MASTER TABLE IS CRM, SO IT INCLUDES ACCURATE DATA
SELECT 
cst_gndr,gen,
CASE WHEN CC.cst_gndr != 'n/a' THEN  CC.cst_gndr
ELSE COALESCE (EC.GEN ,'n/a')
END AS new_gen 
FROM [silver].[crm_cust_info] AS CC
LEFT JOIN SILVER.erp_cust_az12 AS EC
ON CC.cst_key = EC.cid
--=================================================
-- create the dimension of customer  
--=================================================
IF OBJECT_ID('gold.dim_customer', 'V') IS NOT NULL
    DROP VIEW gold.dim_customer;
GO
  
CREATE VIEW gold.dim_customer AS
SELECT
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN  ci.cst_gndr
ELSE COALESCE (ca.GEN ,'n/a')
END AS gender,
	ca.bdate AS birthdate,
	la.cntry AS country,
	ci.cst_create_date AS create_date
	FROM [silver].[crm_cust_info] AS ci
	left join [silver].[erp_cust_az12] as ca
	on ca.cid=ci.cst_key
	left join silver.erp_loc_a101 as la
	on la.cid = ci.cst_key
	--------
SELECT distinct gender FROM gold.dim_customer
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_product AS
SELECT 
row_number() over (order by prd_start_dt,pn.prd_key) as product_key,
pn.prd_id AS product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
FROM [silver].[crm_prd_info] AS pn
LEFT JOIN [silver].[erp_px_cat_g1v2] AS pc
ON pn.cat_id = pc.id
WHERE PN.prd_end_dt IS NULL -- FILTER OUT THE HISTORICAL DATA 
SELECT * FROM [silver].[crm_prd_info]
SELECT * FROM [silver].[erp_px_cat_g1v2]
select * from gold.dim_product
-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS
SELECT 
[sls_ord_num] AS order_number,
pr.product_key,
cr.customer_key ,
[sls_order_dt] AS order_date,
[sls_ship_dt] AS ship_date,
[sls_due_dt] AS due_date,
[sls_sales] AS sales_amount,
[sls_quantity] AS quantity,
[sls_price] AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_product AS pr
on
pr.product_number = sd.[sls_prd_key]
LEFT JOIN
gold.dim_customer AS cr
on cr.customer_id = sd.[sls_cust_id]

select * from [silver].[crm_sales_details]
select * from [gold].[dim_customer]
--------------------------------------------
--CHECK THE QUALITY OF GOLD TABLES
--CHECK THE INTEGRITY OF FORIGNE KEY DIMENSION
SELECT *	
  FROM [DataWareHouse].[gold].[fact_sales] AS S
  LEFT JOIN [gold].[dim_customer] AS C
  ON S.customer_key = C.customer_key
  LEFT JOIN [gold].[dim_product] AS P
  ON P.product_key = S.product_key
WHERE s.customer_key is null
---
