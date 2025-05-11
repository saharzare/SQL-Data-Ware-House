/*
===================================================================================================================
DDL Scripts: Creaye the Bronze tables
===================================================================================================================

Purpose:
This script is used to create and maintain the crm_cust_info table in the bronze schema of the database.
It includes functionality to check if the table exists and drops it before recreating it.
===================================================================================================================
*/
--create customer table
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);

--create product
if OBJECT_ID('bronze.crm_prd_info','U') is not null
drop table bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),prd_cost nvarchar(50),prd_line NVARCHAR(50),prd_start_dt DATE,prd_end_dt date
);

--create sales details
if OBJECT_ID('bronze.crm_sales_details','U') is not null
drop table bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key NVARCHAR(50),
sls_cust_id int,sls_order_dt date,sls_ship_dt date,sls_sales int,sls_quantity int,sls_price int
);

--create erb customer
if OBJECT_ID('bronze.erp_cust_az12','U') is not null
drop table bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12(
cid nvarchar(50),bdate date,gen nvarchar(50));

--create erp location
if OBJECT_ID('bronze.erp_loc_a101','U') is not null
drop table bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101(
cid nvarchar(50),cntry nvarchar(50));

--create 
if OBJECT_ID('bronze.erp_px_cat_g1v2','U') is not null
drop table bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2(
id nvarchar(50),cat nvarchar(50),subcat nvarchar(50),maintenance nvarchar(50));

