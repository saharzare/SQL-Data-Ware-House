/*
Scripts Purpose: This Scripts crete a new database as a DataWareHouse, after checking if there already not exists. If it exists it drops and recreats. and has the setups for creating 3 schemas.
Warning: Running this script will drop the entire 'DataWarehouse' database if it exists. make sure there is backup.


*/

-- drop if the datawarehouse already exists
DROP DATABASE DataWarehouse
-- create databases
CREATE DATABASE DataWareHouse;
go

USE DataWareHouse;
go

-- create schema
CREATE SCHEMA bronze;
go
CREATE SCHEMA silver;
go
CREATE SCHEMA gold;
go
