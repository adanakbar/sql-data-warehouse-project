/*
=============================================================
Database and Schema Setup
=============================================================
Purpose:
    This script checks for an existing 'DataWarehouse' database, removes it if found,
    and creates a fresh version. It also prepares three schemas within the database:
    bronze, silver, and gold.

Note:
    Executing this script will permanently delete the current 'DataWarehouse' database,
    along with all its data. Make sure any important information is backed up before use.
*/


USE master;
GO 

-- Drop and recreate the database 'DataWarehouse' if it exists.
IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database.
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO 

-- Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO


