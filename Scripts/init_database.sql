--create Database 'DataWarehouse'

use master;
 
create database Datawarehouse;

use Datawarehouse;
create schema bronze;
go
create schema silver;
go
create schema gold;
go
