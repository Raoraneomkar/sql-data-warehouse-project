
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


--STORE PROCEDURE FOR LOADING ALL THE TABLES INI SILVER LAYER
EXECUTE bronze.load_bronze
EXECUTE silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
 DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading sILVER Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';
    SET @start_time = GETDATE();
	PRINT'>>Truncate Table : silver.crm_cust_info'
	Truncate Table silver.crm_cust_info
	PRINT'>>Inserting Data into silver.crm_cust_info'
	insert into 
	silver.crm_cust_info(
	[cst_id]
		  ,[cst_key]
		  ,[cst_firstname]
		  ,[cst_lastname]
		  ,[cst_material_status]
		  ,[cst_gndr]
		  ,[cst_create_date])
	 SELECT 
	 cst_id ,
	 cst_key ,
	 trim(cst_firstname) as cst_firstname,
	 trim(cst_lastname) as cst_lastname, 
	 case when UPPER(TRIM(cst_material_status)) = 'S' then 'Single'
		  when UPPER(TRIM(cst_material_status)) = 'M' then 'Married'
		  else 'n/a'
	 end as cst_material_status
	 , 
	 case when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
		 when UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
		 else 'n/a'
	 end as cst_gndr ,
	cst_create_date 
	FROM   (
	select * , ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_last
	from bronze.crm_cust_info
	where cst_id is not null          
	) t
	where t.flag_last=1;

	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


	---loading the silver product table silver.crm_prd_info 
	SET @start_time = GETDATE();
	PRINT'>>Truncate Table : silver.crm_prd_info'
	Truncate Table silver.crm_prd_info
	PRINT'>>Inserting Data into silver.crm_prd_info'

	insert into silver.crm_prd_info (
		 prd_id  ,
		 cat_id  ,
		 prd_key ,
		 prd_nm  ,
		 prd_cost ,
		 prd_line  ,
		 prd_start_dt ,
		 prd_end_dt   
		 )
	select
		   prd_id  
		  ,REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id
		  ,SUBSTRING(prd_key,7,len(prd_key)) as prd_key
		  ,prd_nm 
		  ,isnull(prd_cost, 0) as prod_cost   
		  ,CASE UPPER(TRIM(prd_line)) 
				WHEN 'M' THEN 'Mountain'	 
				WHEN 'R' THEN 'Raod'	
				WHEN 'S' THEN 'Other Sales'	
				WHEN 'T' THEN 'Touring'	
				Else 'n/a'
		   END as prd_line		   
		  ,cast(prd_start_dt as DATE) AS prd_start_dt
		  ,CAST(LEAD(prd_start_dt) over(Partition by prd_key order by prd_start_dt)-1 AS DATE)as prd_end_dt 
	  FROM [Datawarehouse].[bronze].[crm_prd_info] ;

	    SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

	---loading the silver product table silver.crm_sales_details 

	SET @start_time = GETDATE();
	PRINT'>>Truncate Table : silver.crm_sales_details'
	Truncate Table silver.crm_sales_details
	PRINT'>>Inserting Data into silver.crm_sales_details'

	Insert into 
	silver.crm_sales_details
	(
	sls_order_number ,
	sls_prod_key     ,
	sls_cust_id      ,
	sls_order_dt     ,
	sls_ship_dt      ,
	sls_due_dt       ,
	sls_sales        ,
	sls_quantity     ,
	sls_price  
	)
	select  
	sls_order_number ,
	sls_prod_key     ,
	sls_cust_id      ,
	Case  
		 WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 OR sls_order_dt > 20500101 OR sls_order_dt<19000101 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt ,
	Case  
		 WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 OR sls_ship_dt > 20500101 OR sls_ship_dt<19000101 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	Case  
		 WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 OR sls_due_dt > 20500101 OR sls_due_dt<19000101 THEN NULL
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE WHEN sls_sales is null or sls_sales<=0 or sls_sales != sls_quantity*ABS(sls_price)
		  THEN sls_quantity * ABS(sls_price)
		  ELSE sls_sales
	END as sls_sales,
	sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <=0 
		 THEN  ABS(sls_sales)/nullif(sls_quantity,0)
		 ELSE sls_price
	END as sls_price 
	from bronze.crm_sales_details ;
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


	----- Insert into Silver lAYER  silver.erp_cust_az12
	PRINT '------------------------------------------------';
	PRINT 'Loading ERP Tables';
	PRINT '------------------------------------------------';

	SET @start_time = GETDATE();
	PRINT'>>Truncate Table : silver.erp_cust_az12'
	Truncate Table silver.erp_cust_az12
	PRINT'>>Inserting Data into silver.erp_cust_az12'

	Insert into silver.erp_cust_az12 (cid,bdate,gen)
	SELECT  
	CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid,4, LEN(cid))
		 ELSE cid
	END as cid,
	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate 
	END AS bdate,
	CASE WHEN  UPPER(TRIM(gen)) IN ('F' ,'FEMALE') THEN 'Female'                            
		 WHEN UPPER(TRIM(gen)) IN ('M' ,'MALE') THEN 'Male'                         
		 ELSE 'n/a'                                             
	END AS gen 
	FROM [Datawarehouse].[bronze].[erp_cust_az12];

	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';



	 --INSERT INTO SILVER TABLE [silver].[erp_loc_a101]

    SET @start_time = GETDATE();
	PRINT'>>Truncate Table : silver.erp_loc_a101'
	Truncate Table silver.erp_loc_a101
	PRINT'>>Inserting Data into silver.erp_loc_a101'

	   Insert into [silver].[erp_loc_a101]
	   (cid,cntry)
	   select 
	   replace(cid,'-','')as cid,
	   CASE WHEN UPPER(TRIM(cntry)) IN ('DE','GERMANY') THEN 'Germany'
			WHEN UPPER(TRIM(cntry)) IN ('US','USA','AMERICA','UNITED STATES') THEN 'United States'
			WHEN UPPER(TRIM(cntry)) IN ('UK','GREAT BRITAIN','UNITED KINGDOM') THEN 'United Kingdom'
			WHEN cntry IS NULL OR cntry=''  THEN 'N/A'
			ELSE cntry
		END AS cntry
	   FROM [Datawarehouse].[bronze].[erp_loc_a101];

	   SET @end_time = GETDATE();
	   PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	   PRINT '>> -------------';


	----INSERT INTO SILVER TABLE [silver].[erp_px_cat_g1v2]
	SET @start_time = GETDATE();
	PRINT'>>Truncate Table : silver.erp_px_cat_g1v2'
	Truncate Table silver.erp_px_cat_g1v2
	PRINT'>>Inserting Data into silver.erp_px_cat_g1v2'
	INSERT INTO [Datawarehouse].[silver].[erp_px_cat_g1v2]
	  ( [id]
	   ,[cat]
	   ,[subcat]
	   ,[maintainance]
	   )
	 select 
	   id
	   ,cat
	   ,subcat
	   ,maintainance
	 from [Datawarehouse].[bronze].[erp_px_cat_glv2];
	 SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

	SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
   END TRY
 	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
 END
