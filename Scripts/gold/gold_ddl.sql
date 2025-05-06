/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/



select * from silver.crm_cust_info
select * from silver.erp_cust_az12
select * from silver .erp_loc_a101


-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
-- customer_key is the 
CREATE VIEW gold.dim_customer
as  
select
ROW_NUMBER() over(order by cst_id) AS customer_key,
c1.cst_id as customer_id, 
c1.cst_key as customer_number,
c1.cst_firstname as first_name,
c1.cst_lastname as last_name,
c3.cntry as country,
c1.cst_material_status as marital_status,
CASE WHEN c1.cst_gndr!='n/a' THEN c1.cst_gndr  --crm is master for Gender
     ELSE COALESCE(c2.gen,'n/a')
END as new_gen,
c1.cst_create_date as create_date,
c2.bdate as birthdate
from silver.crm_cust_info c1
LEFT JOIN silver.erp_cust_az12 c2
ON c1.cst_key=c2.cid
LEFT JOIN silver.erp_loc_a101 c3
ON c1.cst_key=c3.cid
 

 select * from gold.dim_customer;

------------------------------------------------------------------
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
-- create dimension Product
select * from silver.crm_prd_info
select * from silver.erp_px_cat_g1v2


create view gold.dim_products 
as 
select  
ROW_NUMBER() over (order by pn.prd_start_dt ,pn.prd_key) AS product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintainance ,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc 
on pn.cat_id=pc.id
where prd_end_dt is null  -- filter out all historical data
  

select * from gold.dim_products 

------------------------------------------------------

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
--Building Fact 
--Use the dimension Surrogate keys instead of IDs to connect Facts with Dimensions
Create view gold.fact_sales as 
select 
sd.sls_order_number as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date ,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price price
from silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
on sd.sls_prod_key=pr.product_number
LEFT JOIN gold.dim_customer cu
on sd.sls_cust_id=cu.customer_id

select * from silver.crm_sales_details
select * from gold.dim_products 
select * from gold.dim_customer

select * from gold.fact_sales


-------Check if all dimension tables can successfully join to the fact table
--Foreign key Integration
select * from gold.fact_sales f
LEFT JOIN  gold.dim_customer c
on f.customer_key=c.customer_key
where c.customer_key is null
--expected output zero records as all customers from fact table are present in  customer dimension table 

select * from gold.fact_sales f
LEFT JOIN  gold.dim_products p
on f.product_key=p.product_key
where p.product_key is null
--expected output zero records as all the products in fact table are available in product Dimension table 
