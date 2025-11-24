CREATE VIEW gold.dim_customers AS
SELECT 
ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
CASE 
	WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr 
	ELSE ca.gen
END AS gender,
la.cntry,
ci.cst_marital_status AS marital_status,
ca.bdate AS birth_date,
ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid;

CREATE VIEW gold.dim_products AS 
SELECT 
ROW_NUMBER() OVER (ORDER BY pin.prd_start_dt, pin.prd_key) AS product_key,
pin.prd_id AS product_id,
pin.prd_key AS product_number,
pin.prd_nm AS product_name,
pin.cat_id AS category_id,
ca.cat AS category,
ca.subcate AS subcategory,
ca.maintenance ,
pin.prd_cost AS cost,
pin.prd_line AS product_line,
pin.prd_start_dt AS start_date
FROM silver.crm_prd_info pin
LEFT JOIN silver.erp_px_cat_g1v2 ca
ON pin.cat_id = ca.id
WHERE pin.prd_end_dt IS NULL;

CREATE VIEW gold.dim_sales AS 
SELECT 
sd.sls_ord_num AS order_number,
dp.product_key,
dc.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS ship_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products dp
ON sd.sls_prd_key = dp.product_number
LEFT JOIN gold.dim_customers dc
ON sd.sls_cust_id = dc.customer_id;



