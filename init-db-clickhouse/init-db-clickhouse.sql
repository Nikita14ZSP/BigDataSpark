CREATE DATABASE IF NOT EXISTS reports_db;

USE reports_db;

DROP TABLE IF EXISTS product_sales_summary;
CREATE TABLE product_sales_summary
(
    product_id          Int32,      
    product_name        String,     
    product_category    String,     
    total_revenue       Float64,    
    total_quantity_sold UInt64,     
    number_of_sales     UInt64      
)
ENGINE = MergeTree()
ORDER BY (product_id, product_name)
SETTINGS index_granularity = 8192;

DROP TABLE IF EXISTS category_revenue_summary;
CREATE TABLE category_revenue_summary
(
    product_category    String,     
    total_revenue       Float64     
)
ENGINE = MergeTree()
ORDER BY (product_category)
SETTINGS index_granularity = 8192;

DROP TABLE IF EXISTS product_quality_summary;
CREATE TABLE product_quality_summary
(
    product_id          Int32,      
    product_name        String,     
    average_rating      Float32,    
    reviews_count       UInt64      
)
ENGINE = MergeTree()
ORDER BY (product_id, product_name)
SETTINGS index_granularity = 8192;



DROP TABLE IF EXISTS customer_purchase_summary;
CREATE TABLE customer_purchase_summary
(
    customer_id             Int32,      
    customer_full_name      String,     
    total_purchase_amount   Float64,    
    number_of_purchases     UInt64      
)
ENGINE = MergeTree()
ORDER BY (customer_id, customer_full_name)
SETTINGS index_granularity = 8192;

DROP TABLE IF EXISTS customer_distribution_by_country;
CREATE TABLE customer_distribution_by_country
(
    customer_country    String,     
    customer_count      UInt64      
)
ENGINE = MergeTree()
ORDER BY (customer_country)
SETTINGS index_granularity = 8192;


DROP TABLE IF EXISTS monthly_sales_trends;
CREATE TABLE monthly_sales_trends
(
    sale_year           UInt16,     
    sale_month          UInt8,      
    total_revenue       Float64,    
    total_orders        UInt64,     
    average_order_value Float64    
)
ENGINE = MergeTree()
ORDER BY (sale_year, sale_month)
SETTINGS index_granularity = 8192;

DROP TABLE IF EXISTS yearly_sales_trends;
CREATE TABLE yearly_sales_trends
(
    sale_year           UInt16,     
    total_revenue       Float64,    
    total_orders        UInt64      
)
ENGINE = MergeTree()
ORDER BY (sale_year)
SETTINGS index_granularity = 8192;


DROP TABLE IF EXISTS store_sales_summary;
CREATE TABLE store_sales_summary
(
    store_email         String,     
    store_name          String,     
    total_revenue       Float64,    
    total_orders        UInt64,     
    average_order_value Float64     
)
ENGINE = MergeTree()
ORDER BY (store_email, store_name)
SETTINGS index_granularity = 8192;

DROP TABLE IF EXISTS store_sales_distribution;
CREATE TABLE store_sales_distribution
(
    store_city          String,     
    store_country       String,     
    total_revenue       Float64,    
    total_orders        UInt64      
)
ENGINE = MergeTree()
ORDER BY (store_country, store_city)
SETTINGS index_granularity = 8192;


DROP TABLE IF EXISTS supplier_revenue_summary;
CREATE TABLE supplier_revenue_summary
(
    supplier_name       String,     
    total_revenue_from_supplier_products Float64 
)
ENGINE = MergeTree()
ORDER BY (supplier_name)
SETTINGS index_granularity = 8192;

DROP TABLE IF EXISTS supplier_product_avg_price;
CREATE TABLE supplier_product_avg_price
(
    supplier_name       String,     
    average_product_price Float64   
)
ENGINE = MergeTree()
ORDER BY (supplier_name)
SETTINGS index_granularity = 8192;

DROP TABLE IF EXISTS supplier_sales_distribution_by_country;
CREATE TABLE supplier_sales_distribution_by_country
(
    supplier_country    String,     
    total_revenue       Float64,    
    total_orders        UInt64      
)
ENGINE = MergeTree()
ORDER BY (supplier_country)
SETTINGS index_granularity = 8192;


DROP TABLE IF EXISTS product_rating_sales_correlation;
CREATE TABLE product_rating_sales_correlation 
(
    product_id          Int32,
    product_name        String,
    average_rating      Float32,
    total_quantity_sold UInt64,
    total_revenue       Float64
    
)
ENGINE = MergeTree()
ORDER BY (product_id)
SETTINGS index_granularity = 8192;
