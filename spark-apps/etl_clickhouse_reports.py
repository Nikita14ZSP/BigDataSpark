# /spark-apps/etl_clickhouse_reports.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, desc, year, month, rank, dense_rank, round as spark_round,
    when, lit, corr
)
from pyspark.sql.window import Window

def get_spark_session(app_name="ClickHouseReportsETL"):
    postgres_jar_path = "/opt/spark-jars/postgresql-42.6.0.jar"
    clickhouse_jar_path = "/opt/spark-jars/clickhouse-jdbc-0.4.6.jar" 

    spark = SparkSession.builder \
        .appName(app_name) \
        .master("spark://spark-master:7077") \
        .config("spark.driver.extraClassPath", f"{postgres_jar_path},{clickhouse_jar_path}") \
        .config("spark.executor.extraClassPath", f"{postgres_jar_path},{clickhouse_jar_path}") \
        .config("spark.jars", f"{postgres_jar_path},{clickhouse_jar_path}") \
        .getOrCreate()
    return spark

def write_to_clickhouse(df, table_name, ch_url, ch_properties, database_name="lab_reports"):
    # ch_table_name = f"{database_name}.{table_name}" 
    ch_table_name = table_name

    print(f"Writing to ClickHouse table: {ch_table_name}...")
    try:
        
        df.write \
          .format("jdbc") \
          .option("url", ch_url) \
          .option("dbtable", ch_table_name) \
          .option("user", ch_properties.get("user")) \
          .option("password", ch_properties.get("password")) \
          .option("driver", ch_properties.get("driver")) \
          .mode("overwrite") \
          .save()

        print(f"Successfully wrote to {ch_table_name}. Count: {df.count()}")
    except Exception as e:
        print(f"Error writing {ch_table_name} to ClickHouse: {e}")


def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    pg_url = "jdbc:postgresql://postgres_db:5432/lab_db"
    pg_properties = {"user": "lab_user", "password": "lab_password", "driver": "org.postgresql.Driver"}

    # Используем native порт 9000 для ClickHouse, если используем официальный clickhouse-jdbc
    ch_url = "jdbc:clickhouse://clickhouse_server:9000/lab_reports" 
    ch_properties = {
        "user": "default", 
        "password": "",    
        "driver": "com.clickhouse.jdbc.ClickHouseDriver"
    }
    

    print("Reading data from PostgreSQL Star Schema...")
    try:
        fact_sales_df = spark.read.jdbc(url=pg_url, table="FactSales", properties=pg_properties)
        dim_product_df = spark.read.jdbc(url=pg_url, table="DimProduct", properties=pg_properties)
        dim_customer_df = spark.read.jdbc(url=pg_url, table="DimCustomer", properties=pg_properties)
        dim_date_df = spark.read.jdbc(url=pg_url, table="DimDate", properties=pg_properties)
        dim_store_df = spark.read.jdbc(url=pg_url, table="DimStore", properties=pg_properties)
        dim_supplier_df = spark.read.jdbc(url=pg_url, table="DimSupplier", properties=pg_properties)
        dim_product_category_df = spark.read.jdbc(url=pg_url, table="DimProductCategory", properties=pg_properties)
        dim_country_df = spark.read.jdbc(url=pg_url, table="DimCountry", properties=pg_properties) # Для названий стран
    except Exception as e:
        print(f"Error reading from PostgreSQL Star Schema: {e}")
        spark.stop()
        return

    fact_sales_df.cache()
    dim_product_df.cache()

    print("Generating Product Sales Reports...")
    product_sales_base_df = fact_sales_df \
        .join(dim_product_df, "product_key", "inner") \
        .join(dim_product_category_df, dim_product_df.category_key == dim_product_category_df.category_key, "left") \
        .select(
            fact_sales_df.sale_total_price,
            fact_sales_df.sale_quantity,
            dim_product_df.product_name,
            dim_product_df.rating.alias("product_rating"),
            dim_product_df.reviews.alias("product_reviews"),
            dim_product_category_df.category_name.alias("product_category_name")
         )
    product_sales_base_df.cache()

    report_top_10_revenue_products = product_sales_base_df \
        .groupBy("product_name") \
        .agg(
            sum("sale_total_price").alias("total_revenue"),
            sum("sale_quantity").alias("total_quantity_sold")
        ) \
        .orderBy(desc("total_revenue")) \
        .limit(10)
    write_to_clickhouse(report_top_10_revenue_products, "report_top_10_revenue_products", ch_url, ch_properties)

    report_revenue_by_category = product_sales_base_df \
        .groupBy("product_category_name") \
        .agg(sum("sale_total_price").alias("total_revenue")) \
        .orderBy(desc("total_revenue"))
    write_to_clickhouse(report_revenue_by_category, "report_revenue_by_category", ch_url, ch_properties)

    report_product_ratings_reviews = product_sales_base_df \
        .groupBy("product_name") \
        .agg(
            avg("product_rating").alias("average_rating"),
            sum("product_reviews").alias("total_reviews") # или max
        ) \
        .orderBy(desc("average_rating"), desc("total_reviews"))
    write_to_clickhouse(report_product_ratings_reviews, "report_product_ratings_reviews", ch_url, ch_properties)
    
    product_sales_base_df.unpersist() 

    print("Generating Customer Sales Reports...")
    customer_sales_base_df = fact_sales_df \
        .join(dim_customer_df, "customer_key", "inner") \
        .join(dim_country_df, dim_customer_df.country_key == dim_country_df.country_key, "left") \
        .select(
            fact_sales_df.sale_total_price,
            dim_customer_df.customer_email, 
            dim_country_df.country_name.alias("customer_country_name")
        )
    customer_sales_base_df.cache()

    report_top_10_customers_by_purchase = customer_sales_base_df \
        .groupBy("customer_email") \
        .agg(sum("sale_total_price").alias("total_purchase_amount")) \
        .orderBy(desc("total_purchase_amount")) \
        .limit(10)
    write_to_clickhouse(report_top_10_customers_by_purchase, "report_top_10_customers_by_purchase", ch_url, ch_properties)

    report_customer_distribution_by_country = customer_sales_base_df \
        .groupBy("customer_country_name") \
        .agg(count("*").alias("customer_count")) \
        .orderBy(desc("customer_count"))
    write_to_clickhouse(report_customer_distribution_by_country, "report_customer_distribution_by_country", ch_url, ch_properties)

    report_avg_check_per_customer = customer_sales_base_df \
        .groupBy("customer_email") \
        .agg(avg("sale_total_price").alias("average_check_amount")) \
        .orderBy(desc("average_check_amount"))
    write_to_clickhouse(report_avg_check_per_customer, "report_avg_check_per_customer", ch_url, ch_properties)

    customer_sales_base_df.unpersist()

    print("Generating Time Sales Reports...")
    time_sales_base_df = fact_sales_df \
        .join(dim_date_df, "date_key", "inner") \
        .select(
            fact_sales_df.sale_total_price,
            dim_date_df.year,
            dim_date_df.month,
            dim_date_df.month_name,
            dim_date_df.full_date
        )
    time_sales_base_df.cache()

    report_monthly_sales_trend = time_sales_base_df \
        .groupBy("year", "month", "month_name") \
        .agg(sum("sale_total_price").alias("monthly_revenue")) \
        .orderBy("year", "month")
    write_to_clickhouse(report_monthly_sales_trend, "report_monthly_sales_trend", ch_url, ch_properties)

    report_yearly_sales_trend = time_sales_base_df \
        .groupBy("year") \
        .agg(sum("sale_total_price").alias("yearly_revenue")) \
        .orderBy("year")
    write_to_clickhouse(report_yearly_sales_trend, "report_yearly_sales_trend", ch_url, ch_properties)
    

    report_avg_order_size_by_month = time_sales_base_df \
        .groupBy("year", "month", "month_name") \
        .agg(avg("sale_total_price").alias("average_order_size")) \
        .orderBy("year", "month")
    write_to_clickhouse(report_avg_order_size_by_month, "report_avg_order_size_by_month", ch_url, ch_properties)

    time_sales_base_df.unpersist()
    
    print("Generating Store Sales Reports...")
    store_sales_base_df = fact_sales_df \
        .join(dim_store_df, "store_key", "inner") \
        .join(dim_country_df, dim_store_df.country_key == dim_country_df.country_key, "left") \
        .select(
            fact_sales_df.sale_total_price,
            dim_store_df.store_name,
            dim_store_df.city.alias("store_city"),
            dim_country_df.country_name.alias("store_country_name")
        )
    store_sales_base_df.cache()

    report_top_5_stores_by_revenue = store_sales_base_df \
        .groupBy("store_name") \
        .agg(sum("sale_total_price").alias("total_revenue")) \
        .orderBy(desc("total_revenue")) \
        .limit(5)
    write_to_clickhouse(report_top_5_stores_by_revenue, "report_top_5_stores_by_revenue", ch_url, ch_properties)
    
    report_sales_by_store_city_country = store_sales_base_df \
        .groupBy("store_country_name", "store_city") \
        .agg(sum("sale_total_price").alias("total_revenue"), count("*").alias("number_of_sales")) \
        .orderBy(desc("total_revenue"))
    write_to_clickhouse(report_sales_by_store_city_country, "report_sales_by_store_city_country", ch_url, ch_properties)

    report_avg_check_per_store = store_sales_base_df \
        .groupBy("store_name") \
        .agg(avg("sale_total_price").alias("average_check_amount")) \
        .orderBy(desc("average_check_amount"))
    write_to_clickhouse(report_avg_check_per_store, "report_avg_check_per_store", ch_url, ch_properties)

    store_sales_base_df.unpersist()

    print("Generating Supplier Sales Reports...")
    supplier_sales_base_df = fact_sales_df \
        .join(dim_supplier_df, "supplier_key", "inner") \
        .join(dim_country_df, dim_supplier_df.country_key == dim_country_df.country_key, "left") \
        .select(
            fact_sales_df.sale_total_price,
            fact_sales_df.product_price_at_sale, 
            dim_supplier_df.supplier_name,
            dim_country_df.country_name.alias("supplier_country_name")
        )
    supplier_sales_base_df.cache()

    report_top_5_suppliers_by_revenue = supplier_sales_base_df \
        .groupBy("supplier_name") \
        .agg(sum("sale_total_price").alias("total_revenue_from_supplier_products")) \
        .orderBy(desc("total_revenue_from_supplier_products")) \
        .limit(5)
    write_to_clickhouse(report_top_5_suppliers_by_revenue, "report_top_5_suppliers_by_revenue", ch_url, ch_properties)

    report_avg_product_price_by_supplier = supplier_sales_base_df \
        .groupBy("supplier_name") \
        .agg(avg("product_price_at_sale").alias("average_product_sale_price")) \
        .orderBy(desc("average_product_sale_price"))
    write_to_clickhouse(report_avg_product_price_by_supplier, "report_avg_product_price_by_supplier", ch_url, ch_properties)

    report_sales_by_supplier_country = supplier_sales_base_df \
        .groupBy("supplier_country_name") \
        .agg(sum("sale_total_price").alias("total_revenue")) \
        .orderBy(desc("total_revenue"))
    write_to_clickhouse(report_sales_by_supplier_country, "report_sales_by_supplier_country", ch_url, ch_properties)
    
    supplier_sales_base_df.unpersist()

    print("Generating Product Quality Reports...")
    quality_base_df = fact_sales_df \
        .join(dim_product_df, "product_key", "inner") \
        .select(
            dim_product_df.product_name,
            dim_product_df.rating.alias("product_rating"),
            dim_product_df.reviews.alias("product_reviews"),
            fact_sales_df.sale_quantity
        )
    quality_base_df.cache()

    product_unique_ratings = quality_base_df \
        .groupBy("product_name") \
        .agg(
            avg("product_rating").alias("average_rating"),
            sum("product_reviews").alias("total_reviews")
        )

    report_highest_rated_products = product_unique_ratings \
        .orderBy(desc("average_rating"), desc("total_reviews")) \
        .limit(5)
    write_to_clickhouse(report_highest_rated_products, "report_highest_rated_products", ch_url, ch_properties)

    report_lowest_rated_products = product_unique_ratings \
        .filter(col("average_rating").isNotNull()) \
        .orderBy(col("average_rating").asc(), desc("total_reviews")) \
        .limit(5)
    write_to_clickhouse(report_lowest_rated_products, "report_lowest_rated_products", ch_url, ch_properties)

    product_sales_for_corr = quality_base_df \
        .groupBy("product_name") \
        .agg(
            avg("product_rating").alias("avg_product_rating"),
            sum("sale_quantity").alias("total_quantity_sold")
        ).filter(col("avg_product_rating").isNotNull() & col("total_quantity_sold").isNotNull())

    correlation_rating_sales = product_sales_for_corr.stat.corr("avg_product_rating", "total_quantity_sold")
    print(f"Correlation between product rating and sales quantity: {correlation_rating_sales}")
    correlation_df = spark.createDataFrame([(correlation_rating_sales,)], ["correlation_rating_sales_qty"])
    write_to_clickhouse(correlation_df, "report_correlation_rating_sales", ch_url, ch_properties)


    report_most_reviewed_products = product_unique_ratings \
        .orderBy(desc("total_reviews")) \
        .limit(10) # Например, топ-10
    write_to_clickhouse(report_most_reviewed_products, "report_most_reviewed_products", ch_url, ch_properties)
    
    quality_base_df.unpersist()
    fact_sales_df.unpersist() 
    dim_product_df.unpersist()

    print("ETL to ClickHouse Reports completed.")
    spark.stop()

if __name__ == "__main__":
    main()