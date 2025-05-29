from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, monotonically_increasing_id, md5, concat_ws,
    to_date, to_timestamp, year, month, dayofmonth, quarter, dayofweek,
    udf, expr
)
from pyspark.sql.types import IntegerType, NumericType, DateType, StringType


@udf(returnType=IntegerType())
def safe_to_int(s):
    if s is None or s == '':
        return None
    try:
        return int(s)
    except ValueError:
        return None

@udf(returnType=NumericType(10, 2)) 
def safe_to_numeric(s):
    if s is None or s == '':
        return None
    try:
        return float(s)
    except ValueError:
        return None

@udf(returnType=NumericType(3,1)) 
def safe_to_rating_numeric(s):
    if s is None or s == '':
        return None
    try:
        return float(s)
    except ValueError:
        return None



def get_spark_session(app_name="PostgresStarSchemaETL"):
    postgres_jar_path = "/opt/spark-jars/postgresql-42.6.0.jar" 

    spark = SparkSession.builder \
        .appName(app_name) \
        .master("spark://spark-master:7077") \
        .config("spark.driver.extraClassPath", postgres_jar_path) \
        .config("spark.executor.extraClassPath", postgres_jar_path) \
        .config("spark.jars", postgres_jar_path) \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    return spark

def write_to_postgres(df, table_name, pg_url, pg_properties):
    print(f"Writing to PostgreSQL table: {table_name}...")
    try:
        df.write.jdbc(url=pg_url, table=table_name, mode="overwrite", properties=pg_properties)
        print(f"Successfully wrote to {table_name}. Count: {df.count()}")
    except Exception as e:
        print(f"Error writing {table_name} to PostgreSQL: {e}")

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN") 

    pg_url = "jdbc:postgresql://postgres_db:5432/lab_db"
    pg_properties = {"user": "lab_user", "password": "lab_password", "driver": "org.postgresql.Driver"}

    print("Reading from mock_data_staging...")
    df_staging = spark.read.jdbc(url=pg_url, table="mock_data_staging", properties=pg_properties)
    # df_staging.printSchema()
    # df_staging.show(3, truncate=False)

    print("Applying type conversions to staging data...")
    df_staging = df_staging \
        .withColumn("customer_age", safe_to_int(col("customer_age_text"))) \
        .withColumn("product_price", safe_to_numeric(col("product_price_text"))) \
        .withColumn("product_quantity_at_source", safe_to_int(col("product_quantity_at_source_text"))) \
        .withColumn("sale_date", to_date(col("sale_date_text"), "MM/dd/yyyy")) \
        .withColumn("sale_customer_id_val", safe_to_int(col("sale_customer_id_text"))) \
        .withColumn("sale_seller_id_val", safe_to_int(col("sale_seller_id_text"))) \
        .withColumn("sale_product_id_val", safe_to_int(col("sale_product_id_text"))) \
        .withColumn("sale_quantity", safe_to_int(col("sale_quantity_text"))) \
        .withColumn("sale_total_price", safe_to_numeric(col("sale_total_price_text"))) \
        .withColumn("product_weight", safe_to_numeric(col("product_weight_text"))) \
        .withColumn("product_rating", safe_to_rating_numeric(col("product_rating_text"))) \
        .withColumn("product_reviews", safe_to_int(col("product_reviews_text"))) \
        .withColumn("product_release_date", to_date(col("product_release_date_text"), "MM/dd/yyyy")) \
        .withColumn("product_expiry_date", to_date(col("product_expiry_date_text"), "MM/dd/yyyy"))

    df_staging.cache()
    print(f"Staging data read and converted. Count: {df_staging.count()}")

    print("Processing DimDate...")
    dates_df = df_staging.select(col("sale_date").alias("dt")) \
        .union(df_staging.select(col("product_release_date").alias("dt"))) \
        .union(df_staging.select(col("product_expiry_date").alias("dt"))) \
        .filter(col("dt").isNotNull()).distinct()

    dim_date_df = dates_df.select(
        col("dt").alias("full_date"),
        dayofweek(col("dt")).alias("day_of_week"),
        expr("date_format(dt, 'E')").alias("day_name"), 
        dayofmonth(col("dt")).alias("day_of_month"),
        month(col("dt")).alias("month"),
        expr("date_format(dt, 'MMM')").alias("month_name"), 
        quarter(col("dt")).alias("quarter"),
        year(col("dt")).alias("year")
    ).distinct() 
    write_to_postgres(dim_date_df.select("full_date", "day_of_week", "day_name", "day_of_month", "month", "month_name", "quarter", "year"), "DimDate", pg_url, pg_properties)
    dim_date_loaded_df = spark.read.jdbc(url=pg_url, table="DimDate", properties=pg_properties)

    print("Processing DimCountry...")
    countries_staging_df = df_staging.select(col("customer_country").alias("country")) \
        .union(df_staging.select(col("seller_country").alias("country"))) \
        .union(df_staging.select(col("store_country").alias("country"))) \
        .union(df_staging.select(col("supplier_country").alias("country"))) \
        .filter(col("country").isNotNull() & (col("country") != "")).distinct()
    
    dim_country_df = countries_staging_df.select(col("country").alias("country_name"))
    write_to_postgres(dim_country_df, "DimCountry", pg_url, pg_properties)
    dim_country_loaded_df = spark.read.jdbc(url=pg_url, table="DimCountry", properties=pg_properties)

    print("Processing DimCustomer...")
    dim_customer_intermediate_df = df_staging.select(
        "customer_email", "customer_first_name", "customer_last_name", "customer_age", "customer_country", "customer_postal_code"
    ).filter(col("customer_email").isNotNull() & (col("customer_email") != "")).distinct()

    dim_customer_df = dim_customer_intermediate_df.join(
        dim_country_loaded_df, dim_customer_intermediate_df.customer_country == dim_country_loaded_df.country_name, "left"
    ).select(
        col("customer_email"),
        col("customer_first_name").alias("first_name"),
        col("customer_last_name").alias("last_name"),
        col("customer_age").alias("age"),
        col("country_key"),
        col("customer_postal_code").alias("postal_code")
    )
    write_to_postgres(dim_customer_df, "DimCustomer", pg_url, pg_properties)
    dim_customer_loaded_df = spark.read.jdbc(url=pg_url, table="DimCustomer", properties=pg_properties)

    print("Processing DimCustomerPet...")
    dim_customer_pet_intermediate_df = df_staging.select(
        "customer_email", "customer_pet_type", "customer_pet_name", "customer_pet_breed"
    ).filter(
        col("customer_email").isNotNull() & (col("customer_email") != "") &
        ( (col("customer_pet_type").isNotNull() & (col("customer_pet_type") != "")) |
          (col("customer_pet_name").isNotNull() & (col("customer_pet_name") != "")) |
          (col("customer_pet_breed").isNotNull() & (col("customer_pet_breed") != "")) )
    ).distinct()

    dim_customer_pet_df = dim_customer_pet_intermediate_df.join(
        dim_customer_loaded_df.select("customer_key", "customer_email"), "customer_email", "inner"
    ).select(
        "customer_key", "customer_pet_type", "customer_pet_name", "customer_pet_breed"
    )
    write_to_postgres(dim_customer_pet_df, "DimCustomerPet", pg_url, pg_properties)

    print("Processing DimSeller...")
    dim_seller_intermediate_df = df_staging.select(
        "seller_email", "seller_first_name", "seller_last_name", "seller_country", "seller_postal_code"
    ).filter(col("seller_email").isNotNull() & (col("seller_email") != "")).distinct()

    dim_seller_df = dim_seller_intermediate_df.join(
        dim_country_loaded_df, dim_seller_intermediate_df.seller_country == dim_country_loaded_df.country_name, "left"
    ).select(
        col("seller_email"),
        col("seller_first_name").alias("first_name"),
        col("seller_last_name").alias("last_name"),
        col("country_key"),
        col("seller_postal_code").alias("postal_code")
    )
    write_to_postgres(dim_seller_df, "DimSeller", pg_url, pg_properties)
    dim_seller_loaded_df = spark.read.jdbc(url=pg_url, table="DimSeller", properties=pg_properties)

    print("Processing DimPetCategory...")
    dim_pet_category_df = df_staging.select(
        col("pet_category").alias("pet_category_name")
    ).filter(col("pet_category_name").isNotNull() & (col("pet_category_name") != "")).distinct()
    write_to_postgres(dim_pet_category_df, "DimPetCategory", pg_url, pg_properties)
    dim_pet_category_loaded_df = spark.read.jdbc(url=pg_url, table="DimPetCategory", properties=pg_properties)

    print("Processing DimProductCategory...")
    dim_prod_cat_intermediate_df = df_staging.select(
        col("product_category"),
        col("pet_category")
    ).filter(col("product_category").isNotNull() & (col("product_category") != "")).distinct()

    dim_product_category_df = dim_prod_cat_intermediate_df.join(
        dim_pet_category_loaded_df, dim_prod_cat_intermediate_df.pet_category == dim_pet_category_loaded_df.pet_category_name, "left"
    ).select(
        col("product_category").alias("category_name"),
        col("pet_category_key")
    ).distinct() 
    write_to_postgres(dim_product_category_df, "DimProductCategory", pg_url, pg_properties)
    dim_product_category_loaded_df = spark.read.jdbc(url=pg_url, table="DimProductCategory", properties=pg_properties)

    print("Processing DimProductBrand...")
    dim_product_brand_df = df_staging.select(
        col("product_brand").alias("brand_name")
    ).filter(col("brand_name").isNotNull() & (col("brand_name") != "")).distinct()
    write_to_postgres(dim_product_brand_df, "DimProductBrand", pg_url, pg_properties)
    dim_product_brand_loaded_df = spark.read.jdbc(url=pg_url, table="DimProductBrand", properties=pg_properties)
    
    print("Processing DimProduct...")
    dim_product_intermediate_df = df_staging.select(
        "product_name", "product_category", "pet_category", "product_brand", "product_color", "product_size",
        "product_material", "product_weight", "product_description", "product_rating", "product_reviews",
        "product_release_date", "product_expiry_date"
    ).filter(col("product_name").isNotNull() & (col("product_name") != "")).distinct() # distinct по бизнес-ключам

    dim_product_intermediate_df = dim_product_intermediate_df.join(
        dim_product_brand_loaded_df, dim_product_intermediate_df.product_brand == dim_product_brand_loaded_df.brand_name, "left"
    )

    dim_product_intermediate_df = dim_product_intermediate_df.join(
        dim_pet_category_loaded_df, dim_product_intermediate_df.pet_category == dim_pet_category_loaded_df.pet_category_name, "left"
    ).join(
        dim_product_category_loaded_df,
        (dim_product_intermediate_df.product_category == dim_product_category_loaded_df.category_name) &
        (dim_product_intermediate_df.pet_category_key == dim_product_category_loaded_df.pet_category_key), 
        "left"
    ).withColumnRenamed("category_key", "prod_category_key_from_dim") 

    dim_product_intermediate_df = dim_product_intermediate_df.join(
        dim_date_loaded_df.withColumnRenamed("date_key", "release_date_k").withColumnRenamed("full_date", "release_f_date"), 
        dim_product_intermediate_df.product_release_date == col("release_f_date"), "left"
    ).join(
        dim_date_loaded_df.withColumnRenamed("date_key", "expiry_date_k").withColumnRenamed("full_date", "expiry_f_date"), 
        dim_product_intermediate_df.product_expiry_date == col("expiry_f_date"), "left"
    )

    dim_product_df_to_write = dim_product_intermediate_df.select(
        col("product_name"),
        col("prod_category_key_from_dim").alias("category_key"),
        col("brand_key"),
        col("product_color").alias("color"),
        col("product_size").alias("size_"), # size -> size_
        col("product_material").alias("material"),
        col("product_weight").alias("weight"),
        col("product_description").alias("description"),
        col("product_rating").alias("rating"),
        col("product_reviews").alias("reviews"),
        col("release_date_k").alias("release_date_key"),
        col("expiry_date_k").alias("expiry_date_key")
    ).distinct() 

    write_to_postgres(dim_product_df_to_write, "DimProduct", pg_url, pg_properties)
    dim_product_loaded_df = spark.read.jdbc(url=pg_url, table="DimProduct", properties=pg_properties)

    print("Processing DimStore...")
    dim_store_intermediate_df = df_staging.select(
        "store_name", "store_email", "store_location", "store_city", "store_state", "store_country", "store_phone"
    ).filter(col("store_name").isNotNull() & (col("store_name") != "")).distinct()

    dim_store_df = dim_store_intermediate_df.join(
        dim_country_loaded_df, dim_store_intermediate_df.store_country == dim_country_loaded_df.country_name, "left"
    ).select(
        col("store_name"),
        col("store_email"),
        col("store_location").alias("location_address"),
        col("store_city").alias("city"),
        col("store_state").alias("state"),
        col("country_key"),
        col("store_phone").alias("phone")
    ).distinct()
    write_to_postgres(dim_store_df, "DimStore", pg_url, pg_properties)
    dim_store_loaded_df = spark.read.jdbc(url=pg_url, table="DimStore", properties=pg_properties)

    print("Processing DimSupplier...")
    dim_supplier_intermediate_df = df_staging.select(
        "supplier_name", "supplier_email", "supplier_contact", "supplier_phone",
        "supplier_address", "supplier_city", "supplier_country"
    ).filter(col("supplier_name").isNotNull() & (col("supplier_name") != "")).distinct()

    dim_supplier_df = dim_supplier_intermediate_df.join(
        dim_country_loaded_df, dim_supplier_intermediate_df.supplier_country == dim_country_loaded_df.country_name, "left"
    ).select(
        col("supplier_name"),
        col("supplier_email"),
        col("supplier_contact").alias("contact_person"),
        col("supplier_phone").alias("phone"),
        col("supplier_address").alias("address"),
        col("supplier_city").alias("city"),
        col("country_key")
    ).distinct()
    write_to_postgres(dim_supplier_df, "DimSupplier", pg_url, pg_properties)
    dim_supplier_loaded_df = spark.read.jdbc(url=pg_url, table="DimSupplier", properties=pg_properties)


    print("Processing FactSales...")
    fact_sales_df = df_staging.select(
        col("id_source").alias("sale_id_source"), 
        col("sale_date"),
        col("customer_email"),
        col("seller_email"),
        col("product_name"),
        col("product_brand"), 
        col("product_category"), 
        col("pet_category"), 
        col("store_name"),
        col("store_city"), 
        col("store_country"),
        col("supplier_name"),
        col("supplier_email"), 
        col("sale_quantity"),
        col("product_price").alias("product_price_at_sale"), 
        col("sale_total_price")
    )

    fact_sales_df = fact_sales_df.join(dim_date_loaded_df, fact_sales_df.sale_date == dim_date_loaded_df.full_date, "inner") \
        .withColumnRenamed("date_key", "fs_date_key").drop("sale_date", "full_date") # Убираем дублирующиеся колонки дат

    fact_sales_df = fact_sales_df.join(dim_customer_loaded_df.select("customer_key", "customer_email"), "customer_email", "inner") \
        .withColumnRenamed("customer_key", "fs_customer_key").drop("customer_email")

    fact_sales_df = fact_sales_df.join(dim_seller_loaded_df.select("seller_key", "seller_email"), "seller_email", "inner") \
        .withColumnRenamed("seller_key", "fs_seller_key").drop("seller_email")
        
    dim_product_for_join = dim_product_loaded_df \
        .join(dim_product_brand_loaded_df, "brand_key", "left") \
        .join(dim_product_category_loaded_df, "category_key", "left") \
        .join(dim_pet_category_loaded_df, "pet_category_key", "left") \
        .select(
            col("product_key"), 
            col("product_name").alias("dp_product_name"), 
            col("brand_name").alias("dp_brand_name"),
            col("category_name").alias("dp_category_name"),
            col("pet_category_name").alias("dp_pet_category_name")
        )

    fact_sales_df = fact_sales_df.join(
        dim_product_for_join,
        (fact_sales_df.product_name == dim_product_for_join.dp_product_name) &
        (fact_sales_df.product_brand == dim_product_for_join.dp_brand_name) &
        (fact_sales_df.product_category == dim_product_for_join.dp_category_name) &
        (fact_sales_df.pet_category == dim_product_for_join.dp_pet_category_name),
        "inner"
    ).withColumnRenamed("product_key", "fs_product_key") \
    .drop("product_name", "product_brand", "product_category", "pet_category", "dp_product_name", "dp_brand_name", "dp_category_name", "dp_pet_category_name")

    dim_store_for_join = dim_store_loaded_df \
        .join(dim_country_loaded_df, "country_key", "left") \
        .select(
            col("store_key"),
            col("store_name").alias("ds_store_name"),
            col("city").alias("ds_city"),
            col("country_name").alias("ds_country_name")
        )

    fact_sales_df = fact_sales_df.join(
        dim_store_for_join,
        (fact_sales_df.store_name == dim_store_for_join.ds_store_name) &
        (fact_sales_df.store_city == dim_store_for_join.ds_city) &
        (fact_sales_df.store_country == dim_store_for_join.ds_country_name),
        "inner"
    ).withColumnRenamed("store_key", "fs_store_key") \
    .drop("store_name", "store_city", "store_country", "ds_store_name", "ds_city", "ds_country_name")
    
    fact_sales_df = fact_sales_df.join(
        dim_supplier_loaded_df.select("supplier_key", "supplier_name", "supplier_email"),
        (fact_sales_df.supplier_name == dim_supplier_loaded_df.supplier_name) &
        (fact_sales_df.supplier_email == dim_supplier_loaded_df.supplier_email),
        "inner"
    ).withColumnRenamed("supplier_key", "fs_supplier_key") \
    .drop("supplier_name", "supplier_email")

    fact_sales_final_df = fact_sales_df.select(
        col("sale_id_source"),
        col("fs_date_key").alias("date_key"),
        col("fs_customer_key").alias("customer_key"),
        col("fs_seller_key").alias("seller_key"),
        col("fs_product_key").alias("product_key"),
        col("fs_store_key").alias("store_key"),
        col("fs_supplier_key").alias("supplier_key"),
        col("sale_quantity"),
        col("product_price_at_sale"),
        col("sale_total_price")
    )
    # fact_sales_final_df.printSchema()
    # fact_sales_final_df.show(5)
    write_to_postgres(fact_sales_final_df, "FactSales", pg_url, pg_properties)

    print("ETL to PostgreSQL Star Schema completed.")
    df_staging.unpersist() 
    spark.stop()

if __name__ == "__main__":
    main()