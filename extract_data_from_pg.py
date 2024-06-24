import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

spark = SparkSession.builder \
    .config("spark.jars", "C:\\tools\\spark-3.3.2-bin-hadoop3\\jars\\postgresql-42.6.2.jar")\
    .master("local[*]") \
    .appName('spark-app') \
    .getOrCreate()

spark.version

from pyspark.sql.types import *

def read_table_to_df(table_name, schema):
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", table_name) \
        .option("user", "root") \
        .option("password", "root") \
        .option("driver", "org.postgresql.Driver") \
        .schema(schema) \
        .load()
    return df

schema_customers = StructType([ \
    StructField("customer_id", StringType(), True), \
    StructField("customer_unique_id", StringType(), True), \
    StructField("customer_zip_code_prefix", StringType(), True), \
    StructField("customer_city", StringType(), True), \
    StructField("customer_state", StringType(), True), \
  ])

df_customers = read_table_to_df("olist_customers_dataset", schema_customers) 

df_customers.repartition(16).write.parquet('hdfs://localhost:9000/home/datalake/customers', mode='overwrite')

schema_orders = StructType([ \
    StructField("order_id", StringType(), True), \
    StructField("customer_id", StringType(), True), \
    StructField("order_status", StringType(), True), \
    StructField("order_purchase_timestamp", TimestampType(), True), \
    StructField("order_approved_at", TimestampType(), True), \
    StructField("order_delivered_carrier_date", TimestampType(), True), \
    StructField("order_delivered_customer_date", TimestampType(), True), \
    StructField("order_estimated_delivery_date", TimestampType(), True), \
  ])

df_orders = read_table_to_df("olist_orders_dataset", schema_orders) 

df_orders.repartition(16).write.parquet('hdfs://localhost:9000/home/datalake/orders', mode='overwrite')

schema_geolocation = StructType([ \
    StructField("geolocation_zip_code_prefix", StringType(), True), \
    StructField("geolocation_lat", DoubleType(), True), \
    StructField("geolocation_lng", DoubleType(), True), \
    StructField("geolocation_city", StringType(), True), \
    StructField("geolocation_state", StringType(), True), \
  ])

df_geolocation = read_table_to_df("olist_geolocation_dataset", schema_geolocation) 

df_geolocation.repartition(16).write.parquet('hdfs://localhost:9000/home/datalake/geolocation', mode='overwrite')

schema_order_items = StructType([ \
    StructField("order_id", StringType(), True), \
    StructField("order_item_id", StringType(), True), \
    StructField("product_id", StringType(), True), \
    StructField("seller_id", StringType(), True), \
    StructField("shipping_limit_date", TimestampType(), True), \
    StructField("price", DoubleType(), True), \
    StructField("freight_value", DoubleType(), True), \
  ])

df_order_items = read_table_to_df("olist_order_items_dataset", schema_order_items) 

df_order_items.repartition(16).write.parquet('hdfs://localhost:9000/home/datalake/order_items', mode='overwrite')

schema_order_payments = StructType([ \
    StructField("order_id", StringType(), True), \
    StructField("payment_sequential", DoubleType(), True), \
    StructField("payment_type", StringType(), True), \
    StructField("payment_installments", IntegerType(), True), \
    StructField("payment_value", DoubleType(), True), \
  ])

df_order_payments = read_table_to_df("olist_order_payments_dataset", schema_order_payments) 

df_order_payments.repartition(16).write.parquet('hdfs://localhost:9000/home/datalake/payments', mode='overwrite')

schema_order_reviews = StructType([ \
    StructField("review_id", StringType(), True), \
    StructField("order_id", StringType(), True), \
    StructField("review_score", IntegerType(), True), \
    StructField("review_comment_title", StringType(), True), \
    StructField("review_comment_message", StringType(), True), \
    StructField("review_creation_date", TimestampType(), True), \
    StructField("review_answer_timestamp", TimestampType(), True), \
  ])

df_order_reviews = read_table_to_df("olist_order_reviews_dataset", schema_order_reviews) 

df_order_reviews.repartition(16).write.parquet('hdfs://localhost:9000/home/datalake/reviews', mode='overwrite')

schema_products = StructType([ \
    StructField("product_id", StringType(), True), \
    StructField("product_category_name", StringType(), True), \
    StructField("product_name_lenght", IntegerType(), True), \
    StructField("product_description_lenght", IntegerType(), True), \
    StructField("product_photos_qty", IntegerType(), True), \
    StructField("product_weight_g", IntegerType(), True), \
    StructField("product_length_cm", IntegerType(), True), \
    StructField("product_height_cm", IntegerType(), True), \
    StructField("product_width_cm", IntegerType(), True), \
  ])

df_products = read_table_to_df("olist_products_dataset", schema_products) 

df_products.repartition(16).write.parquet('hdfs://localhost:9000/home/datalake/products', mode='overwrite')

schema_sellers = StructType([ \
    StructField("seller_id", StringType(), True), \
    StructField("seller_zip_code_prefix", StringType(), True), \
    StructField("seller_city", StringType(), True), \
    StructField("seller_state", StringType(), True), \
  ])

df_sellers = read_table_to_df("olist_sellers_dataset", schema_sellers) 

df_sellers.repartition(16).write.parquet('hdfs://localhost:9000/home/datalake/sellers', mode='overwrite')

schema_category_name_translation = StructType([ \
    StructField("product_category_name", StringType(), True), \
    StructField("product_category_name_english", StringType(), True), \
  ])

df_category_name_translation = read_table_to_df("product_category_name_translation", schema_category_name_translation) 

df_category_name_translation.repartition(16).write.parquet('hdfs://localhost:9000/home/datalake/category_name_translation', mode='overwrite')


