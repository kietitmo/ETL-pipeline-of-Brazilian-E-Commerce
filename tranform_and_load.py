import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.functions import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .config("spark.jars", "C:\\tools\\spark-3.3.2-bin-hadoop3\\jars\\postgresql-42.6.2.jar")\
    .master("local[*]") \
    .appName('spark-app') \
    .getOrCreate()

def write_to_pg(df, table_name):
    df.write.format("jdbc")\
        .option("url", "jdbc:postgresql://ep-small-salad-a2sbsxpd.eu-central-1.aws.neon.tech/brazilian_ecommerce") \
        .option("driver", "org.postgresql.Driver").option("dbtable", table_name) \
        .option("user", "brazilian_ecommerce_owner").option("password", "xQI5TMXN3COU").save()


df_orders =spark.read.parquet("hdfs://localhost:9000/home/datalake/orders")
df_customers =spark.read.parquet("hdfs://localhost:9000/home/datalake/customers")
df_order_items =spark.read.parquet("hdfs://localhost:9000/home/datalake/order_items")
df_payments =spark.read.parquet("hdfs://localhost:9000/home/datalake/payments")
df_products =spark.read.parquet("hdfs://localhost:9000/home/datalake/products")
df_reviews =spark.read.parquet("hdfs://localhost:9000/home/datalake/reviews")
df_sellers =spark.read.parquet("hdfs://localhost:9000/home/datalake/sellers")
df_geolocation =spark.read.parquet("hdfs://localhost:9000/home/datalake/geolocation")
df_category_name_translation =spark.read.parquet("hdfs://localhost:9000/home/datalake/category_name_translation")

df_orders_2 = df_orders.groupby('order_status').count()
df_orders_2.show()
write_to_pg(df_orders_2, 'orders_status')


df_order_item_product = df_order_items.join(df_products,'product_id', 'inner')\
                                    .select('product_id', 'product_category_name', 'price',)
df_order_item_product_en = df_order_item_product.join(df_category_name_translation, 'product_category_name', 'inner')\
                                                .drop('product_category_name')

popular_products = df_order_item_product_en.groupby('product_category_name_english')\
                                            .agg(count('product_category_name_english').alias('count'), \
                                                 sum('price').alias('total'))\
                                            .orderBy('count', ascending = False)\
                                            .withColumn('total', round(col('total'),2))

popular_products.show(10)
write_to_pg(popular_products, 'popular_products')

df_sellers_orders_detail = df_order_items.join(df_sellers,'seller_id', 'inner')\
                                        .drop('order_item_id', 'shipping_limit_date', 'freight_value', 'seller_zip_code_prefix')

df_sellers_total = df_sellers_orders_detail.groupby('seller_id').agg(sum('price').alias('total'))\
                        .orderBy('total', ascending = False)\
                        .withColumn('total', round(col('total'),2))
df_sellers_total.show()

write_to_pg(df_sellers_total, 'top_sellers_total')

df_orders_customer = df_orders.join(df_customers,'customer_id', 'inner')\
                                .select('customer_city', 'customer_state', 'order_id' )\
                                .groupby('customer_city', 'customer_state').agg(count('order_id').alias('order_quantity'))\
                                .orderBy('order_quantity', ascending = False)
df_orders_customer.show()

write_to_pg(df_orders_customer, 'top_purchased_cities')


from pyspark.sql import functions as f

df_orders_customer_items = df_orders.join(df_customers,'customer_id', 'inner')
df_orders_customer_items = df_orders_customer_items.join(df_order_items,'order_id', 'inner')
df_orders_customer_items = df_orders_customer_items.join(df_products,'product_id', 'inner')
df_orders_customer_items = df_orders_customer_items.select('customer_city', 'product_category_name')\
                        .groupby( 'customer_city', 'product_category_name')\
                        .agg(count('product_category_name'))\
                        .orderBy('customer_city', ascending = True)
df_orders_customer_items = df_orders_customer_items.withColumn("product_count", f.array("count(product_category_name)","product_category_name"))
df_orders_customer_items = df_orders_customer_items.groupby("customer_city")\
                        .agg(max("product_count").getItem(1).alias("most_purchased_product"), \
                             max("product_count").getItem(0).alias("count"),)
df_orders_customer_items.show()

write_to_pg(df_orders_customer_items, 'most_purchased_product_of_cities')

df_top_payments_type = df_payments.groupby('payment_type').count().alias('count').orderBy('count', ascending = False)
df_top_payments_type.show(10)

write_to_pg(df_top_payments_type, 'top_payments_type')

df_order_shipping = df_orders.select('order_id','order_delivered_customer_date', 'order_estimated_delivery_date').where(df_orders.order_status == 'delivered')

df_order_shipping.show()

df_order_delay = df_order_shipping.filter(df_order_shipping.order_delivered_customer_date > df_order_shipping.order_estimated_delivery_date)

df_order_delay= df_order_delay.withColumn('delay_hours', \
                          round(((f.col('order_delivered_customer_date').cast("long") - f.col('order_estimated_delivery_date').cast("long"))/3600), 2))

write_to_pg(df_order_delay, 'orders_delay')