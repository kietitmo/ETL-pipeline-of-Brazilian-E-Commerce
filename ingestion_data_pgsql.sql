DROP TABLE olist_order_items_dataset;
CREATE TABLE  olist_order_items_dataset
(order_id varchar,
order_item_id varchar,
product_id varchar,
seller_id varchar,
shipping_limit_date timestamp,
price float,
freight_value float
);

COPY olist_order_items_dataset
FROM 'varlibpostgresqldatadatasetolist_order_items_dataset.csv' 
DELIMITER ','
CSV HEADER;

---
DROP TABLE olist_customers_dataset;
CREATE TABLE  olist_customers_dataset
(customer_id varchar,
customer_unique_id varchar,
customer_zip_code_prefix varchar,
customer_city varchar,
customer_state varchar
);

COPY olist_customers_dataset
FROM 'varlibpostgresqldatadatasetolist_customers_dataset.csv' 
DELIMITER ','
CSV HEADER;

---
DROP TABLE olist_geolocation_dataset;
CREATE TABLE  olist_geolocation_dataset
(geolocation_zip_code_prefix varchar,
geolocation_lat float,
geolocation_lng float,
geolocation_city varchar,
geolocation_state varchar
);

COPY olist_geolocation_dataset
FROM 'varlibpostgresqldatadatasetolist_geolocation_dataset.csv' 
DELIMITER ','
CSV HEADER;

---
DROP TABLE olist_order_payments_dataset;
CREATE TABLE  olist_order_payments_dataset
(order_id varchar,
payment_sequential float,
payment_type varchar,
payment_installments integer,
payment_value float
);

COPY olist_order_payments_dataset
FROM 'varlibpostgresqldatadatasetolist_order_payments_dataset.csv' 
DELIMITER ','
CSV HEADER;

---
DROP TABLE olist_order_reviews_dataset;
CREATE TABLE  olist_order_reviews_dataset
(review_id varchar,
order_id varchar,
review_score integer,
review_comment_title varchar,
review_comment_message varchar,
review_creation_date timestamp,
review_answer_timestamp timestamp
);

COPY olist_order_reviews_dataset
FROM 'varlibpostgresqldatadatasetolist_order_reviews_dataset.csv' 
DELIMITER ','
CSV HEADER;

---
DROP TABLE olist_orders_dataset;
CREATE TABLE  olist_orders_dataset
(order_id varchar,
customer_id varchar,
order_status varchar,
order_purchase_timestamp timestamp,
order_approved_at timestamp,
order_delivered_carrier_date timestamp,
order_delivered_customer_date timestamp,
order_estimated_delivery_date timestamp
);

COPY olist_orders_dataset
FROM 'varlibpostgresqldatadatasetolist_orders_dataset.csv' 
DELIMITER ','
CSV HEADER;

---
DROP TABLE olist_products_dataset;
CREATE TABLE  olist_products_dataset
(product_id varchar,
product_category_name varchar,
product_name_lenght integer,
product_description_lenght integer,
product_photos_qty integer,
product_weight_g integer,
product_length_cm integer,
product_height_cm integer,
product_width_cm integer
);

COPY olist_products_dataset
FROM 'varlibpostgresqldatadatasetolist_products_dataset.csv' 
DELIMITER ','
CSV HEADER;


---
DROP TABLE olist_sellers_dataset;
CREATE TABLE  olist_sellers_dataset
(seller_id varchar,
seller_zip_code_prefix varchar,
seller_city varchar,
seller_state varchar
);

COPY olist_sellers_dataset
FROM 'varlibpostgresqldatadatasetolist_sellers_dataset.csv' 
DELIMITER ','
CSV HEADER;

---
DROP TABLE product_category_name_translation;
CREATE TABLE  product_category_name_translation
(product_category_name varchar,
product_category_name_english varchar
);

COPY product_category_name_translation
FROM 'varlibpostgresqldatadatasetproduct_category_name_translation.csv' 
DELIMITER ','
CSV HEADER;