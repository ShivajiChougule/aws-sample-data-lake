import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re 
from pyspark.sql.functions import *



args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)











# Customers
AmazonS3_bronze_Customers = glueContext.create_dynamic_frame.from_catalog(database="data_lake_catalog_db", table_name="bronze_customers", transformation_ctx="AmazonS3_bronze_Customers")

AmazonS3_bronze_Customers_df = AmazonS3_bronze_Customers.toDF()


AmazonS3_bronze_Customers_df = AmazonS3_bronze_Customers_df.withColumn("customer_name_array", split(AmazonS3_bronze_Customers_df["customer_name"], "\s+"))


AmazonS3_bronze_Customers_df = AmazonS3_bronze_Customers_df.withColumn("first_name", AmazonS3_bronze_Customers_df["customer_name_array"][0]) \
    .withColumn("last_name", AmazonS3_bronze_Customers_df["customer_name_array"][1])


AmazonS3_bronze_Customers_df = AmazonS3_bronze_Customers_df.drop("customer_name_array")

ArrayToColumns_customers = DynamicFrame.fromDF(AmazonS3_bronze_Customers_df,glueContext)


Filter_customers = Filter.apply(frame=ArrayToColumns_customers, f=lambda row: (bool(re.match("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", row["email"])) and bool(re.match("^(?:\+\d{1,3}\s?)?\d{10}$", row["phone"]))), transformation_ctx="Filter_customers")



AmazonS3_result_customers = glueContext.write_dynamic_frame.from_options(frame=Filter_customers, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-lake-demo-medallion/silver/customers/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_result_customers")


#source_sales_order_invoice_lins
AmazonS3_bronze_invoice = glueContext.create_dynamic_frame.from_catalog(database="data_lake_catalog_db", table_name="bronze_source_sales_order_invoice_lins", transformation_ctx="AmazonS3_bronze_invoice")

ChangeSchema_invoice = ApplyMapping.apply(frame=AmazonS3_bronze_invoice, mappings=[("store_location_id", "string", "store_location_id", "int"), ("product_group_id", "string", "product_group_id", "int"), ("customer_id", "string", "customer_id", "int"), ("no_of_products", "string", "no_of_products", "int"), ("invoice_total_daily_sale", "string", "invoice_total_daily_sale", "int"), ("prod_id", "string", "prod_id", "int"), ("sales_order_id", "string", "sales_order_id", "int"), ("invoice_date", "string", "invoice_date", "date"), ("warehouse_id", "string", "warehouse_id", "int"), ("customer_name", "string", "customer_name", "string")], transformation_ctx="ChangeSchema_invoice")

AmazonS3_result_invoice = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_invoice, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-lake-demo-medallion/silver/source_sales_order_invoice_lins/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_result_invoice")

# product_group_master
AmazonS3_bronze_product_group_master = glueContext.create_dynamic_frame.from_catalog(database="data_lake_catalog_db", table_name="bronze_product_group_master", transformation_ctx="AmazonS3_bronze_product_group_master")

AmazonS3_result_product_group_master = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_bronze_product_group_master, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-lake-demo-medallion/silver/product_group_master/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_result_product_group_master")

# inventory
AmazonS3_bronze_inventory = glueContext.create_dynamic_frame.from_catalog(database="data_lake_catalog_db", table_name="bronze_inventory", transformation_ctx="AmazonS3_bronze_inventory")

AmazonS3_result_inventory = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_bronze_inventory, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-lake-demo-medallion/silver/inventory/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_result_inventory")

# product_master
AmazonS3_bronze_product_master = glueContext.create_dynamic_frame.from_catalog(database="data_lake_catalog_db", table_name="bronze_product_master", transformation_ctx="AmazonS3_bronze_product_master")

AmazonS3_result_product_master = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_bronze_product_master, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-lake-demo-medallion/silver/product_master/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_result_product_master")


# sales_budget
AmazonS3_bronze_sales_budget = glueContext.create_dynamic_frame.from_catalog(database="data_lake_catalog_db", table_name="bronze_sales_budget", transformation_ctx="AmazonS3_bronze_sales_budget")

AmazonS3_result_sales_budget = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_bronze_sales_budget, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-lake-demo-medallion/silver/sales_budget/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_result_sales_budget")


# store_master_data
AmazonS3_bronze_store_master_data = glueContext.create_dynamic_frame.from_catalog(database="data_lake_catalog_db", table_name="bronze_store_master_data", transformation_ctx="AmazonS3_bronze_store_master_data")

AmazonS3_result_store_master_data = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_bronze_store_master_data, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-lake-demo-medallion/silver/store_master_data/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_result_store_master_data")


# warehouse
AmazonS3_bronze_warehouse = glueContext.create_dynamic_frame.from_catalog(database="data_lake_catalog_db", table_name="bronze_warehouse", transformation_ctx="AmazonS3_bronze_warehouse")


ChangeSchema_warehouse = ApplyMapping.apply(frame=AmazonS3_bronze_warehouse, mappings=[("warehouse_id", "int", "warehouse_id", "int"), ("warehouse_name", "string", "warehouse_name", "string"), ("is_refrigerated", "boolean", "is_refrigerated", "int"), ("store_location_id", "int", "store_location_id", "int")], transformation_ctx="ChangeSchema_warehouse")


Filter_warehouse = Filter.apply(frame=ChangeSchema_warehouse, f=lambda row: (row["is_refrigerated"] == 0 or row["is_refrigerated"] == 1), transformation_ctx="Filter_warehouse")


AmazonS3_result_warehouse = glueContext.write_dynamic_frame.from_options(frame=Filter_warehouse, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-lake-demo-medallion/silver/warehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_result_warehouse")


job.commit()