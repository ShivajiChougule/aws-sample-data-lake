import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Reading from sources
PostgreSQL_cusomers = glueContext.create_dynamic_frame.from_catalog(database="data_lake_catalog_db", table_name="source_centrofood_public_customers", transformation_ctx="PostgreSQL_cusomers")

PostgreSQL_inventory = glueContext.create_dynamic_frame.from_catalog(database="data_lake_catalog_db", table_name="source_centrofood_public_inventory", transformation_ctx="PostgreSQL_inventory")

PostgreSQL_product_group_master = glueContext.create_dynamic_frame.from_catalog(database="data_lake_catalog_db", table_name="source_centrofood_public_product_group_master", transformation_ctx="PostgreSQL_product_group_master")

PostgreSQL_product_master = glueContext.create_dynamic_frame.from_catalog(database="data_lake_catalog_db", table_name="source_centrofood_public_product_master", transformation_ctx="PostgreSQL_product_master")

PostgreSQL_store_master_data = glueContext.create_dynamic_frame.from_catalog(database="data_lake_catalog_db", table_name="source_centrofood_public_store_master_data", transformation_ctx="PostgreSQL_store_master_data")

PostgreSQL_warehouse = glueContext.create_dynamic_frame.from_catalog(database="data_lake_catalog_db", table_name="source_centrofood_public_warehouse", transformation_ctx="PostgreSQL_warehouse")


source_sales_budget = glueContext.create_dynamic_frame.from_catalog(database="data_lake_catalog_db", table_name="source_sales_budget_json", transformation_ctx="source_sales_budget")

source_sales_order_invoice_lins = glueContext.create_dynamic_frame.from_catalog(database="data_lake_catalog_db", table_name="source_sales_order_invoice_lins_json", transformation_ctx="source_sales_order_invoice_lins")

AmazonS3_source_sales_budget = glueContext.write_dynamic_frame.from_options(frame=source_sales_budget, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-lake-demo-medallion/bronze/sales_budget/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_source_sales_budget")

AmazonS3_source_sales_order_invoice_lins = glueContext.write_dynamic_frame.from_options(frame=source_sales_order_invoice_lins, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-lake-demo-medallion/bronzsource_sales_order_invoice_lins/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_source_sales_order_invoice_lins")







# Writing to bronze S3
AmazonS3_customers = glueContext.write_dynamic_frame.from_options(frame=PostgreSQL_cusomers, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-lake-demo-medallion/bronze/customers/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_customers")

AmazonS3_inventory = glueContext.write_dynamic_frame.from_options(frame=PostgreSQL_inventory, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-lake-demo-medallion/bronze/inventory/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_inventory")

AmazonS3_product_group_master = glueContext.write_dynamic_frame.from_options(frame=PostgreSQL_product_group_master, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-lake-demo-medallion/bronze/product_group_master/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_product_group_master")

AmazonS3_product_master = glueContext.write_dynamic_frame.from_options(frame=PostgreSQL_product_master, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-lake-demo-medallion/bronze/product_master/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_product_master")

AmazonS3_store_master_data = glueContext.write_dynamic_frame.from_options(frame=PostgreSQL_store_master_data, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-lake-demo-medallion/bronze/store_master_data/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_store_master_data")

AmazonS3_warehouse = glueContext.write_dynamic_frame.from_options(frame=PostgreSQL_warehouse, connection_type="s3", format="glueparquet", connection_options={"path": "s3://data-lake-demo-medallion/bronze/warehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_warehouse")

job.commit()