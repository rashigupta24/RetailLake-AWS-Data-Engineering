import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
from pyspark.sql import functions as F

products_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://retaillake-data/raw/products/"],
        "recurse": True
    },
    format="json",
    transformation_ctx="products_source"
)

silver_products = products_dyf.toDF()

silver_products.printSchema()

silver_products = silver_products.dropDuplicates(['product_id'])
silver_products = silver_products.withColumn("product_id", F.col("product_id").cast("int"))
silver_products = silver_products.dropna(
    subset=["product_id", "unit_cost", "product_name"]
)

amount_type = silver_products.schema["unit_cost"].dataType

if str(amount_type).startswith("StructType"):
    silver_products = silver_products.withColumn(
        "unit_cost",
        F.coalesce(
            F.col("unit_cost.double"),
            F.col("unit_cost.int").cast("double")
        )
    )
else:
    silver_products = silver_products.withColumn(
        "unit_cost",
        F.col("unit_cost").cast("double")
    )
silver_products = silver_products.fillna("unknown", subset=["category"])
column_to_trim = ['product_name', 'category']
for column in column_to_trim:
  silver_products = silver_products.withColumn(column, F.trim(F.col(column)))
silver_products= silver_products.withColumns({
  'product_name': F.initcap(F.col('product_name')),
  'category': F.initcap(F.col('category'))
})
product_condition = (F.col("product_id") > 0 )&(F.col("product_id").isNotNull()
)
  
unitCost_condition = (F.col("unit_cost") > 0) & (F.col("unit_cost") < 10000)
valid_categories = [
    "Electronics",
    "Clothing",
    "Books",
    "Sports",
    "Beauty",
    "Groceries",
    "Home",
    "Toys"
]

category_condition = F.col("category").isin(valid_categories)
product_name_condition = F.col("product_name").rlike(r"^[a-zA-Z ]+$")

silver_products = silver_products.withColumn(
    "processed_timestamp",
    F.current_timestamp()
)
silver_products = silver_products.withColumn(
    "source_system",
    F.lit("RetailLake")
)
valid_products = silver_products.filter(product_condition & unitCost_condition & category_condition & product_name_condition)
invalid_products = silver_products.filter(~product_condition | ~unitCost_condition | ~category_condition | ~product_name_condition)
invalid_products = invalid_products.withColumn(
    "validation_reason",
    F.when(~product_condition, "Invalid Product ID")
     .when(~unitCost_condition, "Invalid Unit Cost")
     .when(~category_condition, "Invalid Category")
     .when(~product_name_condition, "Invalid Product Name")
)
valid_products = valid_products.withColumn(
    "record_status",
    F.lit("VALID")
)
invalid_products = invalid_products.withColumn(
    "record_status",
    F.lit("INVALID")
)
valid_products = valid_products.withColumn(
    "product_tier",
    F.when(F.col("unit_cost") < 500, "Budget")
     .when(F.col("unit_cost") < 3000, "Standard")
     .otherwise("Premium")
)
valid_products = valid_products.withColumn(
    "price_band",
    F.when(F.col("unit_cost") < 1000, "Low")
     .when(F.col("unit_cost") < 5000, "Medium")
     .otherwise("High")
)
total = silver_products.count()
valid = valid_products.count()
invalid = invalid_products.count()
success_rate = round((valid / total) * 100, 2)

print(f"Total Records : {total}")
print(f"Valid Records : {valid}")
print(f"Invalid Records : {invalid}")
print(f"Success Rate : {success_rate}%")

valid_products.write \
.mode("append") \
.partitionBy("category") \
.parquet("s3://retaillake-data/silver/products/")

invalid_products.write \
.mode("append") \
.parquet("s3://retaillake-data/quarantine/products/")
job.commit()