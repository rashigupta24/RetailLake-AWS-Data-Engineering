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
orders_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://retaillake-data/raw/orders/"],
        "recurse": True
    },
    format="json",
    format_options={
        "multiline": True,
         "jsonPath": "$[*]"
    },
    transformation_ctx="orders_source"
)

print("DynamicFrame count:", orders_dyf.count())

silver_orders = orders_dyf.toDF()
coupons = spark.read.format("parquet").load("s3://retaillake-data/silver/coupons/")

customers = spark.read.format("parquet").load("s3://retaillake-data/silver/customers/")
products = spark.read.format("parquet").load("s3://retaillake-data/silver/products/")
silver_orders = silver_orders.dropDuplicates(['order_id'])
silver_orders = silver_orders.dropna(subset=["order_id","order_date","amount","product_id","customer_id"])
silver_orders = silver_orders.withColumn("order_id", F.col("order_id").cast("int"))
amount_type = silver_orders.schema["amount"].dataType

if str(amount_type).startswith("StructType"):
    silver_orders = silver_orders.withColumn(
        "amount",
        F.coalesce(
            F.col("amount.double"),
            F.col("amount.int").cast("double")
        )
    )
else:
    silver_orders = silver_orders.withColumn(
        "amount",
        F.col("amount").cast("double")
    )
silver_orders = silver_orders.withColumn("product_id", F.col("product_id").cast("int"))
silver_orders = silver_orders.withColumn("customer_id", F.col("customer_id").cast("int"))
silver_orders= silver_orders.withColumn("order_date", F.to_date(F.col("order_date")))
silver_orders= silver_orders.withColumn("order_month", F.month(F.col("order_date")))
silver_orders= silver_orders.withColumn("order_year", F.year(F.col("order_date")))
silver_orders= silver_orders.withColumn("order_day", F.dayofweek(F.col("order_date")))
silver_orders= silver_orders.withColumn("quarter", F.quarter(F.col("order_date")))

products_lookup = products.select(["product_id","product_name","category"]).dropDuplicates()
silver_orders = silver_orders.join(
    products_lookup,
    on="product_id",
    how="left"
).withColumn("product_exists", F.when(F.isnull(F.col("product_name")), False).otherwise(True))

customer_lookup = customers.select(["customer_id","name","city"]).dropDuplicates()
silver_orders = silver_orders.join(
    customer_lookup,
    on="customer_id",
    how="left"
).withColumn("customer_exists", F.when(F.isnull(F.col("name")), False).otherwise(True))

coupon_lookup = coupons.select(
    "coupon_id",
    "coupon_code"
).dropDuplicates()


silver_orders = silver_orders.join(
    coupon_lookup,
    on="coupon_id",
    how="left"
)

silver_orders = silver_orders.withColumn(
    "coupon_exists",
    F.when(
        F.col("coupon_id").isNull(),
        F.lit(True)              
    ).otherwise(
        F.when(
            F.col("coupon_code").isNull(),
            F.lit(False)         
        ).otherwise(
            F.lit(True)          
        )
    )
)
order_id_condition = F.col("order_id").isNotNull() & (F.col("order_id") > 0)
amount_condition = (F.col("amount") > 0) & (F.col("amount") < 10000)
customer_condition = F.col("customer_exists") == True
product_condition = F.col("product_exists") == True
coupon_condition = F.col("coupon_exists") == True
order_date_condition =(F.col("order_date") <= F.current_date() )& (F.col("order_date").isNotNull())
order_month_condition = (F.col("order_month") >= 1) & (F.col("order_month") <= 12)

silver_orders= silver_orders.withColumn("processed_timestamp", F.current_timestamp())
silver_orders= silver_orders.withColumn("source_system", F.lit("RetailLake"))
valid_silver_orders = silver_orders.filter(customer_condition & product_condition & coupon_condition & order_date_condition & order_month_condition & amount_condition & order_id_condition)

invalid_silver_orders = silver_orders.filter(~customer_condition | ~product_condition | ~coupon_condition | ~order_date_condition | ~order_month_condition | ~amount_condition | ~order_id_condition)
invalid_silver_orders = invalid_silver_orders.withColumn(
    "validation_reason",
    F.when(~customer_condition, "Invalid Customer")
     .when(~product_condition, "Invalid Product")
     .when(~coupon_condition, "Invalid Coupon")
     .when(~order_date_condition, "Invalid Order Date")
      .when(~order_month_condition, "Invalid Order Month")
      .when(~amount_condition, "Invalid Amount" )
      .when(~order_id_condition, "Invalid OrderId")
)
valid_silver_orders = valid_silver_orders.withColumn(
    "record_status",
    F.lit("VALID")    
)

valid_silver_orders = valid_silver_orders.withColumn(
    "amount_band", F.when(F.col("amount") <= 1000, "Low")
                                                   .when((F.col("amount") > 1000) & (F.col("amount") <= 2500), "Medium")
                                                   .when((F.col("amount") > 2500 )& (F.col("amount") <= 4000), "High")
                                                   .otherwise("Very High"))
valid_silver_orders = valid_silver_orders.withColumn("order_age_days" , F.datediff(F.current_date(), F.col("order_date")))
invalid_silver_orders = invalid_silver_orders.withColumn(
    "record_status",
    F.lit("INVALID")
)
valid_silver_orders = valid_silver_orders.drop("product_exists", "customer_exists", "coupon_exists")
invalid_silver_orders = invalid_silver_orders.drop("product_exists", "customer_exists", "coupon_exists")
total = silver_orders.count()
valid = valid_silver_orders.count()
invalid = invalid_silver_orders.count()
success_rate = round((valid / total) * 100, 2)

print(f"Total Records : {total}")
print(f"Valid Records : {valid}")
print(f"Invalid Records : {invalid}")
print(f"Success Rate : {success_rate}%")

valid_silver_orders.write \
.mode("append") \
.partitionBy("order_year","order_month") \
.parquet("s3://retaillake-data/silver/orders/")

invalid_silver_orders.write \
.mode("append") \
.parquet("s3://retaillake-data/quarantine/orders/")

job.commit()