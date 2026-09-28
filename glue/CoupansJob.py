import sys
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
from pyspark.sql import functions as F
coupons_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://retaillake-data/raw/coupons/"],
        "recurse": True
    },
    format="json",
    format_options={
        "multiline": True,
        "jsonPath": "$[*]"
    },
    transformation_ctx="coupons_source"
)

print("DynamicFrame count:", coupons_dyf.count())

df = coupons_dyf.toDF()

silver_coupons = coupons_dyf.toDF()


silver_coupons = silver_coupons.dropDuplicates(['coupon_id'])
silver_coupons = silver_coupons.dropna(subset=["coupon_id","coupon_code","expiry_date"])
silver_coupons = silver_coupons.withColumn("coupon_id", F.col("coupon_id").cast("int"))
silver_coupons = silver_coupons.withColumn("discount_percent", F.col("discount_percent").cast("double"))
silver_coupons = silver_coupons.withColumn("expiry_date", F.to_date(F.col("expiry_date")))
silver_coupons= silver_coupons.withColumn("expiry_year", F.year(F.col("expiry_date")))
silver_coupons= silver_coupons.withColumn("expiry_month", F.month(F.col("expiry_date")))                                       
for column in ['coupon_code']:
  silver_coupons = silver_coupons.withColumn(column, F.trim(F.col(column)))
silver_coupons= silver_coupons.withColumn(
  'coupon_code',F.upper(F.col('coupon_code'))
)
silver_coupons = silver_coupons.withColumn("processed_timestamp", F.current_timestamp())
silver_coupons = silver_coupons.withColumn("source_system", F.lit("RetailLake"))
discount_condition = (F.col("discount_percent") > 0) & (F.col("discount_percent") < 100) & (F.col("discount_percent").isNotNull())
expiration_condition = F.col("expiry_date").isNotNull()
coupon_code_condition = (
    F.col("coupon_code").rlike(r"^[A-Z0-9]+$") &
    F.col("coupon_code").isNotNull()
)
valid_coupons = silver_coupons.filter(
    discount_condition &
    expiration_condition &
    coupon_code_condition
)
invalid_coupons = silver_coupons.filter(
    ~discount_condition |
    ~expiration_condition |
    ~coupon_code_condition
)
invalid_coupons = invalid_coupons.withColumn(
    "validation_reason",
    F.when(~discount_condition, "Invalid Discount")
     .when(~expiration_condition, "Invalid Expiration Date")
     .when(~coupon_code_condition, "Invalid Coupon Code")
)
valid_coupons = valid_coupons.withColumn(
    "record_status",
    F.lit("VALID")
)
valid_coupons= valid_coupons.withColumn("discount_category",
   F.when(F.col("discount_percent") < 20, "Budget")
    .when(F.col("discount_percent") < 50, "Standard")
    .otherwise("Premium"))

valid_coupons= valid_coupons.withColumn("status",F.when(F.col("expiry_date") <= F.current_date(), "Expired")
    .otherwise("Active")
)
valid_coupons = valid_coupons.withColumn(
    "days_until_expiry",
    F.datediff(F.col("expiry_date"), F.current_date()).cast("int")
)

invalid_coupons = invalid_coupons.withColumn(
    "record_status",
    F.lit("INVALID")
)
total = silver_coupons.count()
valid = valid_coupons.count()
invalid = invalid_coupons.count()
success_rate = round((valid / total) * 100, 2)

print(f"Total Records : {total}")
print(f"Valid Records : {valid}")
print(f"Invalid Records : {invalid}")
print(f"Success Rate : {success_rate}%")

valid_coupons.write \
.mode("append") \
.partitionBy("expiry_year","expiry_month") \
.parquet("s3://retaillake-data/silver/coupons/")

invalid_coupons.write \
.mode("append") \
.parquet("s3://retaillake-data/quarantine/coupons/")
job.commit()