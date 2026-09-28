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
customers_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://retaillake-data/raw/customers/"],
        "recurse": True
    },
    format="csv",
    format_options={
        "withHeader": True,
        "separator": ",",
        "inferSchema": True
    },
    transformation_ctx="customers_source"
)

silver_customers = customers_dyf.toDF()

silver_customers = silver_customers.withColumn(
    "customer_id",
    F.col("customer_id").cast("integer")
)
silver_customers = silver_customers.dropDuplicates(["customer_id"])
silver_customers = silver_customers.dropna(subset=["customer_id","email"])

silver_customers = silver_customers.fillna(value={"city": "unknown"})
name_condition = F.col("name").rlike(r"^[a-zA-Z ]+$")
column_to_trim = ["name","email","city"]
for column in column_to_trim:
  silver_customers = silver_customers.withColumn(column, F.trim(F.col(column)))

silver_customers = silver_customers.withColumn(
    "registration_date",
    F.to_date(F.col("registration_date"))
)

silver_customers = silver_customers.withColumn("city", F.lower(F.col("city")))
silver_customers = silver_customers.withColumn("name", F.initcap(F.col("name")))
email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
phone_regex = r"^[6-9][0-9]{9}$"
email_condition = F.col("email").rlike(email_regex)
phone_condition = F.col("phone").rlike(phone_regex)
registration_condition = (
    F.col("registration_date").isNotNull() &
    (F.col("registration_date") <= F.current_date())
)
customer_condition = (
    (F.col("customer_id") > 0) &
    F.col("customer_id").isNotNull()
)
silver_customers = silver_customers.withColumn(
    "processed_timestamp",
    F.current_timestamp()
)
print("Email:", silver_customers.filter(email_condition).count())
print("Phone:", silver_customers.filter(phone_condition).count())
print("Customer:", silver_customers.filter(customer_condition).count())
print("Registration:", silver_customers.filter(registration_condition).count())
print("Name:", silver_customers.filter(name_condition).count())
silver_customers = silver_customers.withColumn(
    "source_system",
    F.lit("RetailLake")
)
valid_customers = silver_customers.filter(
    email_condition &
    phone_condition &
    customer_condition &
    registration_condition &
    name_condition
)
valid_customers = valid_customers.withColumn(
    "record_status",
    F.lit("VALID")
)
valid_customers = valid_customers.withColumn(
    "customer_age_days",
    F.datediff(
        F.current_date(),
        F.col("registration_date")
    )
)
valid_customers = valid_customers.withColumn(
    "year",
    F.year("registration_date")
)

valid_customers = valid_customers.withColumn(
    "month",
    F.month("registration_date")
)
valid_customers = valid_customers.withColumn("day", F.dayofmonth("registration_date"))

valid_customers = valid_customers.withColumn(
    "customer_type",
    F.when(F.col("customer_age_days") < 30, "NEW")
     .when(F.col("customer_age_days") < 365, "ACTIVE")
     .otherwise("LOYAL")
)

invalid_customers = silver_customers.filter(
    ~(email_condition &
      phone_condition &
      customer_condition &
      registration_condition &
      name_condition)
)
invalid_customers = invalid_customers.withColumn(
    "record_status",
    F.lit("INVALID")
)
invalid_customers = invalid_customers.withColumn(
    "validation_reason",
    F.when(~email_condition, "Invalid Email")
     .when(~phone_condition, "Invalid Phone")
     .when(~customer_condition, "Invalid Customer ID")
     .when(~registration_condition, "Future Registration Date")
     .when(~name_condition, "Invalid Name")
)
valid_customers.printSchema()
invalid_customers.write \
.mode("append") \
.parquet("s3://retaillake-data/quarantine/customers/")
valid_customers.write \
.mode("append") \
.partitionBy("year","month","day") \
.parquet("s3://retaillake-data/silver/customers/")
total = silver_customers.count()
valid = valid_customers.count()
invalid = invalid_customers.count()
success_rate = round((valid / total) * 100, 2)

print(f"Total Records : {total}")
print(f"Valid Records : {valid}")
print(f"Invalid Records : {invalid}")
print(f"Success Rate : {success_rate}%")

job.commit()