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
payments_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://retaillake-data/raw/payments/"],
        "recurse": True
    },
    format="json",
    format_options={
        "multiline": True,
         "jsonPath": "$[*]"
    },
    transformation_ctx="payments_source"
)

print("DynamicFrame count:", payments_dyf.count())

silver_payments = payments_dyf.toDF()
silver_payments= silver_payments.dropDuplicates(["payment_id"])

amount_type = silver_payments.schema["amount"].dataType

if str(amount_type).startswith("StructType"):
    silver_payments = silver_payments.withColumn(
        "amount",
        F.coalesce(
            F.col("amount.double"),
            F.col("amount.int").cast("double")
        )
    )
else:
    silver_payments = silver_payments.withColumn(
        "amount",
        F.col("amount").cast("double")
    )
silver_payments = silver_payments.withColumn("payment_id" , F.col("payment_id").cast("integer"))
silver_payments = silver_payments.withColumn("order_id" , F.col("order_id").cast("integer"))
silver_payments = silver_payments.withColumn("payment_date" , F.to_date(F.col("payment_date")))
silver_payments = silver_payments.withColumn("payment_status" , F.col("payment_status").cast("string"))
silver_payments = silver_payments.withColumn("payment_method" , F.col("payment_method").cast("string"))

payment_methods = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Net Banking",
    "Cash",
    "Wallet"
]

payment_method_condition = silver_payments.payment_method.isNotNull() & (silver_payments.payment_method != "") & (F.col("payment_method").isin(payment_methods))

payment_id_validation = silver_payments.payment_id.isNotNull() & (silver_payments.payment_id > 0) 
payment_statuses = [
    "SUCCESS",
    "FAILED",
    "PENDING",
    "REFUNDED"
]



payment_status_condition = (
    (F.col("payment_status").isNotNull()) & 
    (F.col("payment_status") != "") & 
    (F.col("payment_status").isin(payment_statuses))
)



amount_condition = silver_payments.amount.isNotNull() & (silver_payments.amount > 0) & (silver_payments.amount <= 100000)

orders = spark.read.format("parquet").load("s3://retaillake-data/silver/orders/")

orders_lookup = orders.select(
    ["order_id", "category", "amount", "order_date"]
).withColumnRenamed("amount", "order_amount")
silver_payments = silver_payments.alias("s").join(orders_lookup.alias("o"),
    on="order_id",
    how="left"
)
silver_payments = silver_payments.withColumn("order_exists", F.when(F.col("o.order_id").isNull(), False).otherwise(True))
silver_payments = silver_payments.withColumn(
    "same_amount", 
    F.when(F.col("o.order_amount").isNull() | F.col("s.amount").isNull(), F.lit(None))
     .when(F.col("o.order_amount") != F.col("s.amount"), False)
     .otherwise(True)
)
order_id_condition = (silver_payments.order_exists == True) 
same_amount_condition =(silver_payments.same_amount == True)
payment_date_condition = silver_payments.payment_date.isNotNull() & (F.col("s.payment_date" )>= (F.col("o.order_date")))& (F.col("s.payment_date" ) <= F.current_date())


silver_payments= silver_payments.withColumn("processed_timestamp", F.current_timestamp())
silver_payments= silver_payments.withColumn("source_system", F.lit("RetailLake"))

valid_silver_payments = silver_payments.filter(payment_method_condition & payment_status_condition & amount_condition & order_id_condition & payment_date_condition & payment_id_validation & same_amount_condition )

invalid_silver_payments = silver_payments.filter(~payment_method_condition | ~payment_status_condition | ~amount_condition | ~order_id_condition | ~payment_date_condition | ~payment_id_validation | ~same_amount_condition)

invalid_silver_payments = invalid_silver_payments.withColumn(
    "validation_reason",
    F.when(~payment_method_condition, "Invalid Payment Method").when(~payment_status_condition, "Invalid Payment Status").when(~amount_condition, "Invalid Amount").when(~order_id_condition, "Invalid Order ID").when(~payment_date_condition,"Invalid Payment Date").when(~payment_id_validation,"Invalid Payment ID").when(~same_amount_condition,"Payment Amount Does Not Match Order")
)
payment_digital = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Net Banking",
    "Wallet"
]

valid_silver_payments = valid_silver_payments.withColumn("payment_category", \
    F.when(F.col("payment_method").isin(payment_digital), "Digital").otherwise("Physical")) 
valid_silver_payments = valid_silver_payments.withColumn("record_status", F.lit("VALID"))
invalid_silver_payments = invalid_silver_payments.withColumn("record_status", F.lit("INVALID"))
valid_silver_payments = valid_silver_payments.withColumn("year",F.year("payment_date"))
valid_silver_payments = valid_silver_payments.withColumn("month",F.month("payment_date"))
valid_silver_payments = valid_silver_payments.withColumn("payment_age_days", F.datediff(F.current_date(),F.col("payment_date")))



total = silver_payments.count()
valid = valid_silver_payments.count()
invalid = invalid_silver_payments.count()
success_rate = round((valid / total) * 100, 2)

print(f"Total Records : {total}")
print(f"Valid Records : {valid}")
print(f"Invalid Records : {invalid}")
print(f"Success Rate : {success_rate}%")
valid_silver_payments = valid_silver_payments.drop("order_exists","same_amount")
invalid_silver_payments = invalid_silver_payments.drop("order_exists","same_amount")
valid_silver_payments.write \
.mode("append") \
.partitionBy("year","month") \
.parquet("s3://retaillake-data/silver/payments/")
invalid_silver_payments.write.mode("append").format("parquet").save("s3://retaillake-data/quarantine/payments/")


job.commit()