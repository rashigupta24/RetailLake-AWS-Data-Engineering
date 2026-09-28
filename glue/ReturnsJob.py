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
returns_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://retaillake-data/raw/returns/"],
        "recurse": True
    },
    format="json",
    format_options={
        "multiline": True,
         "jsonPath": "$[*]"
    },
    transformation_ctx="returns_source"
)

print("DynamicFrame count:", returns_dyf.count())

silver_returns = returns_dyf.toDF()
silver_returns = silver_returns.dropDuplicates(["return_id"])
silver_returns = silver_returns.withColumn("return_id", F.col("return_id").cast("integer"))
silver_returns = silver_returns.withColumn("order_id", F.col("order_id").cast("integer"))
silver_returns = silver_returns.withColumn("return_date", F.to_date("return_date"))

return_reasons = [
    "Damaged",
    "Wrong Item",
    "Quality Issue",
    "Late Delivery",
    "Customer Changed Mind",
    "Defective"
]


return_reason_condition = (silver_returns.reason.isNotNull()) & (silver_returns.reason.isin(return_reasons)) &  (silver_returns.reason != "")

orders = spark.read.format("parquet").load("s3://retaillake-data/silver/orders/")
orders_lookup = orders.select(["order_id","category","amount","order_date"]).withColumnRenamed("order_id","order_id_lookup").withColumnRenamed(
    "order_date", "original_order_date"
)
silver_returns = silver_returns.join(orders_lookup,silver_returns["order_id"] == orders_lookup["order_id_lookup"],how="left")
order_id_condition = F.col("order_id_lookup").isNotNull()

return_id_condition = silver_returns.return_id.isNotNull() & (silver_returns.return_id > 0)

return_date_condition = silver_returns.return_date.isNotNull()  & (F.col("return_date") <=  F.current_date()) & (F.col("return_date") >= F.col("original_order_date"))

silver_returns = silver_returns.withColumn("processed_timestamp", F.current_timestamp())
silver_returns = silver_returns.withColumn("source_system", F.lit("RetailLake"))

print("Total:", silver_returns.count())

print("Valid reasons:",
      silver_returns.filter(return_reason_condition).count())

print("Valid return IDs:",
      silver_returns.filter(return_id_condition).count())

print("Valid return dates:",
      silver_returns.filter(return_date_condition).count())

print("Valid order IDs:",silver_returns.filter(order_id_condition).count())
silver_returns.select("order_id","order_id_lookup","return_date","original_order_date").show(20, False)

valid_silver_returns = silver_returns.filter(return_reason_condition & return_id_condition & return_date_condition & order_id_condition)
valid_silver_returns = valid_silver_returns.drop("order_id_lookup","category","amount","original_order_date")
valid_silver_returns = valid_silver_returns.withColumn("record_status", F.lit("VALID"))
valid_silver_returns = valid_silver_returns.withColumn("year",F.year("return_date"))
valid_silver_returns = valid_silver_returns.withColumn("month",F.month("return_date"))
valid_silver_returns = valid_silver_returns.withColumn("return_age_days", F.datediff(F.current_date(),F.col("return_date")))



invalid_silver_returns = silver_returns.filter(~return_reason_condition | ~ return_id_condition | ~return_date_condition | ~ order_id_condition)

invalid_silver_returns = invalid_silver_returns.withColumn("record_status", F.lit("INVALID"))
invalid_silver_returns = invalid_silver_returns.withColumn("validation_reason", F.when(~return_reason_condition, "Return Reason is not valid").when(~return_id_condition, "Return ID is not valid").when(~return_date_condition, "Return Date is not valid").when(~order_id_condition, "Order ID is not valid"))

total = silver_returns.count()
valid = valid_silver_returns.count()
invalid = invalid_silver_returns.count()
success_rate = round((valid / total) * 100, 2)
invalid_silver_returns = invalid_silver_returns.drop("order_id_lookup","category","amount","original_order_date")
print(f"Total Records : {total}")
print(f"Valid Records : {valid}")
print(f"Invalid Records : {invalid}")
print(f"Success Rate : {success_rate}%")
invalid_silver_returns.write.mode("append").format("parquet").save("s3://retaillake-data/quarantine/returns/")
valid_silver_returns.write.mode("append").format("parquet").partitionBy("year","month").save("s3://retaillake-data/silver/returns/")
job.commit()