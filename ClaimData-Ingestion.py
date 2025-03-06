# Databricks notebook source
from pyspark.sql.functions import lit
claim_schema = "claim_id int, policy_id int, date_of_claim timestamp, claim_amount double, claim_status string, LastUpdatedTimeStamp timestamp"
df = spark.read.parquet("/mnt/landing/ClaimData/*.parquet",inferSchema=False, schema=claim_schema)
df_with_flag = df.withColumn("merge_flag", lit(False))
df_with_flag.write.mode("append").parquet("/mnt/bronzelayer/Claim")
df_with_flag.write.mode("append").saveAsTable("bronzelayer.Claim")

# COMMAND ----------

from datetime import datetime

current_time = datetime.now().strftime("%m-%d-%Y")

print(current_time)

new_folder = "/mnt/processed/ClaimData/" + current_time

dbutils.fs.mv("/mnt/landing/ClaimData", new_folder,True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.claim
# MAGIC