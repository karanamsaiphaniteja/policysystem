# Databricks notebook source
from pyspark.sql.functions import lit
policy_schema = "policy_id int,policy_type string,customer_id int,start_date timestamp,end_date timestamp,premium double,coverage_amount double"
df = spark.read.json("/mnt/landing/Policydata",schema=policy_schema)
df_merge_flag = df.withColumn("merge_flag",lit(False))
df_merge_flag.write.mode("append").json("/mnt/bronzelayer/PolicyData")
df_merge_flag.write.mode("append").saveAsTable("bronzelayer.policy")

# COMMAND ----------

from datetime import datetime

def getFilePathWithDates(filePath):
    current_time = datetime.now().strftime("%m-%d-%Y")
    new_filepath = filePath +'/'+current_time
    return new_filepath

dbutils.fs.mv("/mnt/landing/Policydata",getFilePathWithDates("/mnt/processed/PolicyData"),True)