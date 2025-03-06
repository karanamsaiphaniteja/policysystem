# Databricks notebook source
from pyspark.sql.functions import lit
branch_schema = "branch_id int, branch_country string, branch_city string"
df = spark.read.parquet("/mnt/landing/BranchData/*.parquet",inferSchema=False,schema=branch_schema)
df_merge_flag = df.withColumn("merge_flag",lit(False))
df_merge_flag.write.mode("append").parquet("/mnt/bronzelayer/Branch")
df_merge_flag.write.mode("append").saveAsTable("bronzelayer.branch")   


# COMMAND ----------

from datetime import datetime

#get the current time in mm-dd-yyyy format
current_time = datetime.now().strftime("%m-%d-%Y")

print(current_time)

new_folder = "/mnt/processed/BranchData/" + current_time

dbutils.fs.mv("/mnt/landing/BranchData/", new_folder,True)