# Databricks notebook source
from pyspark.sql.functions import lit
customer_schema = "customer_id int,first_name string,last_name string,email string,phone string,country string,city string,registration_date timestamp,date_of_birth timestamp,gender string"
df = spark.read.csv("/mnt/landing/Customer/*.csv",inferSchema=False,schema=customer_schema,header=True)
df_merge_flag = df.withColumn("merge_flag",lit(False))
df_merge_flag.write.mode("append").csv("/mnt/bronzelayer/Customer",header=True)
df_merge_flag.write.mode("append").saveAsTable("bronzelayer.customer")



# COMMAND ----------

from datetime import datetime

current_time = datetime.now().strftime("%m-%d-%Y")
print(current_time)

newfolder = "/mnt/processed/Customer/" + current_time

dbutils.fs.mv("/mnt/landing/Customer", newfolder,True)