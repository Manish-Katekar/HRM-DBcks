# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

landing = spark.sql("DESCRIBE EXTERNAL LOCATION `datalake-hrm-landing`").select("url").collect()[0][0]

cptcodes_df =spark.read.csv(landing+"/cptcodes/*.csv",header=True)

# Replace whitespaces in column names with underscores and convert to lowercase
for col in cptcodes_df.columns:
    new_col = col.replace(" ", "_").lower()
    cptcodes_df = cptcodes_df.withColumnRenamed(col, new_col)


# COMMAND ----------

cptcodes_df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("mergeschema", "true") \
  .saveAsTable("`dev-catalog`.`bronze`.`cptcodes`")
