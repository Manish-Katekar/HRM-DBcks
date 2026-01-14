# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

landing = spark.sql("DESCRIBE EXTERNAL LOCATION `datalake-hrm-landing`").select("url").collect()[0][0]

claims_df=spark.read.csv(landing+"/claims_data/*.csv",header=True)

claims_df = claims_df.withColumn(
    "datasource",
    f.when(f.col("_metadata.file_path").contains("hospital1"), "hosa")
    .when(f.col("_metadata.file_path").contains("hospital2"), "hosb")
     .otherwise(None)
)


# COMMAND ----------

# DBTITLE 1,Parquet file creation
claims_df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("mergeschema", "true") \
  .saveAsTable("`dev-catalog`.`bronze`.`claims`")
