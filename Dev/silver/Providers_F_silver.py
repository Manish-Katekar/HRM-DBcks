# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

bronze = spark.sql("DESCRIBE EXTERNAL LOCATION `datalake-hrm-bronze`").select("url").collect()[0][0]

#Reading Hospital A departments data 
df_hosa=spark.read.parquet(bronze+"/hosa/providers")

#Reading Hospital B departments data 
df_hosb=spark.read.parquet(bronze+"/hosb/providers")

#union two departments dataframes
df_merged = df_hosa.unionByName(df_hosb)
display(df_merged)

df_merged.createOrReplaceTempView("providers")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS `dev-catalog`.`silver`.`providers` (
# MAGIC ProviderID string,
# MAGIC FirstName string,
# MAGIC LastName string,
# MAGIC Specialization string,
# MAGIC DeptID string,
# MAGIC NPI long,
# MAGIC datasource string,
# MAGIC is_quarantined boolean
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table `dev-catalog`.`silver`.`providers` 

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into `dev-catalog`.`silver`.`providers` 
# MAGIC select 
# MAGIC distinct
# MAGIC ProviderID,
# MAGIC FirstName,
# MAGIC LastName,
# MAGIC Specialization,
# MAGIC DeptID,
# MAGIC Cast(NPI as BIGINT) NPI,
# MAGIC datasource,
# MAGIC     CASE 
# MAGIC         WHEN ProviderID IS NULL OR DeptID IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC from providers
