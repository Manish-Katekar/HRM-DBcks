# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS `dev-catalog`.`gold`.`dim_departments`
# MAGIC (
# MAGIC Dept_Id string,
# MAGIC SRC_Dept_Id string,
# MAGIC Name string,
# MAGIC datasource string
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC TRUNCATE TABLE `dev-catalog`.`gold`.`dim_departments`

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into `dev-catalog`.`gold`.`dim_departments`
# MAGIC select 
# MAGIC distinct
# MAGIC Dept_Id ,
# MAGIC SRC_Dept_Id ,
# MAGIC Name ,
# MAGIC datasource 
# MAGIC  from `dev-catalog`.`silver`.`departments`
# MAGIC  where is_quarantined=false
