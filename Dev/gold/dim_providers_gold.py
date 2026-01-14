# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS `dev-catalog`.`gold`.`dim_providers`
# MAGIC (
# MAGIC ProviderID string,
# MAGIC FirstName string,
# MAGIC LastName string,
# MAGIC DeptID string,
# MAGIC NPI long,
# MAGIC datasource string
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC truncate TABLE `dev-catalog`.`gold`.`dim_providers`

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into `dev-catalog`.`gold`.`dim_providers`
# MAGIC select 
# MAGIC ProviderID ,
# MAGIC FirstName ,
# MAGIC LastName ,
# MAGIC concat(DeptID,'-',datasource) deptid,
# MAGIC NPI ,
# MAGIC datasource 
# MAGIC  from `dev-catalog`.`silver`.`providers`
# MAGIC  where is_quarantined=false
