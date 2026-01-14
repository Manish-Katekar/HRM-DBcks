# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS `dev-catalog`.`gold`.`dim_cptcodes`
# MAGIC (
# MAGIC cpt_codes string,
# MAGIC procedure_code_category string,
# MAGIC procedure_code_descriptions string,
# MAGIC code_status string,
# MAGIC refreshed_at timestamp
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC TRUNCATE TABLE `dev-catalog`.`gold`.`dim_cptcodes`

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into `dev-catalog`.`gold`.`dim_cptcodes`
# MAGIC select 
# MAGIC cpt_codes,
# MAGIC procedure_code_category,
# MAGIC procedure_code_descriptions ,
# MAGIC code_status,
# MAGIC current_timestamp() as refreshed_at
# MAGIC  from `dev-catalog`.`silver`.`cptcodes`
# MAGIC  where is_quarantined=false and is_current=true
