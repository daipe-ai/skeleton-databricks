# Databricks notebook source
dbutils.widgets.dropdown("1. poetry action", choices=["add", "update", "remove"], defaultValue="add")
dbutils.widgets.text("2. package", defaultValue="")
poetry_action = dbutils.widgets.get("1. poetry action")
package = dbutils.widgets.get("2. package")

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install benvy==1.2.2.dev2

# COMMAND ----------

# MAGIC %load_ext benvy.databricks.repos.poetry.magic

# COMMAND ----------

# MAGIC %poetry $poetry_action $package
