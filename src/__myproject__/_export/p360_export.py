# Databricks notebook source
# MAGIC %pip install p360_export

# COMMAND ----------

from p360_export.ExportRunner import export

# COMMAND ----------

dbutils.widgets.text("config", "", "Config")

# COMMAND ----------

export(dbutils.widgets.get("config"))
