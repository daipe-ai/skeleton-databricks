# Databricks notebook source
# MAGIC %md
# MAGIC <a href="$../index">Back to index</a>

# COMMAND ----------

# MAGIC %run ../bootstrap/install_benvy

# COMMAND ----------

dbutils.widgets.dropdown("1. poetry action", choices=["add", "update", "remove"], defaultValue="add")
dbutils.widgets.text("2. package", defaultValue="")
poetry_action = dbutils.widgets.get("1. poetry action")
package = dbutils.widgets.get("2. package")

# COMMAND ----------

if not package:
    dbutils.notebook.exit(0)

# COMMAND ----------

# MAGIC %load_ext benvy.databricks.repos.poetry.magic

# COMMAND ----------

# MAGIC %poetry $poetry_action $package
