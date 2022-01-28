# Databricks notebook source
# MAGIC %run ../bootstrap/install_benvy

# COMMAND ----------

from benvy.databricks.repos import bootstrap
from benvy.databricks.detector import is_databricks_repo

if is_databricks_repo():
    bootstrap.install_dev()

# COMMAND ----------

from benvy.databricks.repos import bootstrap
from benvy.databricks.detector import is_databricks_repo

if is_databricks_repo():
    bootstrap.setup_env()

# COMMAND ----------

# MAGIC %load_ext benvy.databricks.repos.pylint.magic

# COMMAND ----------

# MAGIC %pylint src/
