# Databricks notebook source
# this notebook should be included (% run) by all other Databricks notebooks to load the Master Package properly

# COMMAND ----------

# MAGIC %install_master_package_whl

# COMMAND ----------

import os

if "APP_ENV" not in os.environ:
    os.environ["APP_ENV"] = "dev"
