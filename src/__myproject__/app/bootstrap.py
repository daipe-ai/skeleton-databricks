# Databricks notebook source
# MAGIC %sh
# MAGIC pip install benvy==1.2.1

# COMMAND ----------

from benvy.databricks.repos import bootstrap  # noqa
from benvy.databricks.detector import is_databricks_repo  # noqa

if is_databricks_repo():
    bootstrap.install()

# COMMAND ----------

from benvy.databricks.repos import bootstrap  # noqa
from benvy.databricks.detector import is_databricks_repo  # noqa

if is_databricks_repo():
    bootstrap.setup_env()

# COMMAND ----------

# %install_master_package_whl

# COMMAND ----------

import os  # noqa

if "APP_ENV" not in os.environ:
    os.environ["APP_ENV"] = "dev"

if "DAIPE_BOOTSTRAPPED" not in os.environ:
    os.environ["DAIPE_BOOTSTRAPPED"] = "1"
