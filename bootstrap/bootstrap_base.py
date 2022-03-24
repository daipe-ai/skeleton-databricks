# Databricks notebook source
# MAGIC %run ./install_benvy

# COMMAND ----------

from benvy.databricks.repos import bootstrap
from benvy.databricks.detector import is_databricks_repo

bootstrap.install()

# COMMAND ----------

from benvy.databricks.repos import bootstrap
from benvy.databricks.detector import is_databricks_repo

bootstrap.setup_env()

# COMMAND ----------

import os
import colorlog

if "APP_ENV" not in os.environ:
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s%(levelname)s: %(message)s'))
    logger = colorlog.getLogger('example')
    logger.addHandler(handler)
    
    logger.warning("Using default DEV environment. Please set the APP_ENV variable on cluster and delete this cell.")
    os.environ["APP_ENV"] = "dev"
