# Databricks notebook source
# MAGIC %sh
# MAGIC pip install benvy==1.2.2b1

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
import IPython  # noqa

if "APP_ENV" not in os.environ:
    os.environ["APP_ENV"] = "dev"

if "DAIPE_BOOTSTRAPPED" not in os.environ:
    os.environ["DAIPE_BOOTSTRAPPED"] = "1"

if os.environ["APP_ENV"] == "dev":
    IPython.get_ipython().run_line_magic("load_ext", "autoreload")
    IPython.get_ipython().run_line_magic("autoreload", "2")

# COMMAND ----------

from daipecore.decorator.ContainerManager import ContainerManager  # noqa
from daipecore.bootstrap.container_factory import create_container  # noqa

container = create_container(os.environ["APP_ENV"])
ContainerManager.set_container(container)
