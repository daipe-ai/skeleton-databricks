# Databricks notebook source
# MAGIC %run ../../../bootstrap/bootstrap_base

# COMMAND ----------

import os
import IPython

# pylint: disable=wrong-import-position

if "DAIPE_BOOTSTRAPPED" not in os.environ:
    os.environ["DAIPE_BOOTSTRAPPED"] = "1"

if os.environ["APP_ENV"] == "dev":
    IPython.get_ipython().run_line_magic("load_ext", "autoreload")
    IPython.get_ipython().run_line_magic("autoreload", "2")

# COMMAND ----------

from daipecore.decorator.ContainerManager import ContainerManager
from daipecore.bootstrap.container_factory import create_container

def init_container():
    ContainerManager.set_container(create_container(os.environ["APP_ENV"]))

init_container()

# COMMAND ----------

from databricksbundle.notebook.helpers import get_notebook_path
from daipecore.container import container_refresher

if os.environ["APP_ENV"] == "dev" and "watcher_thread" not in globals():
    configs_dir = "/Workspace" + "/".join(get_notebook_path().split("/")[:6]) + "/_config"

    watcher_logger, watcher_thread = container_refresher.watch_configs(
        configs_dir, init_container
    )
    watcher_logger.print()
