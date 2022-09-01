# Databricks notebook source
# MAGIC %run ../app/bootstrap

# COMMAND ----------

import daipe as dp
from p360_interface_bundle.export.ExportRunner import ExportRunner

# COMMAND ----------

@dp.notebook_function()
def init_widgets(widgets: dp.Widgets):
    widgets.add_text("config", "", "Config")

# COMMAND ----------

@dp.notebook_function()
def export(export_runner: ExportRunner):
    export_runner.run()
