# Databricks notebook source
# MAGIC %run ../app/bootstrap

# COMMAND ----------

import daipe as dp
from p360_export.P360ExportRunner import P360ExportRunner

# COMMAND ----------

@dp.notebook_function()
def init_widgets(widgets: dp.Widgets):
    widgets.add_text("config_url", "", "Config url")

# COMMAND ----------

@dp.notebook_function(dp.get_widget_value("config_url"))
def export(config_url: str, p360_export_runner: P360ExportRunner):
    p360_export_runner.export(config_url=config_url)
