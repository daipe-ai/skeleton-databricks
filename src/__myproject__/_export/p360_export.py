# Databricks notebook source
# MAGIC %run ../app/bootstrap

# COMMAND ----------

import json
import daipe as dp
from p360_export.P360ExportRunner import P360ExportRunner
from typing import Dict, Any

# COMMAND ----------

@dp.notebook_function()
def init_widgets(widgets: dp.Widgets):
    widgets.add_text("config", "", "Config")

# COMMAND ----------

@dp.notebook_function(dp.get_widget_value("config"))
def export(config: Dict[str, Any], p360_export_runner: P360ExportRunner):
    p360_export_runner.export(json.loads(config))
