# Databricks notebook source
# MAGIC %run ../app/bootstrap

# COMMAND ----------

import daipe as dp
from box import Box

# COMMAND ----------

@dp.notebook_function()
def init_widgets(widgets_factory: dp.fs.WidgetsFactory):
    widgets_factory.create()
    widgets_factory.create_for_notebooks()

# COMMAND ----------

@dp.notebook_function(dp.fs.get_stages())
def orchestrate(stages: Box, orchestrator: dp.fs.DatabricksOrchestrator):
    orchestrator.orchestrate(stages)
