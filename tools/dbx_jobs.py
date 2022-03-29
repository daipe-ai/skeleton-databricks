# Databricks notebook source
# MAGIC %md
# MAGIC <a href="$../index">Back to index</a>

# COMMAND ----------

# MAGIC %pip install jobs-bundle==0.5.2 &> /dev/null

# COMMAND ----------

# MAGIC %run ../bootstrap/bootstrap_base

# COMMAND ----------

import os
from argparse import Namespace
from jobsbundle.job.JobCreateOrUpdateCommand import JobCreateOrUpdateCommand
from daipecore.imports import Widgets, get_widget_value
from datalakebundle.imports import notebook_function

os.environ["DBX_TOKEN"] = str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())

# COMMAND ----------

@notebook_function("%jobsbundle.jobs%")
def create_widget(jobs, widgets: Widgets):
    widgets.add_select("job", ["<all>"] + list(jobs), "<all>")

# COMMAND ----------

def create_or_update_job(command: JobCreateOrUpdateCommand, identifier: str):
    cwd_parts = os.getcwd().split("/")

    command.run(
        Namespace(identifier=identifier, user_folder_name=cwd_parts[3], dir_name=cwd_parts[4],)
    )

# COMMAND ----------

@notebook_function(get_widget_value("job"), "%jobsbundle.jobs%")
def create_job(job: str, jobs, command: JobCreateOrUpdateCommand):
    if job == "<all>":
        for job in jobs:
            create_or_update_job(command, job)
    else:
        create_or_update_job(command, job)
