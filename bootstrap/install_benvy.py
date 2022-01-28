# Databricks notebook source
import os
import IPython

PENVY_VERSION = "1.2.3"
BENVY_VERSION = "1.3.2"

command = f"pip install penvy=={PENVY_VERSION} benvy=={BENVY_VERSION}"

if "DAIPE_DEPENDENCIES_DIR" in os.environ:
    command += f" --no-index --find-links {os.environ['DAIPE_DEPENDENCIES_DIR']}"

output_lines = IPython.get_ipython().run_line_magic("system", command)

print("\n".join(output_lines))
