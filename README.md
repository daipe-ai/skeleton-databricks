# Daipe Framework project template

## 1. What's inside

This is a Daipe project skeleton covering our **best practices for Databricks projects**. Daipe is focused on the following paradigms:

* anyone with basic python skills can create pipelines and improve the business logic,
* developing a standard DataLake project requires almost no engineers,
* one code for all environments (your favorite IDE + Databricks UI),
* pursue consistency as the project grows.

**Base components** to be used by everyone:

1. Configuration in YAML
1. Tables & schema management
1. Automated deployment to Databricks
1. Documentation automation

**Advanced components** to be used mostly by engineers:

1. Production releases workflow
1. Unit & pipeline testing
1. Extensions API

## 2. Local environment setup

The following software needs to be installed first:
  * [Miniconda package manager](https://docs.conda.io/en/latest/miniconda.html)
  * [Git for Windows](https://git-scm.com/download/win) or standard Git in Linux (_apt-get install git_)
  
We recommend using the following IDEs:  
  * [PyCharm Community or Pro](https://www.jetbrains.com/pycharm/download/) with the [EnvFile plugin](https://plugins.jetbrains.com/plugin/7861-envfile) installed
  * [Visual Studio Code](https://code.visualstudio.com/download) with the [PYTHONPATH setter extension](https://marketplace.visualstudio.com/items?itemName=datasentics.pythonpath-setter) installed

## 3. Create your first Daipe-powered project

* On **Windows**, use Git Bash.
* On **Linux/Mac**, the use standard terminal 

```
# check documentation on https://github.com/daipe-ai/project-creator

source <(curl -s https://raw.githubusercontent.com/daipe-ai/project-creator/master/create_project.sh)
```

When the environment setup is completed, [configure your Databricks cluster connection details](https://docs.databricks.com/dev-tools/databricks-connect.html#step-2-configure-connection-properties):

Update *src/[ROOT_MODULE]/_config/config_dev.yaml* with your Databricks `address`, `clusterId` and `orgId` (Azure only).

![](docs/config_dev.png)

Add your Databricks token to the `[PROJECT_ROOT]/.env` file

![](docs/dotenv.png)

## 4. Activate your project environment

Now activate the Conda environment for your new project:

```bash
$ conda activate $PWD/.venv
```

or use a shortcut

```bash
$ ca
```

## 5. Important scripts

1. ```poe flake8``` - checks coding standards
1. ```poe container-check``` - check app container consistency (if configured properly)
