# MLflow Recipes Examples
This repository contains example projects for the
[MLflow Recipes](https://mlflow.org/docs/latest/recipes.html) (previously known as MLflow Pipelines).
To learn about specific recipe,
follow the [installation instructions](#installation-instructions) below to install all necessary packages,
then checkout the relevant example projects listed [here](#example-projects).

**Note**: This example repo is intended for first-time MLflow Recipes users to learn
its fundamental concepts and workflows.
For users already familiar with MLflow Recipes, find a template repository
to solve a specific ML problem. For example, for regression problem, use
[recipes-regression-template](https://github.com/mlflow/recipes-regression-template) instead.

**Note**: [MLflow Recipes](https://mlflow.org/docs/latest/recipes.html)
is an experimental feature in [MLflow](https://mlflow.org).
If you observe any issues,
please report them [here](https://github.com/mlflow/mlflow/issues).
For suggestions on improvements,
please file a discussion topic [here](https://github.com/mlflow/mlflow/discussions).
Your contribution to MLflow Recipes is greatly appreciated by the community!

## Example Projects
- [**Regression**: NYC taxi fare prediction](regression/README.md)
- [**Classification**: Credit card fraud detection](classification/README.md)

## Installation instructions
To use MLflow Recipes in this example repository,
simply install the packages listed in the `requirements.txt` file. Note that `Python 3.8` or above is required.
```
pip install -r requirements.txt
```

You may need to install additional libraries for extra features:
- [Hyperopt](https://pypi.org/project/hyperopt/)  is required for hyperparameter tuning.
- [PySpark](https://pypi.org/project/pyspark/)  is required for distributed training or to ingest Spark tables.
- [Delta](https://pypi.org/project/delta-spark/) is required to ingest Delta tables.
These libraries are available natively in the [Databricks Runtime for Machine Learning](https://docs.databricks.com/runtime/mlruntime.html).

## Log to the designated MLflow Experiment
To log recipe runs to a particular MLflow experiment:
1. Open `profiles/databricks.yaml` or `profiles/local.yaml`, depending on your environment.
2. Edit (and uncomment, if necessary) the `experiment` section, specifying the name of the
   desired experiment for logging.

## Development Environment -- Databricks
[Sync](https://docs.databricks.com/repos.html) this repository with
[Databricks Repos](https://docs.databricks.com/repos.html) and run the `notebooks/databricks`
notebook on a Databricks Cluster running version 11.0 or greater of the
[Databricks Runtime](https://docs.databricks.com/runtime/dbr.html) or the
[Databricks Runtime for Machine Learning](https://docs.databricks.com/runtime/mlruntime.html)
with [workspace files support enabled](https://docs.databricks.com/repos.html#work-with-non-notebook-files-in-a-databricks-repo).

**Note**: When making changes to recipes on Databricks,
it is recommended that you edit files on your local machine and
use [dbx](https://docs.databricks.com/dev-tools/dbx.html) to sync them to Databricks Repos, as
demonstrated [here](https://mlflow.org/docs/latest/recipes.html#usage)

**Note**: data profiles display in step cards are not visually compatible with dark theme.
Please avoid using the dark theme if possible.

### Accessing MLflow recipe Runs
You can find MLflow Experiments and MLflow Runs created by the recipe on the
[Databricks ML Experiments page](https://docs.databricks.com/applications/machine-learning/experiments-page.html#experiments).

## Development Environment -- Local machine
### Jupyter

1. Launch the Jupyter Notebook environment via the `jupyter notebook` command.
2. Open and run the `notebooks/jupyter.ipynb` notebook in the Jupyter environment.

**Note**: data profiles display in step cards are not visually compatible with dark theme.
Please avoid using the dark theme if possible.

### Command-Line Interface (CLI)

First, enter the corresponding example root directory and set the profile via environment variable.
For example, for the regression example project,
```
cd regression
```
```
export MLFLOW_RECIPES_PROFILE=local
```

Then, try running the
following [MLflow Recipes CLI](https://mlflow.org/docs/latest/cli.html#mlflow-recipes)
commands to get started.
Note that the `--step` argument is optional.
Recipe commands without a `--step` specified act on the entire recipe instead.

Available step names are: `ingest`, `split`, `transform`, `train`, `evaluate` and `register`.

- Display the help message:
```
mlflow recipes --help
```

- Run a recipe step or the entire recipe:
```
mlflow recipes run --step step_name
```

- Inspect a step card or the recipe dependency graph:
```
mlflow recipes inspect --step step_name
```

- Clean a step cache or all step caches:
```
mlflow recipes clean --step step_name
```

### Accessing MLflow Recipe Runs
To view MLflow Experiments and MLflow Runs created by the recipe:

1. Enter the example root directory, for example: `cd regression`

2. Start the MLflow UI

```sh
mlflow ui \
   --backend-store-uri sqlite:///metadata/mlflow/mlruns.db \
   --default-artifact-root ./metadata/mlflow/mlartifacts \
   --host localhost
```
