# MLflow Pipelines Regression Template
This repository serves as a customizable template for the
[MLflow Regression Pipeline](https://mlflow.org/docs/latest/pipelines.html#regression-pipeline)
to develop high-quality production-ready regression models.

Currently supported ML models are limited to scikit-learn and frameworks that
integrate with scikit-learn, such as the ``XGBRegressor`` API from XGBoost.

**Note**: [MLflow Pipelines](https://mlflow.org/docs/latest/pipelines.html)
is an experimental feature in [MLflow](https://mlflow.org).
If you observe any issues,
please report them [here](https://github.com/mlflow/mlflow/issues).
For suggestions on improvements,
please file a discussion topic [here](https://github.com/mlflow/mlflow/discussions).
Your contribution to MLflow Pipelines is greatly appreciated by the community!

## Installation instructions
(Optional) Create a clean Python environment either via 
[virtualenv](https://pypi.org/project/virtualenv) or 
[conda](https://pypi.org/project/conda) for the best experience.
Python 3.7 or higher is required.

1. Install the latest MLflow with Pipelines:
```
pip install "mlflow[pipelines]"
```

2. Clone this MLflow Regression Pipeline template repository locally:
```
git clone https://github.com/mlflow/mlp-regression-template.git
```

3. Enter the root directory of the cloned pipeline template:
```
cd mlp-regression-template
```

4. Install the template dependencies:
```
pip install -r requirements.txt
```

## Log to the designated MLflow Experiment
To log pipeline runs to a particular MLflow experiment:
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

**Note**: When making changes to pipelines on Databricks,
it is recommended that you either edit files on your local machine and
use [dbx](https://docs.databricks.com/dev-tools/dbx.html) to sync them to Databricks Repos, as
demonstrated [here](https://mlflow.org/docs/latest/pipelines.html#usage), or edit files in
Databricks Repos by opening separate browser tabs for each YAML file or Python code module that you
wish to modify.

For the latter approach, we recommend opening at least **3 browser tabs** to
facilitate easier development:
- One tab for modifying configurations in `pipeline.yaml` and / or `profiles/{profile}.yaml`
- One tab for modifying step function(s) defined in `steps/{step}.py`
- One tab for modifying and running the driver notebook (`notebooks/databricks`)

### Accessing MLflow Pipeline Runs
You can find MLflow Experiments and MLflow Runs created by the pipeline on the
[Databricks ML Experiments page](https://docs.databricks.com/applications/machine-learning/experiments-page.html#experiments).

## Development Environment -- Local machine
### Jupyter

1. Launch the Jupyter Notebook environment via the `jupyter notebook` command.
2. Open and run the `notebooks/jupyter.ipynb` notebook in the Jupyter environment.

### Command-Line Interface (CLI)

First, enter the template root directory and set the profile via environment variable
```
cd mlp-regression-template
```
```
export MLFLOW_PIPELINES_PROFILE=local
```

Then, try running the
following [MLflow Pipelines CLI](https://mlflow.org/docs/latest/cli.html#mlflow-pipelines)
commands to get started. 
Note that the `--step` argument is optional.
Pipeline commands without a `--step` specified act on the entire pipeline instead.

Available step names are: `ingest`, `split`, `transform`, `train`, `evaluate` and `register`.

- Display the help message:
```
mlflow pipelines --help
```

- Run a pipeline step or the entire pipeline:
```
mlflow pipelines run --step step_name
```

- Inspect a step card or the pipeline dependency graph:
```
mlflow pipelines inspect --step step_name
```

- Clean a step cache or all step caches:
```
mlflow pipelines clean --step step_name
```

**Note**: a short cut to `mlflow pipelines` is installed as `mlp`.
For example, to run the ingest step,
instead of issuing `mlflow pipelines run --step ingest`, you may type
```
mlp -s ingest
```

### Accessing MLflow Pipeline Runs
To view MLflow Experiments and MLflow Runs created by the pipeline:

1. Enter the template root directory: `cd mlp-regression-template`

2. Start the MLflow UI

```sh
mlflow ui \
   --backend-store-uri sqlite:///metadata/mlflow/mlruns.db \
   --default-artifact-root ./metadata/mlflow/mlartifacts \
   --host localhost
```

3. Open a browser tab pointing to [http://127.0.0.1:5000](http://127.0.0.1:5000)
