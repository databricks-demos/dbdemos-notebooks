# Databricks notebook source
# MAGIC %pip install category_encoders==2.6.0

# COMMAND ----------

from sklearn.base import TransformerMixin, BaseEstimator
from scipy import sparse
from category_encoders import one_hot

from abc import ABC
from typing import List, Union

import pandas as pd
import mlflow

# COMMAND ----------

# MAGIC %md 
# MAGIC AutoML classification with serverless coming soon. Until then, this notebook will ensure that the demo still works with serverless. 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Workarounds for autoML to create a mock run
# creates a mock run simulating automl results
def create_mockup_automl_run_for_dbdemos(full_xp_path, df):
    print('Creating mockup automl run...')
    xp = mlflow.create_experiment(full_xp_path)
    mlflow.set_experiment(experiment_id=xp)
    
    with mlflow.start_run(run_name="DBDemos automl mock autoML run", experiment_id=xp) as run:
        mlflow.set_tag('mlflow.source.name', 'Notebook: DataExploration')
        mlflow.log_metric('val_f1_score', 0.81)
        split_choices = ['train', 'val', 'test']
        split_probabilities = [0.7, 0.2, 0.1]  # 70% train, 20% val, 10% test
        # Add a new column with random assignments
        import numpy as np
        df['_automl_split_col'] = np.random.choice(split_choices, size=len(df), p=split_probabilities)
        df.to_parquet('/tmp/dataset.parquet', index=False)
        mlflow.log_artifact('/tmp/dataset.parquet', artifact_path='data/training_data')

# COMMAND ----------

# directly taken from source code of databricks/automl
# source: https://github.com/databricks/automl/blob/main/runtime/databricks/automl_runtime/sklearn/column_selector.py
class ColumnSelector(ABC, TransformerMixin, BaseEstimator):
    """
    Transformer to select specific columns from a dataset

    Parameters
    ----------
    cols: A list specifying the feature column names to be selected.
    """

    def __init__(self, cols: Union[List[str], str]) -> None:
        if not isinstance(cols, list):
            self.cols = [cols]
        else:
            self.cols = cols

    def fit(self, X: pd.DataFrame, y: pd.Series):
        """
        Do nothing and return the estimator unchanged.
        This method is just there to implement the usual API and hence work in pipelines.
        """
        return self

    def transform(self, X:pd.DataFrame) -> pd.DataFrame:
        """
        Select the chosen columns
        :param X: pd.DataFrame of shape (n_samples, n_features)
            n_features is the number of feature columns.
        :return: pd.DataFrame of shape (n_samples, k_features)
            Subset of the feature space where k_features <= n_features.
        """
        X = X.copy()
        return X[self.cols]

    def get_feature_names_out(self, input_features=None):
        return self.cols

# COMMAND ----------

# source: https://github.com/databricks/automl/blob/main/runtime/databricks/automl_runtime/sklearn/one_hot_encoder.py
class OneHotEncoder(TransformerMixin, BaseEstimator):
    """
    A wrapper around the category_encoder's `OneHotEncoder` with additional support for sparse output.
    """

    def __init__(self, sparse=True, **kwargs):
        """Creates a wrapper around category_encoder's `OneHotEncoder`

        Parameters
        ----------
        Same parameters as category_encoder's OneHotEncoder, but with sparse as a new addition

        sparse: boolean describing whether you want a sparse outut or not
        """
        self.sparse = sparse
        self.base_one_hot_encoder = one_hot.OneHotEncoder(**kwargs)

    def fit_transform(self, X, y=None, **fit_params):
        """Fits the encoder according to X and y (and additional fit_params) and then transforms X

        Parameters
        ----------

        X : pd.DataFrame of shape = [n_samples, n_features]
            Training dataframe, where n_samples is the number of samples
            and n_features is the number of features.
        y : array-like, shape = [n_samples]
            Target values.

        Returns
        -------

        X_tr : pd.DataFrame of shape (n_samples, n_features_encoded)
            Transformed features.
        """
        self.base_one_hot_encoder.fit(X, y, **fit_params)
        X_updated = self.base_one_hot_encoder.transform(X)
        if self.sparse:
            X_updated = sparse.csr_matrix(X_updated)
        return X_updated

    def fit(self, X, y=None, **kwargs):
        """Fits the encoder according to X and y (and additional fit_params)

        Parameters
        ----------

        X : pd.DataFrame of shape = [n_samples, n_features]
            Training dataframe, where n_samples is the number of samples
            and n_features is the number of features.
        y : array-like, shape = [n_samples]
            Target values.
        """
        self.base_one_hot_encoder.fit(X, y, **kwargs)

    def transform(self, X):
        """Transforms the input by adding additional columns for OneHotEncoding

        Parameters
        ----------

        X : pd.DataFrame of shape = [n_samples, n_features]
            Training dataframe, where n_samples is the number of samples
            and n_features is the number of features.

        Returns
        -------

        X_tr : pd.DataFrame of shape (n_samples, n_features_encoded)
            Transformed features.
        """
        X_updated = self.base_one_hot_encoder.transform(X)
        if self.sparse:
            X_updated = sparse.csr_matrix(X_updated)
        return X_updated