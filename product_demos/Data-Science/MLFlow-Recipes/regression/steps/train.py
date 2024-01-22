"""
Show resolved
This module defines the following routines used by the 'train' step of the regression recipe:
- ``estimator_fn``: Defines the customizable estimator type and parameters that are used
  during training to produce a model recipe.
"""
from typing import Dict, Any

'''
def estimator_fn(estimator_params: Dict[str, Any] = None):
    """
    Returns an *unfitted* estimator that defines ``fit()`` and ``predict()`` methods.
    The estimator's input and output signatures should be compatible with scikit-learn
    estimators.
    """
    if estimator_params is None:
        estimator_params = {}
    from sklearn.linear_model import SGDRegressor

    return SGDRegressor(random_state=42, **estimator_params)
'''
def estimator_fn():
    """
    Returns an *unfitted* estimator that defines ``fit()`` and ``predict()`` methods.
    The estimator's input and output signatures should be compatible with scikit-learn
    estimators.
    """
    from xgboost import XGBRegressor
    #import mlflow
    #mlflow.set_tag("field_demos", "mlflow_pipeline")
    return XGBRegressor(objective = 'reg:squarederror',
        colsample_bytree = 0.5,
        learning_rate = 0.05,
        max_depth = 6,
        min_child_weight = 1,
        n_estimators = 1000,
        subsample = 0.7)