"""
This module defines the following routines used by the 'transform' step of the regression recipe:

- ``transformer_fn``: Defines customizable logic for transforming input data before it is passed
  to the estimator during model inference.
"""

from pandas import DataFrame
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler, FunctionTransformer

'''
def calculate_features(df: DataFrame):
    """
    Extend the input dataframe with pickup day of week and hour, and trip duration.
    Drop the now-unneeded pickup datetime and dropoff datetime columns.
    """
    df["pickup_dow"] = df["tpep_pickup_datetime"].dt.dayofweek
    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    trip_duration = df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    df["trip_duration"] = trip_duration.map(lambda x: x.total_seconds() / 60)
    dateTimeColumns = list(df.select_dtypes(include=["datetime64"]).columns)
    df[dateTimeColumns] = df[dateTimeColumns].astype(str)
    df.drop(columns=["tpep_pickup_datetime", "tpep_dropoff_datetime"], inplace=True)
    return df
'''

def transformer_fn():
    """
    Returns an *unfitted* transformer that defines ``fit()`` and ``transform()`` methods.
    The transformer's input and output signatures should be compatible with scikit-learn
    transformers.
    """
    import sklearn

    function_transformer_params = (
        {}
        if sklearn.__version__.startswith("1.0")
        else {"feature_names_out": "one-to-one"}
    )
    return Pipeline(
        steps=[
            (
                "encoder",
                ColumnTransformer(
                    transformers=[
                        ("sex_encoder", OneHotEncoder(sparse_output=False), ["sex"]),
                        ("smoker_encoder", OneHotEncoder(sparse_output=False),["smoker"]),
                        ("region_encoder", OneHotEncoder(sparse_output=False),["region"]),
                        ("std_scaler_bmi", StandardScaler(), ["bmi"])
                    ]
                )
            )
        ]
    )
