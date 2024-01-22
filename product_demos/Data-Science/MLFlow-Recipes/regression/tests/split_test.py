import pytest
import os
import pandas as pd
from pandas import Series
from steps.split import create_dataset_filter


@pytest.fixture
def sample_data():
    return pd.read_parquet(
        os.path.join(os.path.dirname(__file__), "test_sample.parquet")
    )


def test_split_step_outcome(sample_data):
    processed = create_dataset_filter(sample_data)
    assert isinstance(processed, Series)
    assert not processed.empty
    assert sample_data.size == 60
    assert sample_data[processed].size == 60
