import pandas as pd
from sklearn.model_selection import train_test_split

try:
    from mage_ai.data_preparation.decorators import transformer, test
except ImportError:
    def transformer(func):
        return func

    def test(func):
        return func


@transformer
def transform(df, *args, **kwargs):
    train_df, test_df = train_test_split(
        df, test_size=0.2, random_state=42
    )

    train_df = train_df.copy()
    test_df = test_df.copy()

    train_df["split"] = "train"
    test_df["split"] = "test"

    return pd.concat([train_df, test_df])


@test
def test_output(output, *args):
    assert "split" in output.columns