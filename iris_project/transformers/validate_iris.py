
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    return df.copy()


@test
def test_iris_data(output, *args):
    assert output.isnull().sum().sum() == 0
    assert output["target"].nunique() == 3
