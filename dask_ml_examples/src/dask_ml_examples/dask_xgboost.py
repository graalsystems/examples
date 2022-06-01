import xgboost as xgb
import dask.dataframe
import dask

def test_xgboost(client=None):
    # X and y must be Dask dataframes or arrays
    df = dask.dataframe.read_csv('dataset_test_dask.csv')
    X = df[['x','y']].to_dask_array(lengths=True)
    y = df['target'].to_dask_array(lengths=True)

    dtrain = xgb.dask.DaskDMatrix(client, X, y)

    output = xgb.dask.train(
        client,
        {"verbosity": 2, "tree_method": "hist", "objective": "reg:squarederror"},
        dtrain,
        num_boost_round=4,
        evals=[(dtrain, "train")],
    )

    print("Dask Xgboost!")
    return True