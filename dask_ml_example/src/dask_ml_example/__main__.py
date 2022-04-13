#!/usr/bin/env python
# coding: utf-8

def main(client=None):
    print('we are in the main')
    # # Dask
    import dask

    df = dask.datasets.timeseries()

    mean_std_df = df.groupby("name")
    mean_std_df = mean_std_df.mean()
    mean_std_df = mean_std_df.std()
    print(mean_std_df.head())

    print("Dask OK !")


    # # Dask Scikit-Learn & Joblib

    import numpy as np
    import joblib
    from sklearn.datasets import load_digits
    from sklearn.model_selection import RandomizedSearchCV
    from sklearn.svm import SVC

    digits = load_digits()

    param_space = {
        'C': np.logspace(-6, 6, 13),
        'gamma': np.logspace(-8, 8, 17),
        'tol': np.logspace(-4, -1, 4),
        'class_weight': [None, 'balanced'],
    }

    model = SVC(kernel='rbf')
    search = RandomizedSearchCV(model, param_space, cv=3, n_iter=10, verbose=10)

    with joblib.parallel_backend('dask'):
        search.fit(digits.data, digits.target)

    print("Dask Sklearn & Joblib OK !")


    # # Dask XGBoost

    import xgboost as xgb
    import dask.array as da

    # X and y must be Dask dataframes or arrays
    num_obs = 1e5
    num_features = 20
    X = da.random.random(size=(num_obs, num_features), chunks=(1000, num_features))
    y = da.random.random(size=(num_obs, 1), chunks=(1000, 1))

    dtrain = xgb.dask.DaskDMatrix(client, X, y)

    output = xgb.dask.train(
        client,
        {"verbosity": 2, "tree_method": "hist", "objective": "reg:squarederror"},
        dtrain,
        num_boost_round=4,
        evals=[(dtrain, "train")],
    )

    print("Dask Xgboost!")


    # #  Dask LightGBM

    from sklearn.datasets import make_blobs
    import lightgbm as lgb


    print("loading data")

    X, y = make_blobs(n_samples=1000, n_features=50, centers=2)

    print("distributing training data on the Dask cluster")

    dX = da.from_array(X, chunks=(100, 50))
    dy = da.from_array(y, chunks=(100,))

    print("beginning training")

    dask_model = lgb.DaskLGBMClassifier(n_estimators=10)
    dask_model.fit(dX, dy)
    assert dask_model.fitted_

    print("done training")
    print("LGB OK !")


if __name__ == "__main__":
    main()
