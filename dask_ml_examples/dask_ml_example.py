"""Dask examples: a dataframe aggregation, scikit-learn on the Dask joblib backend, and
distributed XGBoost and LightGBM training. All the data is generated in code.

The script connects to the Dask scheduler given by --scheduler or DASK_SCHEDULER_ADDRESS;
without either, it starts a local cluster.
"""

import argparse
import os

import dask
import dask.array as da
import joblib
import numpy as np
from dask.distributed import Client


def dataframe_example() -> None:
    df = dask.datasets.timeseries(start="2000-01-01", end="2000-01-08", freq="1s", seed=42)
    stats = df.groupby("name")[["x", "y"]].agg(["mean", "std"]).compute()
    print(stats.head())
    print("Dask dataframe OK")


def sklearn_example() -> None:
    from sklearn.datasets import load_digits
    from sklearn.model_selection import RandomizedSearchCV
    from sklearn.svm import SVC

    digits = load_digits()
    param_space = {
        "C": np.logspace(-6, 6, 13),
        "gamma": np.logspace(-8, 8, 17),
        "tol": np.logspace(-4, -1, 4),
        "class_weight": [None, "balanced"],
    }
    search = RandomizedSearchCV(SVC(kernel="rbf"), param_space, cv=3, n_iter=10, random_state=0)
    # Each fit of the search runs as a Dask task.
    with joblib.parallel_backend("dask"):
        search.fit(digits.data, digits.target)
    print(f"Best parameters: {search.best_params_} (score {search.best_score_:.3f})")
    print("Dask scikit-learn OK")


def make_data(rows: int = 100_000, features: int = 20, chunk: int = 10_000):
    rng = da.random.default_rng(42)
    X = rng.random((rows, features), chunks=(chunk, features))
    y = (X[:, 0] + X[:, 1] > 1).astype("int32")
    return X, y


def xgboost_example(client: Client) -> None:
    from xgboost import dask as dxgb  # the submodule must be imported explicitly

    X, y = make_data()
    dtrain = dxgb.DaskDMatrix(client, X, y)
    output = dxgb.train(
        client,
        {"tree_method": "hist", "objective": "binary:logistic", "eval_metric": "logloss"},
        dtrain,
        num_boost_round=10,
        evals=[(dtrain, "train")],
    )
    print(f"Final train logloss: {output['history']['train']['logloss'][-1]:.4f}")
    print("Dask XGBoost OK")


def lightgbm_example(client: Client) -> None:
    import lightgbm as lgb

    X, y = make_data()
    model = lgb.DaskLGBMClassifier(n_estimators=10, client=client)
    model.fit(X, y)
    accuracy = (model.predict(X) == y).mean().compute()
    print(f"Train accuracy: {accuracy:.3f}")
    print("Dask LightGBM OK")


def main() -> None:
    parser = argparse.ArgumentParser(description="Dask examples")
    parser.add_argument("--scheduler", default=os.getenv("DASK_SCHEDULER_ADDRESS"))
    args = parser.parse_args()

    with Client(args.scheduler) as client:
        print(f"Dask dashboard: {client.dashboard_link}")
        dataframe_example()
        sklearn_example()
        xgboost_example(client)
        lightgbm_example(client)


if __name__ == "__main__":
    main()
