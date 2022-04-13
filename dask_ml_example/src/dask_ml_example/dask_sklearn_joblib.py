import numpy as np
import joblib
from sklearn.datasets import load_digits
from sklearn.model_selection import RandomizedSearchCV
from sklearn.svm import SVC

def test_dask_sklearn_joblib():

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
        search.fit(digits.data, digits.target) # Code SKLEARN à compute en parallèle

    print("Dask Sklearn & Joblib OK !")

    return True