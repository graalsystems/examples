import lightgbm as lgb
import dask

def test_lightgbm():
    print("loading data")
    df = dask.dataframe.read_csv('https://raw.githubusercontent.com/ThomasGraff/test_dask/main/dataset_test_dask.csv?token=GHSAT0AAAAAABTODS3JU2CBSAUZJD3OXPHYYSWZKTQ')
    dX = df[['x','y']].to_dask_array(lengths=True)
    dy = df['target'].to_dask_array(lengths=True)
    print("beginning training")

    dask_model = lgb.DaskLGBMClassifier(n_estimators=10)
    dask_model.fit(dX, dy)
    assert dask_model.fitted_
    print("done training")
    print("LGB OK !")
    return True
