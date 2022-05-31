import dask.dataframe

def test_dask():
    df = dask.dataframe.read_csv('dataset_test_dask.csv')
    mean_std_df = df.groupby("name")
    mean_std_df = mean_std_df.mean()
    mean_std_df = mean_std_df.std()
    print(mean_std_df.head())

    print("Dask OK !")
    return True