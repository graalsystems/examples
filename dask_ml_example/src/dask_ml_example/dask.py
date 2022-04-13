import dask.dataframe

def test_dask():
    df = dask.dataframe.read_csv('https://raw.githubusercontent.com/ThomasGraff/test_dask/main/dataset_test_dask.csv?token=GHSAT0AAAAAABTODS3JU2CBSAUZJD3OXPHYYSWZKTQ')
    mean_std_df = df.groupby("name")
    mean_std_df = mean_std_df.mean()
    mean_std_df = mean_std_df.std()
    print(mean_std_df.head())

    print("Dask OK !")
    return True