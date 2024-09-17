import os
import math
import pandas as pd


class SQLHandler:
    def __init__(self, folder, path_list, dtypes):
        self.path_list = path_list
        self.folder = folder
        self.dtypes = dtypes

    def as_pdDataframe(self) -> pd.DataFrame:
        dt = pd.DataFrame(columns=self.dtypes.keys()).astype(self.dtypes)
        size_to_read = 1_000_000
        lines = 0
        skiprows = 0

        for f in self.path_list:
            path = os.path.join(self.folder, f)

            with open(path) as file:
                lines = sum(1 for _ in file)

            chunk = math.ceil(lines / size_to_read)

            for i in range(0, chunk):
                df = pd.read_csv(
                    filepath_or_buffer=path,
                    sep=';',
                    nrows=size_to_read,
                    skiprows=skiprows,
                    header=None,
                    dtype=self.dtypes,
                    names=self.dtypes.keys(),
                    encoding='latin-1',
                    )

                df.reset_index()
                dt = pd.concat([dt, df], ignore_index=True)

                skiprows += size_to_read

        return dt

    def to_sql(self, name, engine):
        size_to_read = 1_000_000
        lines = 0
        skiprows = 0

        for f in self.path_list:
            path = os.path.join(self.folder, f)

            with open(path) as file:
                lines = sum(1 for _ in file)

            chunk = math.ceil(lines / size_to_read)

            for i in range(0, chunk):
                df = pd.read_csv(
                    filepath_or_buffer=path,
                    sep=';',
                    nrows=size_to_read,
                    skiprows=skiprows,
                    header=None,
                    dtype=self.dtypes,
                    names=self.dtypes.keys(),
                    encoding='latin-1',
                    )

                df.reset_index()

                skiprows += size_to_read

                df.to_sql(
                    df,
                    name=name,
                    con=engine,
                    if_exists='append',
                    index=False
                    )
