import os
import math
import pandas as pd
from sqlalchemy import create_engine, text


class SQLHandler:
    """
    :param str folder: Path of the folder containing the csv files
    :param list path_list: list with CSV file names
    :param dict dtypes: Dictionary with Column name and type
    """

    def __init__(self, folder, path_list, dtypes):
        self.path_list = path_list
        self.folder = folder
        self.dtypes = dtypes

    def as_pdDataframe(self) -> pd.DataFrame:
        """
        Only exists as means of exploring the csv file
        without loading everything into memory.
        """

        df = pd.DataFrame(columns=self.dtypes.keys()).astype(self.dtypes)
        size_to_read = 1_024_000

        path = os.path.join(self.folder, self.path_list[0])

        df = pd.read_csv(
            filepath_or_buffer=path,
            sep=';',
            nrows=size_to_read,
            skiprows=0,
            header=None,
            dtype=self.dtypes,
            names=self.dtypes.keys(),
            encoding='latin-1',
            decimal=','
            )

        df.reset_index()

        return df

    def to_sql_db(self, name, engine):
        """
        :param str name: Name of table
        :param engine: SQL Database connection made with SQLAlchemy
        """

        size_to_read = 1_024_000
        lines = 0
        skiprows = 0

        # Drop table before adding newer data
        sql_cmd = text(f'DROP TABLE IF EXISTS {name};')
        with engine.connect() as conn:
            conn.execute(sql_cmd)
            conn.commit()

        # Loop through files in chunks and add the data to the database
        for f in self.path_list:
            print(f'Table {name} - On file {f}')
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
                    decimal=','
                    )

                df.reset_index()

                skiprows += size_to_read

                self.__to_sql(
                    df,
                    name=name,
                    con=engine,
                    if_exists='append',
                    index=False
                    )

    def __to_sql(self, df, **kwargs):
        # Break down the file in chunks and then push to database
        size = 4096
        total = len(df)

        def chunker(df):
            return (df[i:i + size] for i in range(0, len(df), size))

        for i, dt in enumerate(chunker(df)):
            dt.to_sql(**kwargs)
