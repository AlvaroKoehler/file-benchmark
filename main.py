from lib.proc import ParquetWrap
from lib.proc import persist_h5
from lib.proc import persist_sql
import pandas as pd
import numpy as np
import os
import conf


def persist_file(csv_file):
    current = csv_file.split('/').pop()
    name = current.split('.')[0]
    print(f'Processing {name}')

    df = pd.read_csv(
        filepath_or_buffer=csv_file,
        dtype={
            'ID': str,
            'AlphaCode': str,
            'Company': str,
            'Country': str,
            'latlong': np.float32,
        },
        chunksize=conf.CHUNK_SIZE,
        low_memory=True,
    )

    f_h5 = conf.TO_FOLDER + 'hdf5/' + name + '.h5'
    f_parquet = conf.TO_FOLDER + 'parquet/' + name + '.parquet'
    pw = ParquetWrap(f_parquet, chunk=False)

    for row in df:
        # SQLite
        persist_sql(row, name, chunk=False)
        # H5
        persist_h5(row, f_h5, name, chunk=False)
        # Parquet
        pw.write(row)

    del df


def main():
    # Obtain list of files 
    path = os.path.abspath('.')
    new_path = path + '/' + conf.DATA_FOLDER
    csv_files = [new_path + x for x in os.listdir(new_path) if 'DS' not in x]

    # Create folders if doesn't exist and Sqlite connection
    if not os.path.exists(conf.TO_FOLDER):
        os.makedirs(conf.TO_FOLDER)
        for f in conf.FOLDERS:
            os.makedirs(conf.TO_FOLDER + f)

    # Execute everythings
    for f in csv_files:
        persist_file(f)


if __name__ == "__main__":
    main()
