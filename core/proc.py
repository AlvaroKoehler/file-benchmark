import pyarrow as pa
import pyarrow.parquet as pq
import sqlite3
import conf


class ParquetWrap:

    def __init__(self, fparquet, chunk=False):

        self.file = fparquet
        self.chunk = chunk
        self.pwriter = None
        self.schema = None

    def get_writer(self):
        if self.pwriter:
            return self.pwriter
        else:
            self.pwriter = pq.ParquetWriter(self.file, self.schema)
            return self.pwriter

    def write(self, df):
        if self.chunk:
            for row in df:
                table = pa.Table.from_pandas(row)
                self.schema = table.schema
                pwriter = self.get_writer()
                pwriter.write_table(table)
        else:
            table = pa.Table.from_pandas(df)
            self.schema = table.schema
            pwriter = self.get_writer()
            pwriter.write_table(table)

    def destroy(self):
        if self.write:
            self.pwriter.close()


def persist_sql(df, name, chunk=False):
    conn = sqlite3.connect(conf.SQLITE)
    if chunk:
        for row in df:
            row.to_sql(name, conn, if_exists="append", index=False)
    else:
        df.to_sql(name, conn, if_exists="append", index=False)


def persist_h5(df, filename, name, chunk=False):
    if chunk:
        for row in df:
            row.to_hdf(filename, key=name)
    else:
        df.to_hdf(filename, key=name)
