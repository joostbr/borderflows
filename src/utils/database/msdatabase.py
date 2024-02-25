import datetime
import json
import os

import pandas as pd
import pyodbc
import pytz
from sqlalchemy import create_engine, text
from src.utils.constants import CONFIG_PATH

with open(os.path.join(CONFIG_PATH, "config.json"), "r") as f:
    HEXATRADERS_DB_CONFIG = json.loads(f.read())["hexatraders"]

class MSDatabase:

    _hexatraders_instance = None
    _hexatraders_nom_instance = None
    _lynx_instance = None

    @staticmethod
    def hexatraders():
        if MSDatabase._hexatraders_instance is None:
            MSDatabase._hexatraders_instance = MSDatabase(**HEXATRADERS_DB_CONFIG)
        return MSDatabase._hexatraders_instance

    @staticmethod
    def close_all():
        if not MSDatabase._hexatraders_instance is None:
            MSDatabase._hexatraders_instance.close()
        if not MSDatabase._lynx_instance is None:
            MSDatabase._lynx_instance.close()

    def __init__(self, server, user, password, database):
        self.server = server
        self.user = user
        self.password = password
        self.database = database

        self._engine = None

    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_engine('mssql+pyodbc://' + self.user + ':' + self.password + '@' + self.server + ':1433/' + self.database + "?driver=ODBC+Driver+17+for+SQL+Server",fast_executemany=True)

        return self._engine

    def close(self):
        if not self._engine is None:
            self._engine.dispose()
            self._engine = None

    def get_engine(self):
        return self.engine

    def query(self, sql, timecols=None, localdatecols=None, indexcols=None):

        alldatecols = None
        if timecols != None:
            alldatecols = timecols
        if localdatecols != None:
            alldatecols = localdatecols if alldatecols == None else alldatecols + localdatecols

        result = pd.read_sql_query(sql, self.engine, parse_dates=alldatecols, index_col=indexcols)
        if timecols != None:
            for tc in timecols:
                result[tc] = result[tc].dt.tz_localize("UTC").dt.tz_convert("Europe/Brussels")
        if localdatecols != None:
            for tc in localdatecols:
                result[tc] = result[tc].dt.tz_localize("Europe/Brussels")

        return result

    def delete(self, table, where):

        with self.engine.connect() as connection:
            connection.execute("DELETE FROM " + table + " WHERE " + where)
            connection.close()

    def update(self, table, setclause, where=None):

        with self.engine.connect() as connection:
            if where is not None:
                connection.execute("UPDATE " + table + " SET " + setclause + " WHERE " + where)
            else:
                connection.execute("UPDATE " + table + " SET " + setclause)
            connection.close()

    def execute(self, proc="my_procedure", params=['x', 'y', 'z']):

        connection = self.engine.raw_connection()
        try:
            cursor = connection.cursor()
            cursor.callproc("my_procedure", ['x', 'y', 'z'])
            results = list(cursor.fetchall())
            cursor.close()
            connection.commit()
        finally:
            connection.close()

    def update_batch(self, sql, values):

        with self.engine.connect() as con:

            statement = text(sql)

            for line in values:
                con.execute(statement, **line)

if __name__ == "__main__":
    fromdt = datetime.datetime(2022, 11, 1)
    todt = datetime.datetime(2022, 12, 1)

    fromdtlocal = pytz.utc.localize(fromdt)
    todtlocal = pytz.utc.localize(todt)

    df = MSDatabase.hexatraders().query("""
    SELECT * FROM EPEXSPOT WHERE UTCTIME>='{}' and UTCTIME<'{}'
    """.format(fromdtlocal.isoformat(), todtlocal.isoformat()))

    df_range = pd.date_range(min(df["UTCTIME"]), max(df["UTCTIME"]) + tdelta(minutes=14),
                             freq="1min").to_frame().rename(columns={0: "UTCTIME"})
    df = df_range.merge(df, how="left", left_on="UTCTIME", right_on="UTCTIME")

    df = df.ffill()
    df["TIME"] = df["UTCTIME"].dt.tz_localize(pytz.utc).dt.tz_convert(pytz.timezone("Europe/Brussels")).dt.tz_localize(
        None)
    df[["TIME", "PRICE"]].to_excel("epex.xlsx")

