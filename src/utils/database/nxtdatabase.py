import datetime
import json
import os

import pandas as pd
import pyodbc
import pytz
from sqlalchemy import create_engine, text
from src.utils.constants import CONFIG_PATH

with open(CONFIG_PATH, "r") as f:
    NXT_TRADING_DB_CONFIG = json.loads(f.read())["nxt_trading"]

class NXTDatabase:

    _instance_trading = None

    @staticmethod
    def trading():
        if NXTDatabase._instance_trading is None:
            NXTDatabase._instance_trading = NXTDatabase(**NXT_TRADING_DB_CONFIG)
        return NXTDatabase._instance_trading

    @staticmethod
    def close_all():
        if not NXTDatabase._instance_trading is None:
            NXTDatabase._instance_trading.close()

    def __init__(self, server, user, password, database):
        self.server = server
        self.user = user
        self.password = password
        self.database = database

        self._engine = None

    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_engine('mysql://' + self.user + ':' + self.password + '@' + self.server + ':3306/' + self.database, pool_recycle=60 * 5, pool_pre_ping=True)

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
    db = NXTDatabase.trading()

    print(db.engine.execute("SELECT 1"))

