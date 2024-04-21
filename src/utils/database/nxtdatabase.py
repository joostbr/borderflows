import datetime
import json
import os

import numpy as np
import pandas as pd
import pyodbc
import pytz
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

class NXTDatabase:

    _instance_energy = None

    @staticmethod
    def energy():
        load_dotenv()
        host = os.getenv('NXT_HOST')
        database = os.getenv('NXT_DATABASE')
        username = os.getenv("NXT_USERNAME")
        password = os.getenv('NXT_PASSWORD')

        if NXTDatabase._instance_energy is None:
            NXTDatabase._instance_energy = NXTDatabase(server=host, user=username, password=password, database=database)
        return NXTDatabase._instance_energy

    @staticmethod
    def close_all():
        if not NXTDatabase._instance_energy is None:
            NXTDatabase._instance_energy.close()

    def __init__(self, server, user, password, database):
        self.server = server
        self.user = user
        self.password = password
        self.database = database

        self._engine = None

    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_engine('mysql+pymysql://' + self.user + ':' + self.password + '@' + self.server + ':3306/' + self.database, pool_recycle=60 * 5, pool_pre_ping=True)
            #self._engine = create_engine(
            #    'mysql://' + self.user + ':' + self.password + '@' + self.server + ':3306/' + self.database,
            #    pool_recycle=60 * 5, pool_pre_ping=True)

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

    def bulk_upsert(self, df, table, cols, moddate_col=None):
        if not moddate_col is None:
            df[moddate_col] = datetime.datetime.utcnow()
            cols.append(moddate_col)

        params = df[cols].replace({np.nan: None}).to_dict('records')

        v_str = ",".join(f":{col}" for col in cols)
        update_str = ",".join(f'{table}.{col} = t.{col}' for col in cols)

        query = text(f""" 
                        INSERT INTO {table}({",".join(cols)})
                        VALUES ({v_str}) AS t({",".join(cols)})
                        ON DUPLICATE KEY
                        UPDATE {update_str}
                 """)

        with self.engine.connect() as con:
            t = con.begin()
            con.execute(query, params)
            t.commit()

if __name__ == "__main__":
    db = NXTDatabase.energy()

    print(db.engine.execute("SELECT 1"))

