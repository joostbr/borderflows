import os
from os import getenv

import pandas as pd
import sqlalchemy.engine
from dotenv import load_dotenv
from elindus_utils.msdatabase.MSDatabase import MSDatabase as MSDatabaseElindus


# pip install python-dotenv
# INDIEN GEEN .env FILE
# DOWNLOAD VAN SERVER .6 of .20

class MSDatabaseAbstract(MSDatabaseElindus):

    def __init__(self, **kwargs):
        self.loaded_env = False
        super().__init__(**kwargs)

    def load_specific_env(self):
        path = os.getenv('ENVIRONMENT_FILE_PATH', None)
        if path is None:
            self.loaded_env = False
            return False
        found_dotenv = dotenv.find_dotenv(path)
        load_dotenv(dotenv_path=found_dotenv)
        self.loaded_env = True
        return True

    def query(self, sql: str) -> pd.DataFrame:
        return self.get_pandas_df(sql)

    def delete(self, table: str, where: str = None) -> sqlalchemy.engine.CursorResult:
        sql = f"DELETE FROM {table}"
        if where is not None:
            sql += " WHERE " + where
        return self.execute_sql(sql)

    @property
    def engine(self) -> sqlalchemy.engine.base.Connection:
        return self.get_engine().connect()


class HexatradersDatabase(MSDatabaseAbstract):
    _instance = None

    @staticmethod
    def get_instance():
        if HexatradersDatabase._instance is None:
            HexatradersDatabase._instance = HexatradersDatabase()
        return HexatradersDatabase._instance

    def __init__(self):
        #loaded = self.load_specific_env()
        load_dotenv()
        port = getenv('H_PORT')
        odbc_driver_version = getenv('H_ODBC_VERSION')
        host = getenv('H_HOST')
        database = getenv('H_DATABASE')
        username = getenv("H_USERNAME")
        environment = os.getenv('ENVIRONMENT', None)


        password = os.getenv('H_PASSWORD')

        super().__init__(host=host, username=username, password=password,
                         database=database, port=port,
                         odbc_driver_version=odbc_driver_version)

    def execute(self, sql):
        with self.engine.connect() as connection:
            t = connection.begin()
            connection.execute(sql)
            t.commit()


class HexatradersDatabase_RO(MSDatabaseAbstract):
    _instance = None

    @staticmethod
    def get_instance():
        if HexatradersDatabase_RO._instance is None:
            HexatradersDatabase_RO._instance = HexatradersDatabase_RO()
        return HexatradersDatabase_RO._instance

    def __init__(self):
        loaded = self.load_specific_env()
        port = getenv('H_PORT')
        odbc_driver_version = getenv('H_ODBC_VERSION')
        host = getenv('H_HOST_RO')
        database = getenv('H_DATABASE_RO')
        username = getenv("H_USERNAME_RO")
        environment = os.getenv('ENVIRONMENT', None)

        if not loaded and (environment == "production" or environment == "test"):
            password = open(f"/run/secrets/H_PASSWORD_RO", "r").read()
        else:
            password = os.getenv('H_PASSWORD_RO')

        super().__init__(host=host, username=username, password=password,
                         database=database, port=port,
                         odbc_driver_version=odbc_driver_version)


class LynxDatabase(MSDatabaseAbstract):
    _instance = None

    @staticmethod
    def get_instance():
        if LynxDatabase._instance is None:
            LynxDatabase._instance = LynxDatabase()
        return LynxDatabase._instance

    def __init__(self):
        loaded = self.load_specific_env()
        port = getenv('L_PORT')
        odbc_driver_version = getenv('L_ODBC_VERSION')
        host = getenv('L_HOST')
        database = getenv('L_DATABASE')
        username = os.getenv('L_USERNAME')
        environment = os.getenv('ENVIRONMENT', None)
        if not loaded and (environment == "production" or environment == "test"):
            password = open(f"/run/secrets/L_PASSWORD", "r").read()
        else:
            password = os.getenv('L_PASSWORD')

        super().__init__(host=host, username=username, password=password,
                         database=database, port=port,
                         odbc_driver_version=odbc_driver_version)
