import datetime

from dateutil.relativedelta import relativedelta

from src.jao.jao_api import JaoAPI
from src.utils.constants import LOCALTZ
from src.utils.database.nxtdatabase import NXTDatabase
from src.utils.tasks.task_orchestrator import Task


class DABorderFlowsJAO(Task):
    def __init__(self, **kwargs):
        super().__init__(task=self.upload_data, task_name="UPLOAD DA BORDER FLOWS JAO", **kwargs)

        self.cols = ['UTCTIME', 'AT_CZ', 'AT_DE', 'AT_HU', 'AT_SI', 'BE_DE', 'BE_FR',
         'BE_NL', 'CZ_AT', 'CZ_DE', 'CZ_PL', 'CZ_SK', 'DE_AT', 'DE_BE', 'DE_CZ',
         'DE_FR', 'DE_NL', 'DE_PL', 'FR_BE', 'FR_DE', 'HR_HU', 'HR_SI', 'HU_AT',
         'HU_HR', 'HU_RO', 'HU_SI', 'HU_SK', 'NL_BE', 'NL_DE', 'PL_CZ', 'PL_DE',
         'PL_SK', 'RO_HU', 'SI_AT', 'SI_HR', 'SI_HU', 'SK_CZ', 'SK_HU', 'SK_PL',
         'FR_ES', 'ES_FR', 'DK1_DE', 'DE_DK1']

        self.jao_api = JaoAPI()
        self.nxt_db = NXTDatabase.energy()

    def get_time_window(self):
        today_utc = datetime.datetime.now(LOCALTZ).replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0)
        return today_utc + datetime.timedelta(days=1), today_utc + datetime.timedelta(days=2)


    def upload_data(self, fromutc=None, toutc=None):
        if fromutc is None or toutc is None:
            fromutc, toutc = self.get_time_window()
        df = self.jao_api.get_core_scheduled_exchanges(fromutc, toutc)

        if len(df) > 0:
            self.nxt_db.bulk_upsert(df, "DA_CORE_BORDER_FLOWS_JAO", self.cols, "CREATIONDATE")
        else:
            print("NO DATA FOUND")

class DANetpositionsJAO(Task):
    def __init__(self, **kwargs):
        super().__init__(task=self.upload_data, task_name="UPLOAD DA BORDER FLOWS JAO", **kwargs)

        self.cols = ['UTCTIME', 'ALBE', 'ALDE', 'AT', 'BE', 'CZ', 'DE', 'HR', 'HU', 'FR', 'NL', 'RO', 'SI', 'SK', 'PL']

        self.jao_api = JaoAPI()
        self.nxt_db = NXTDatabase.energy()

    def get_time_window(self):
        today_utc = datetime.datetime.now(LOCALTZ).replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0)
        return today_utc + datetime.timedelta(days=1), today_utc + datetime.timedelta(days=2)

    def upload_data(self, fromutc=None, toutc=None):
        if fromutc is None or toutc is None:
            fromutc, toutc = self.get_time_window()
        df = self.jao_api.get_core_netpositions(fromutc, toutc)

        if len(df) > 0:
            self.nxt_db.bulk_upsert(df, "DA_CORE_NETPOSITIONS_JAO", self.cols, "CREATIONDATE")
        else:
            print("NO DATA FOUND")

class NTCJAO(Task):
    def __init__(self, **kwargs):
        super().__init__(task=self.upload_data, task_name="UPLOAD DA BORDER FLOWS JAO", **kwargs)

        self.cols = ['UTCTIME', 'AT_CZ', 'AT_DE', 'AT_HU', 'AT_SI', 'BE_DE', 'BE_FR',
           'BE_NL', 'CZ_AT', 'CZ_DE', 'CZ_PL', 'CZ_SK', 'DE_AT', 'DE_BE', 'DE_CZ',
           'DE_FR', 'DE_NL', 'DE_PL', 'FR_BE', 'FR_DE', 'HR_HU', 'HR_SI', 'HU_AT',
           'HU_HR', 'HU_RO', 'HU_SI', 'HU_SK', 'NL_BE', 'NL_DE', 'PL_CZ', 'PL_DE',
           'PL_SK', 'RO_HU', 'SI_AT', 'SI_HR', 'SI_HU', 'SK_CZ', 'SK_HU', 'SK_PL']

        self.jao_api = JaoAPI()
        self.nxt_db = NXTDatabase.energy()

    def get_time_window(self):
        today_utc = datetime.datetime.now(LOCALTZ).replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0)
        return today_utc + datetime.timedelta(days=1), today_utc + datetime.timedelta(days=2)

    def upload_data(self, fromutc=None, toutc=None):
        if fromutc is None or toutc is None:
            fromutc, toutc = self.get_time_window()
        df = self.jao_api.get_ntc(fromutc, toutc)

        if len(df) > 0:
            self.nxt_db.bulk_upsert(df, "DA_CORE_NTC_JAO", self.cols, "CREATIONDATE")
        else:
            print("NO DATA FOUND")

class ATCJAO(Task):
    def __init__(self, **kwargs):
        super().__init__(task=self.upload_data, task_name="UPLOAD DA BORDER FLOWS JAO", **kwargs)

        self.cols = ['UTCTIME', 'AT_CZ', 'AT_DE', 'AT_HU', 'AT_SI', 'BE_DE', 'BE_FR',
           'BE_NL', 'CZ_AT', 'CZ_DE', 'CZ_PL', 'CZ_SK', 'DE_AT', 'DE_BE', 'DE_CZ',
           'DE_FR', 'DE_NL', 'DE_PL', 'FR_BE', 'FR_DE', 'HR_HU', 'HR_SI', 'HU_AT',
           'HU_HR', 'HU_RO', 'HU_SI', 'HU_SK', 'NL_BE', 'NL_DE', 'PL_CZ', 'PL_DE',
           'PL_SK', 'RO_HU', 'SI_AT', 'SI_HR', 'SI_HU', 'SK_CZ', 'SK_HU', 'SK_PL']

        self.cols_updates = [
            'UTCTIME',
            'AT_DE', 'BE_DE', 'BE_FR', 'BE_NL',
            'DE_AT', 'DE_BE', 'DE_FR', 'DE_NL',
            'FR_BE', 'FR_DE', 'NL_BE', 'NL_DE'
        ]

        self.jao_api = JaoAPI()
        self.nxt_db = NXTDatabase.energy()

    def get_time_window(self):
        today_utc = datetime.datetime.now(LOCALTZ).replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0)
        return today_utc + datetime.timedelta(days=1), today_utc + datetime.timedelta(days=2)

    def upload_data(self, fromutc=None, toutc=None):
        if fromutc is None or toutc is None:
            fromutc, toutc = self.get_time_window()

        df = self.jao_api.get_atc(fromutc, toutc)

        if len(df) > 0:
            df_deltas = df[[("DELTA_" + c) if not c == "UTCTIME" else c for c in self.cols_updates]].rename(
                columns=lambda x: x.replace('DELTA_', ''))

            self.nxt_db.bulk_upsert(df, "DA_CORE_ATC_JAO", self.cols, "CREATIONDATE")
            self.nxt_db.bulk_upsert(df_deltas.fillna(0), "ID_CORE_NTC_UPDATES_JAO", self.cols_updates, "CREATIONDATE")
        else:
            print("NO DATA FOUND")

if __name__ == "__main__":
    fromutc = datetime.datetime(2024,2,1)
    toutc = datetime.datetime(2024,3,1)

    ndt = fromutc

    while ndt < toutc:
        print(ndt)
        nndt = ndt + relativedelta(months=1)

        t = ATCJAO(frequency=0).upload_data(ndt, nndt)

        ndt = nndt


