import datetime

from src.unicorn.unicorn_rnp import RnpAPI
from src.utils.constants import LOCALTZ
from src.utils.database.nxtdatabase import NXTDatabase
from src.utils.tasks.task_orchestrator import Task, NoDataException


class UploadUKBorderFlowsRNP(Task):

    def __init__(self, **kwargs):
        super().__init__(task=self.upload_data, task_name="UPLOAD UK IMPORT EXPORT RNP", **kwargs)
        self.database = NXTDatabase.energy()
        self.scraper = RnpAPI()

    def get_time_window(self):
        today_utc = datetime.datetime.now(LOCALTZ).replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0)
        return today_utc, today_utc + datetime.timedelta(days=2)

    def upload_data(self, fromutc=None, toutc=None):
        if fromutc is None or toutc is None:
            fromutc, toutc = self.get_time_window()
        df = self.scraper.uk_import_export_scraper(fromutc, toutc)
        if df.empty:
            raise NoDataException

        cols = ['UTCTIME', 'ID_BE_UK', 'DA_BE_UK', 'LT_BE_UK', 'ID_UK_BE', 'DA_UK_BE', 'LT_UK_BE']
        self.database.bulk_upsert(df=df, table='UK_BORDER_FLOWS_RNP', cols=cols)

if __name__ == '__main__':
    fromdt = datetime.datetime(2024, 1, 1)

    for i in range(1000):
        print(fromdt+datetime.timedelta(days=i),i)
        t = UploadUKBorderFlowsRNP(frequency=0).upload_data(fromdt, fromdt+datetime.timedelta(days=i))