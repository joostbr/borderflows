from datetime import date, timedelta, datetime

import pandas as pd

from src.transnet.transnet_api import TransnetAPI
from src.utils.tasks.task_orchestrator import Task


class UploadPICASSOMOLTask(Task):
    def __init__(self, **kwargs):
        super().__init__(task=self.upload_data, task_name="UPLOAD PICASSO MOL", **kwargs)
        self.transnet_api = TransnetAPI()
        self.c = 0

    def upload_data(self):
        fromdt = date.today() - timedelta(days=1 if self.c % 10 == 0 else 0)
        todt = date.today() + timedelta(days=1)
        dt = fromdt

        while dt < todt:
            print("Uploading", dt)
            cmols = self.transnet_api.get_picasso_cmol(dt)

            if cmols is not None:
                self.transnet_api.upload_lmols(cmols)
                self.transnet_api.upload_cmols(cmols)
            else:
                print("No data found for", dt)

            dt += timedelta(days=1)

        self.c += 1

class UploadPICASSOExchangedVolumesTask(Task):
    def __init__(self, **kwargs):
        super().__init__(task=self.upload_data, task_name="UPLOAD PICASSO EXCHANGED VOLUMES", **kwargs)
        self.transnet_api = TransnetAPI()

    def upload_data(self):
        today = date.today()

        if datetime.now().hour < 6: # bc first quarter hour is missing
            df_vol_prev = self.transnet_api.get_picasso_exchanged_volumes(today - timedelta(days=1))
        else:
            df_vol_prev = None

        df_vol = self.transnet_api.get_picasso_exchanged_volumes(today)

        if df_vol_prev is not None:
            df_vol = pd.concat([df_vol_prev, df_vol])

        self.transnet_api.upload_exchanged_volumes(df_vol)

if __name__ == '__main__':
    UploadPICASSOExchangedVolumesTask(frequency=0).execute()