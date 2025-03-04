from datetime import date, timedelta

import pandas as pd

from src.transnet.transnet_api import TransnetAPI
from src.utils.database.nxtdatabase import NXTDatabase

if __name__ == "__main__":
    ta = TransnetAPI()

    startdt = date(2025, 2, 1)
    todt = date(2025, 3, 1)
    dt = startdt

    df_vol_prev = None

    while dt < todt:
        print(dt)
        ndt = dt + timedelta(days=1)
        num_rows = NXTDatabase.energy().engine.execute(
            "SELECT COUNT(DISTINCT UTCTIME) FROM PICASSO_EXCHANGED_VOLUMES WHERE UTCTIME >= '{}' AND UTCTIME < '{}'".format(
                dt.strftime("%Y-%m-%d"), ndt.strftime("%Y-%m-%d"))).fetchone()[0]

        # query if we dont have previous
        if df_vol_prev is None:
            df_vol_prev = ta.get_picasso_exchanged_volumes(dt - timedelta(days=1))

        print("ROWS", num_rows)
        if num_rows < 96:
            df_vol = ta.get_picasso_exchanged_volumes(dt)

            df = pd.concat([df_vol, df_vol_prev])
            ta.upload_exchanged_volumes(df)

            df_vol_prev = df_vol
        else:
            df_vol_prev = None

        dt = ndt