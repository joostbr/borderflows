from datetime import datetime, timedelta

from src.transnet.transnet_api import TransnetAPI
from src.utils.database.nxtdatabase import NXTDatabase

if __name__ == "__main__":
    ta = TransnetAPI()

    startdt = datetime(2025, 8, 4)
    todt = datetime(2025, 8, 5)
    dt = startdt

    while dt < todt:
        print(dt)
        ndt = dt + timedelta(days=1)
        num_rows = NXTDatabase.energy().engine.execute("SELECT COUNT(DISTINCT UTCTIME) FROM PICASSO_CMOL WHERE UTCTIME >= '{}' AND UTCTIME < '{}'".format(dt.strftime("%Y-%m-%d"), ndt.strftime("%Y-%m-%d"))).fetchone()[0]

        print("ROWS", num_rows)
        if num_rows < 96:
            cmols = ta.get_picasso_cmol(dt)
            ta.upload_lmols(cmols)
            ta.upload_cmols(cmols)

        dt = ndt