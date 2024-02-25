import datetime

import pandas as pd
import requests


class JaoAPI:
    def __init__(self):
        pass

    def get_scheduled_exchanges(self, fromutc, toutc):
        r = requests.post('https://publicationtool.jao.eu/core/api/data/scheduledExchanges', json={
            'FromUtc': fromutc.isoformat(),
            'ToUtc': toutc.isoformat()
        }, timeout=10)

        content = r.json()

        df = pd.DataFrame(content["data"])

        ren = {
            'dateTimeUtc': 'UTCTIME',
        }

        if len(df) > 0:
            df["UTCTIME"] = pd.to_datetime(df["UTCTIME"])


        return df.rename(columns=ren).rename(columns=lambda x: x.replace('border_', '')).drop(columns=["id"])


if __name__ == "__main__":
    jp = JaoAPI()

    df = jp.get_scheduled_exchanges(datetime.datetime(2024,2,20), datetime.datetime(2024,2,21))

    print(df)