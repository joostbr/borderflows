import datetime

import pandas as pd
import requests


class JaoAPI:
    def __init__(self):
        pass

    def get_core_scheduled_exchanges(self, fromutc, toutc):
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
            df["dateTimeUtc"] = pd.to_datetime(df["dateTimeUtc"]).dt.tz_localize(None)
            return df.rename(columns=ren).rename(columns=lambda x: x.replace('border_', '')).drop(columns=["id"])
        else:
            return df

    def get_core_netpositions(self, fromutc, toutc):
        r = requests.post('https://publicationtool.jao.eu/core/api/data/netpos', json={
            'FromUtc': fromutc.isoformat(),
            'ToUtc': toutc.isoformat()
        }, timeout=10)

        content = r.json()

        df = pd.DataFrame(content["data"])

        ren = {
            'dateTimeUtc': 'UTCTIME',
        }

        if len(df) > 0:
            df["dateTimeUtc"] = pd.to_datetime(df["dateTimeUtc"]).dt.tz_localize(None)
            return df.rename(columns=ren).rename(columns=lambda x: x.replace('hub_', '')).drop(columns=["id"])
        else:
            return df

    def get_ntc(self, fromutc, toutc):
        r = requests.post('https://publicationtool.jao.eu/core/api/data/intradayNtc', json={
            'FromUtc': fromutc.isoformat(),
            'ToUtc': toutc.isoformat()
        }, timeout=10)

        content = r.json()

        df = pd.DataFrame(content["data"])

        ren = {
            'dateTimeUtc': 'UTCTIME',
        }

        if len(df) > 0:
            df["dateTimeUtc"] = pd.to_datetime(df["dateTimeUtc"]).dt.tz_localize(None)
            return df.rename(columns=ren).rename(columns=lambda x: x.replace('initial_', '')).drop(columns=["id"])
        else:
            return df

    def get_atc(self, fromutc, toutc):
        r = requests.post('https://publicationtool.jao.eu/core/api/data/intradayAtc', json={
            'FromUtc': fromutc.isoformat(),
            'ToUtc': toutc.isoformat()
        }, timeout=10)

        content = r.json()

        df = pd.DataFrame(content["data"])

        ren = {
            'dateTimeUtc': 'UTCTIME',
        }

        if len(df) > 0:
            df["dateTimeUtc"] = pd.to_datetime(df["dateTimeUtc"]).dt.tz_localize(None)
            return df.rename(columns=ren).rename(columns=lambda x: x.replace('initial_', '').replace('delta_', 'DELTA_')).drop(columns=["id"])
        else:
            return df


if __name__ == "__main__":
    jp = JaoAPI()

    df = jp.get_atc(datetime.datetime(2024, 2, 20), datetime.datetime(2024, 2, 21))

    print(df.columns)