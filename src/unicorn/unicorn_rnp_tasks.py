from datetime import date, datetime, timedelta
import json

import numpy as np
import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup

#from src.datauploader.TaskOrchestrator import NoDataException, Task
#from src.database.MSDatabaseElindus import HexatradersDatabase
from src.utils.database.nxtdatabase import NXTDatabase
from src.utils.tasks.task_orchestrator import Task, NoDataException

class RnpAPI:

    def uk_import_export_scraper(self, fromdt=None, todt=None):
        if fromdt is None:
            fromdt = pytz.timezone("Europe/Brussels").localize(datetime.combine(date.today(), datetime.min.time()))
        else:
            fromdt = pytz.timezone("Europe/Brussels").localize(datetime.combine(fromdt.date(), datetime.min.time()))

        if todt is None:
            todt = pytz.timezone("Europe/Brussels").localize(datetime.combine(date.today(), datetime.min.time())) + \
                   timedelta(days=1)
        else:
            todt = pytz.timezone("Europe/Brussels").localize(datetime.combine(todt.date(), datetime.min.time()))

        dfs = []
        for country in ['BE']:  # , 'NL'
            while fromdt <= todt:
                ndt = fromdt + timedelta(days=1)

                p = self.get_data(country, fromdt)
                if len(p) != 0:
                    dfs.append(p)

                fromdt = ndt

        if len(dfs) == 0:
            return pd.DataFrame()
        df = pd.concat(dfs)

        return df

    def get_data(self, country, dt, retries=0):
        icnames = {'BE': 'Nemo Link', 'NL': 'BritNed'}
        urlpost = 'https://rnp.unicorn.com/api/Nominations/GetCrossBorderOverviewValues'
        urlcookie = 'https://rnp.unicorn.com/NOM04'

        dtutc = dt.astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')

        response = requests.get(urlcookie)
        cookies = response.cookies
        xsrf = BeautifulSoup(response.content, "lxml").find('input')['value']
        result = None
        for nomtype in [
                        {
                            'id': 405,
                            'name': 'Long-term',
                            'businessType': 'A06',
                            'isNominationType': 'yes',
                            'nominationTypeGroup': 'commercial',
                            'isTransmissionRightsType': 'yes',
                            'transmissionRightsContractType': 'A06',
                            'order': 1,
                            'code': 'longTerm',
                            'validFrom': '2019-01-01',
                            'validTo': '2050-12-31',
                        },
                        {
                            'id': 406,
                            'name': 'Daily',
                            'businessType': 'A01',
                            'isNominationType': 'yes',
                            'nominationTypeGroup': 'commercial',
                            'isTransmissionRightsType': 'yes',
                            'transmissionRightsContractType': 'A01',
                            'order': 2,
                            'code': 'daily',
                            'validFrom': '2019-01-01',
                            'validTo': '2050-12-31',
                        },
                        {
                            'id': 407,
                            'name': 'Intraday',
                            'businessType': 'A07',
                            'isNominationType': 'yes',
                            'nominationTypeGroup': 'commercial',
                            'isTransmissionRightsType': 'yes',
                            'transmissionRightsContractType': 'A07',
                            'order': 3,
                            'code': 'intraday',
                            'validFrom': '2019-01-01',
                            'validTo': '2050-12-31',
                        },
                    ]:
            try:
                #response = requests.get(urlcookie)
                #cookies = response.cookies
                #xsrf = BeautifulSoup(response.content, "lxml").find('input')['value']
                headers = {
                    'Accept': '*/*',
                    'Accept-Language': 'nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Connection': 'keep-alive',
                    'Content-Type': 'application/json',
                    'Origin': 'https://rnp.unicorn.com',
                    'Referer': 'https://rnp.unicorn.com/NOM04',
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'same-origin',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                  'Chrome/118.0.0.0 Safari/537.36',
                    'X-Requested-With': 'XMLHttpRequest',
                    'X-XSRF-TOKEN': xsrf,
                    'sec-ch-ua': '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                }

                json_data = {
                    'parameters': {
                        'busDay': dtutc,
                        'icId': 403,
                        'icName': icnames[country],
                        'nomTypeIds': [
                            nomtype['id'],
                        ],
                        'nomTypes': [nomtype],
                        'operationalReferencePointEnumValue': 'midInterconnector',
                    },
                }
                response = requests.post(urlpost, cookies=cookies, headers=headers, json=json_data)

                df = pd.DataFrame([item["columns"] for item in response.json()['crossBorderValues']["rows"]])

                label_prefix = 'ID' if nomtype['code'] == 'intraday' else ('DA' if nomtype['code'] == 'daily' else 'LT')
                cols = {
                    'timeFrom': 'UTCTIME',
                    'aggregatedValueDirA': label_prefix+"_BE_UK",
                    'aggregatedValueDirB': label_prefix+"_UK_BE",
                }
                df = df.rename(columns=cols)
                df = df[cols.values()]

                df['UTCTIME'] = pd.to_datetime(df['UTCTIME'])
                df['UTCTIME'] = df['UTCTIME'].dt.tz_localize(None)

                result = df if result is None else result.merge(df, on='UTCTIME', how='outer')

            except Exception as e:
                print('Error scraping rnp data : ' + str(e))
                if retries < 10:
                    return self.get_data(country, dt, retries=retries + 1)
                else:
                    return pd.DataFrame()

        return result


class UploadUKBorderFlowsRNP(Task):

    def __init__(self, **kwargs):
        super().__init__(task=self.upload_data, task_name="UPLOAD UK IMPORT EXPORT RNP", **kwargs)
        self.database = NXTDatabase.energy()
        self.scraper = RnpAPI()

    def upload_data(self, fromdt=None, todt=None):
        df = self.scraper.uk_import_export_scraper(fromdt, todt)
        if df.empty:
            raise NoDataException

        cols = ['UTCTIME', 'ID_BE_UK', 'DA_BE_UK', 'LT_BE_UK', 'ID_UK_BE', 'DA_UK_BE', 'LT_UK_BE']
        self.database.bulk_upsert(df=df, table='UK_BORDER_FLOWS_RNP', cols=cols)



if __name__ == '__main__':
    fromdt = datetime(2024, 2, 25)

    t = UploadUKBorderFlowsRNP(frequency=0).upload_data(fromdt, fromdt+timedelta(days=1))


