import re
import tempfile
import zipfile
from datetime import date, datetime, timedelta
from io import StringIO, BytesIO
import os
import pandas as pd
import requests
import xml.etree.ElementTree as ET

from src.intraday.delivery_areas import TSO_AREA_MAPPING
from src.model.cmol.cmol import CMOL
from src.utils.database.nxtdatabase import NXTDatabase

MOL_QUANTILES = (0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1)

class TransnetAPI:

    def get_picasso_cbmp(self, date):
        url = "https://api.transnetbw.de/picasso-cbmp/csv?date={}".format(date.strftime("%Y-%m-%d"))

        r = requests.get(url, timeout=60)

        df = pd.read_csv(StringIO(r.text), sep=";", parse_dates=["Zeit (ISO 8601)"])
        df = df.rename(columns={"Zeit (ISO 8601)": "UTCTIME"})
        df["UTCTIME"] = df["UTCTIME"].dt.tz_localize(None)

        return df

    def get_picasso_exchanged_volumes(self, date):
        url = "https://api.transnetbw.de/picasso-interchange/csv?date={}".format(date.strftime("%Y-%m-%d"))

        r = requests.get(url, timeout=60)

        df = pd.read_csv(StringIO(r.text), sep=";", parse_dates=["Zeit (ISO 8601)"])
        df = df.rename(columns={"Zeit (ISO 8601)": "UTCTIME"})
        df["UTCTIME"] = df["UTCTIME"].dt.tz_localize(None)

        return df

    def get_picasso_cmol(self, date):
        url = "https://webservices.transnetbw.de/files/bis/picasso/cmol/AFRR_PUBLICATION_PICASSO-CMOL_{}.zip".format(date.strftime("%Y%m%d"))
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"
        }

        r = requests.get(url, headers=headers, stream=True, timeout=60)

        if r.status_code == 200:

            # Open the ZIP file in memory
            with zipfile.ZipFile(BytesIO(r.content)) as zip_file:
                # List all files in the ZIP
                file_list = sorted(zip_file.namelist(), reverse=True) # reverse sort to get the latest files first

                # Process each file
                result = {}

                for i, file_name in enumerate(file_list):
                    with zip_file.open(file_name) as file:
                        try:
                            dt = self._extract_xml_time(file)

                            if dt is None:
                                print(f"Failed to extract time from file {file_name}")
                                continue
                            elif dt not in result:
                                result[dt] = self.parse_xml(file)
                        except Exception as e:
                            print(f"Failed to parse XML file {file_name}. Error: {e}")
                            continue

                        if i % 10 == 0:
                            print("Processing file", i, len(file_list))

                return list(result.values())

        else:
            print(f"Failed to download ZIP. HTTP Status Code: {r.status_code}")

    def _extract_xml_time(self, xml):
        header = xml.read(1000).decode("utf-8")
        match = re.search(r"<start>(.*?)</start>.*?<end>(.*?)</end>", header, re.DOTALL)

        xml.seek(0) # reset file pointer

        if match:
            start_time, end_time = match.groups()
            return datetime.strptime(start_time, "%Y-%m-%dT%H:%MZ")
        else:
            return None


    def parse_xml(self, xml):
        xml_content = xml.read()#.decode('utf-8').replace('?"', '"')

        if xml_content.startswith(b'<xml'):
            xml_content = xml_content.decode('utf-8').replace('?"', '"') #+ "</xml>\n"
            xml_content = xml_content.encode('utf-8')

            root = ET.fromstring(xml_content)[0]
        else:
            root = ET.fromstring(xml_content)

        return CMOL.from_xml(root)

    def _build_lmol_df(self, cmols, quantiles):
        result = {}

        for cmol in cmols:
            if cmol.start_utc not in result or cmol.creation_utc > result[cmol.start_utc].creation_utc:
                result[cmol.start_utc] = cmol

        df = pd.concat([cmol.to_lmol_df(quantiles=quantiles) for cmol in result.values()])
        return df

    def _build_cmol_df(self, cmols, quantiles):
        result = {}

        for cmol in cmols:
            if cmol.start_utc not in result or cmol.creation_utc > result[cmol.start_utc].creation_utc:
                result[cmol.start_utc] = cmol

        df = pd.concat([cmol.to_cmol_df(quantiles=quantiles) for cmol in result.values()])
        return df

    def upload_lmols(self, cmols):
        df = self._build_lmol_df(cmols, MOL_QUANTILES)

        data_cols = [col for col in df.columns if col.startswith("UP_") or col.startswith("DOWN_")]

        NXTDatabase.energy().bulk_upsert(df, "PICASSO_LMOL", key_cols=["UTCTIME", "REGION"], data_cols=data_cols, moddate_col="CREATIONDATE")

    def upload_cmols(self, cmols):
        df = self._build_cmol_df(cmols, MOL_QUANTILES)

        data_cols = [col for col in df.columns if col.startswith("UP_") or col.startswith("DOWN_")]

        NXTDatabase.energy().bulk_upsert(df, "PICASSO_CMOL", key_cols=["UTCTIME"], data_cols=data_cols, moddate_col="CREATIONDATE")

    def upload_cbmp(self, df):
        print(df)

    def upload_exchanged_volumes(self, df):
        map = {col: value.area_code for col, value in TSO_AREA_MAPPING.items() if col in df.columns}
        df = df.rename(columns=map)
        df["QUARTERHOUR"] = df["UTCTIME"].dt.floor("15T")

        agg = {"UTCTIME": "count"}
        data_cols = []
        for region in map.values():
            df[f"{region}_UP"] = df[region].clip(0, None)
            df[f"{region}_DOWN"] = (-df[region]).clip(0, None)

            agg[f"{region}_UP"] = "mean"
            agg[f"{region}_DOWN"] = "mean"

            data_cols.append(f"{region}_UP")
            data_cols.append(f"{region}_DOWN")

        df = df.groupby("QUARTERHOUR").agg(agg).reset_index()
        df = df[df["UTCTIME"] >= 225].drop(columns="UTCTIME").rename(columns={"QUARTERHOUR": "UTCTIME"})

        NXTDatabase.energy().bulk_upsert(df, "PICASSO_EXCHANGED_VOLUMES", key_cols=["UTCTIME"], data_cols=data_cols, moddate_col="CREATIONDATE")


if __name__ == "__main__":
    ta = TransnetAPI()

    dt = date(2025, 2, 1)
    #df_vol = ta.get_picasso_exchanged_volumes(dt)
    #df_price = ta.get_picasso_cbmp(dt)
    cmol = ta.get_picasso_cmol(dt)

    df = cmol[0].to_lmol_df(quantiles=MOL_QUANTILES)
    print(df)

    #ta.upload_cbmp(df_price)
    #ta.upload_exchanged_volumes(df_vol)



