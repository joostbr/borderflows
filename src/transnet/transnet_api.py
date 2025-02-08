import zipfile
from datetime import date
from io import StringIO, BytesIO

import pandas as pd
import requests
import xml.etree.ElementTree as ET

from src.model.cmol.cmol import CMOL


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
            print("ZIP file downloaded successfully!")

            # Open the ZIP file in memory
            with zipfile.ZipFile(BytesIO(r.content)) as zip_file:
                # List all files in the ZIP
                file_list = zip_file.namelist()
                print("Files in ZIP:", file_list)

                # Process each file (Example: read CSV or JSON files directly)
                for file_name in file_list:
                    with zip_file.open(file_name) as file:
                        content = file.read().decode('utf-8')  # Decode text-based files
                        print(f"Contents of {file_name}:")
                        print(content[:500])  # Print first 500 characters for preview

        else:
            print(f"Failed to download ZIP. HTTP Status Code: {r.status_code}")

    def parse_xml(self, xml):
        tree = ET.parse('picasso.xml')
        root = tree.getroot()

        return CMOL.from_xml(root)

if __name__ == "__main__":
    ta = TransnetAPI()

    cmol = ta.parse_xml("")

    cmol_by_area_and_direction = {}

    for bid in cmol.bids:
        key = (bid.connecting_domain_id, bid.direction)
        if key not in cmol_by_area_and_direction:
            cmol_by_area_and_direction[key] = []

        cmol_by_area_and_direction[key].append(bid)

    print(cmol_by_area_and_direction)

    dt = date(2025, 1, 1)
    #df = ta.get_picasso_cmol(dt)

    #print(df)