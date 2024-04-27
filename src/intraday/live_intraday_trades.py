import datetime

import pandas as pd
import pytz
from sqlalchemy import text

from src.intraday.delivery_areas import DeliveryArea
from src.intraday.intraday_trades import IntradayTrades
from src.utils.database.msdb_elindus import HexatradersDatabase
from src.utils.database.nxtdatabase import NXTDatabase


class LiveIntradayTrades(IntradayTrades):
    def __init__(self, region):
        super().__init__(region)

        self._epex_id = None
        self._np_id = None

        self._epex_df = None
        self._np_df = None

    def _get_new_epex_trades(self, id_from):
        df = self.msdb_ro.query(self._epex_query + f"""
            WHERE ID > '{id_from}'
        """)

        if len(df) == 0:
            return None

        df['BUYERAREA'] = df['BUYERAREA'].apply(lambda x: DeliveryArea.get_delivery_area_by_eic_code(x).area_code)
        df['SELLERAREA'] = df['SELLERAREA'].apply(lambda x: DeliveryArea.get_delivery_area_by_eic_code(x).area_code)

        return df

    def _get_new_np_trades(self, id_from):
        df = self.msdb_ro.query(self._np_query + f"""
            WHERE ID > '{id_from}'
        """)

        if len(df) == 0:
            return None

        df["TRADETIMEUTC"] = df["TIME"].dt.tz_localize(pytz.timezone('Europe/Brussels'),
                                                       ambiguous="NaT").dt.tz_convert(pytz.utc).dt.tz_localize(None)
        df["TRADETIMEUTC"] = df["TRADETIMEUTC"].ffill()  # fill the NaT values with the previous value

        df['BUYERAREA'] = df['BUYERAREA'].apply(lambda x: DeliveryArea.get_delivery_area_by_id(x).area_code)
        df['SELLERAREA'] = df['SELLERAREA'].apply(lambda x: DeliveryArea.get_delivery_area_by_id(x).area_code)

        return df.drop(columns="TIME")

    def update(self):
        from_utc = datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)
        to_utc = datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0)  + datetime.timedelta(days=1)

        if self._epex_id is None:
            new_epex = self._get_epex_trades(from_utc=from_utc, to_utc=to_utc)
            if new_epex is not None:
                self._epex_df = new_epex
        else:
            old_df = self._epex_df[self._epex_df["DELIVERYSTARTUTC"] >= from_utc]
            self._epex_df = pd.concat([old_df, self._get_new_epex_trades(self._epex_id)])

        self._epex_id = self._epex_df["ID"].max()

        if self._np_df is None:
            new_np = self._get_np_trades(from_utc=from_utc, to_utc=to_utc)
            if new_np is not None:
                self._np_df = new_np
        else:
            old_df = self._np_df[self._np_df["DELIVERYSTARTUTC"] >= from_utc]
            self._np_df = pd.concat([old_df, self._get_new_np_trades(self._np_id)])

        self._np_id = self._np_df["ID"].max()

    def get_live_trades(self):
        self.update()

        trades = self.combine_trades(epex_trades=self._epex_df, np_trades=self._np_df)

        return trades

if __name__ == "__main__":
    lit = LiveIntradayTrades()
    trades = lit.get_live_trades()
    print(trades)

