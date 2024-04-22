import datetime

import pandas as pd
import pytz
from sqlalchemy import text

from src.intraday.delivery_areas import DeliveryArea
from src.utils.database.msdb_elindus import HexatradersDatabase, HexatradersDatabase_RO
from src.utils.database.nxtdatabase import NXTDatabase


class IntradayTrades:
    def __init__(self):
        self.msdb = HexatradersDatabase.get_instance()
        self.msdb_ro = HexatradersDatabase_RO.get_instance()

        self._id_cols = ["PRICE", "VOLUME", "DELIVERYSTARTUTC", "DELIVERYENDUTC", "BUYERAREA", "SELLERAREA"]
        self._epex_query = """
            SELECT ID, PRICE, QUANTITY AS VOLUME, DELIVERYSTARTUTC, DELIVERYENDUTC, BUYERAREA, SELLERAREA, TRADETIMEUTC
            FROM PUBLICTRADEEPEX
        """

        self._np_query = """
            SELECT ID, PRICE, QUANTITY AS VOLUME, DELIVERYSTARTUTC, DELIVERYENDUTC, BUYERAREA, SELLERAREA, TIME
            FROM PUBLICTRADE
        """

    def _get_epex_trades(self, from_utc, to_utc):
        df = self.msdb_ro.query(self._epex_query + f"""
            WHERE DELIVERYSTARTUTC >= '{from_utc.isoformat()}' AND DELIVERYENDUTC <= '{to_utc.isoformat()}'
        """)

        df['BUYERAREA'] = df['BUYERAREA'].apply(lambda x: DeliveryArea.get_delivery_area_by_eic_code(x).area_code)
        df['SELLERAREA'] = df['SELLERAREA'].apply(lambda x: DeliveryArea.get_delivery_area_by_eic_code(x).area_code)

        return df

    def _get_np_trades(self, from_utc, to_utc):
        df = self.msdb_ro.query(self._np_query + f"""
            WHERE DELIVERYSTARTUTC >= '{from_utc.isoformat()}' AND DELIVERYENDUTC <= '{to_utc.isoformat()}'
        """)

        df["TRADETIMEUTC"] = df["TIME"].dt.tz_localize(pytz.timezone('Europe/Brussels'), ambiguous="NaT").dt.tz_convert(pytz.utc).dt.tz_localize(None)
        df["TRADETIMEUTC"] = df["TRADETIMEUTC"].ffill() # fill the NaT values with the previous value

        df['BUYERAREA'] = df['BUYERAREA'].apply(lambda x: DeliveryArea.get_delivery_area_by_id(x).area_code)
        df['SELLERAREA'] = df['SELLERAREA'].apply(lambda x: DeliveryArea.get_delivery_area_by_id(x).area_code)

        return df.drop(columns="TIME")

    def _add_occurrence_counter(self, trades):
        trades['OCCURRENCES'] = trades.groupby(self._id_cols).cumcount()+1

    def _concat_unique(self, epex_trades, np_trades):
        np_trades_ind = np_trades.set_index(self._id_cols + ["OCCURRENCES"])
        epex_trades_ind = epex_trades.set_index(self._id_cols + ["OCCURRENCES"])

        np_trades_ind = np_trades_ind[~np_trades_ind.index.isin(epex_trades_ind.index)]

        return pd.concat([epex_trades, np_trades_ind.reset_index()], ignore_index=True)

    def combine_trades(self, epex_trades, np_trades):
        self._add_occurrence_counter(epex_trades)
        self._add_occurrence_counter(np_trades)

        combined = self._concat_unique(epex_trades, np_trades).drop(columns="OCCURRENCES")
        combined["BUYERAREA"] = combined["BUYERAREA"].apply(
            lambda x: DeliveryArea.get_delivery_area_by_area_code(x).country_iso_code)
        combined["SELLERAREA"] = combined["SELLERAREA"].apply(
            lambda x: DeliveryArea.get_delivery_area_by_area_code(x).country_iso_code)

        return combined

    def get_trades(self, from_utc, to_utc):
        epex_trades = self._get_epex_trades(from_utc, to_utc)
        np_trades = self._get_np_trades(from_utc, to_utc)

        return self.combine_trades(epex_trades, np_trades)

    def calculate_netborder(self, trades):
        trades = trades.copy()
        trades["VOLPRICE"] = trades["VOLUME"] * trades["PRICE"]

        trades_grouped = trades.groupby(["BUYERAREA", "SELLERAREA", "DELIVERYSTARTUTC", "DELIVERYENDUTC"]).agg({
            "VOLPRICE": "sum",
            "VOLUME": "sum"
        }).reset_index()

        netborder_q = trades_grouped[trades_grouped["DELIVERYENDUTC"] - trades_grouped["DELIVERYSTARTUTC"] == datetime.timedelta(minutes=15)].set_index("DELIVERYSTARTUTC")
        netborder_hh = trades_grouped[trades_grouped["DELIVERYENDUTC"] - trades_grouped["DELIVERYSTARTUTC"] == datetime.timedelta(minutes=30)].set_index("DELIVERYSTARTUTC")
        netborder_h = trades_grouped[trades_grouped["DELIVERYENDUTC"] - trades_grouped["DELIVERYSTARTUTC"] == datetime.timedelta(minutes=60)].set_index("DELIVERYSTARTUTC")

        netborder_hh2 = netborder_hh.copy()
        netborder_hh2.index += datetime.timedelta(minutes=15)

        netborder_h2 = netborder_h.copy()
        netborder_h2.index += datetime.timedelta(minutes=15)

        netborder_h3 = netborder_h.copy()
        netborder_h3.index += datetime.timedelta(minutes=30)

        netborder_h4 = netborder_h.copy()
        netborder_h4.index += datetime.timedelta(minutes=45)

        netborder = pd.concat([netborder_q, netborder_hh, netborder_hh2, netborder_h, netborder_h2, netborder_h3, netborder_h4]).groupby(["DELIVERYSTARTUTC", "BUYERAREA", "SELLERAREA"]).agg({
            "VOLPRICE": "sum",
            "VOLUME": "sum"
        }).reset_index()

        netborder["PRICE"] = netborder["VOLPRICE"] / netborder["VOLUME"]

        return netborder.drop(columns="VOLPRICE").rename(columns={"DELIVERYSTARTUTC": "UTCTIME"})

    def upload_netborder(self, netborder):
        NXTDatabase.energy().bulk_upsert(netborder, "XBID_TRADES", ["UTCTIME", "BUYERAREA", "SELLERAREA", "VOLUME", "PRICE"], "CREATIONDATE")

        # Upload to smart
        self._upload_to_smart(netborder)

    def _upload_to_smart(self, netborder):
        HexatradersDatabase.get_instance().bulk_upsert(df=netborder, table="traders.XBID_TRADES", key_cols=["UTCTIME", "BUYERAREA", "SELLERAREA"], data_cols=["VOLUME", "PRICE"], moddate_col="CREATIONDATE")



if __name__ == "__main__":
    from_utc = datetime.datetime(2024,2,1,0,0)
    to_utc = from_utc + datetime.timedelta(hours=2)

    intraday_trades = IntradayTrades()
    trades = intraday_trades.get_trades(from_utc, to_utc)
    netborder = intraday_trades.calculate_netborder(trades)

    print(trades)





