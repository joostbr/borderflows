import datetime
from datetime import tzinfo

import pandas as pd
import pytz
from sqlalchemy import text

from src.intraday.delivery_areas import DeliveryArea
from src.utils.database.msdb_elindus import HexatradersDatabase, HexatradersDatabase_RO
from src.utils.database.nxtdatabase import NXTDatabase


class IntradayTrades:
    def __init__(self, region):
        self.msdb = HexatradersDatabase.get_instance()
        self.msdb_ro = HexatradersDatabase_RO.get_instance()

        self._id_cols = ["PRICE", "VOLUME", "DELIVERYSTARTUTC", "DELIVERYENDUTC", "BUYERAREA", "SELLERAREA"]
        self._epex_query = f"""
            SELECT ID, PRICE, QUANTITY AS VOLUME, DELIVERYSTARTUTC, DELIVERYENDUTC, BUYERAREA, SELLERAREA, TRADETIMEUTC
            FROM {self._get_epex_table_for_region(region)}
        """

        self._np_query = f"""
            SELECT ID, PRICE, QUANTITY AS VOLUME, DELIVERYSTARTUTC, DELIVERYENDUTC, BUYERAREA, SELLERAREA, TIME
            FROM {self._get_nordpool_table_for_region(region)}
        """

    def _get_epex_table_for_region(self, region):
        if region == "Belgium":
            return "PUBLICTRADEEPEX"
        else:
            return "traders.PUBLICTRADEEPEX"

    def _get_nordpool_table_for_region(self, region):
        if region == "Belgium":
            return "PUBLICTRADE"
        else:
            return "traders.PUBLICTRADENORDPOOL"

    def _get_epex_trades(self, from_utc, to_utc):
        df = self.msdb_ro.query(self._epex_query + f"""
            WHERE DELIVERYSTARTUTC >= '{from_utc.isoformat()}' AND DELIVERYENDUTC <= '{to_utc.isoformat()}'
        """)

        if len(df) == 0:
            return df

        df['BUYERAREA'] = df['BUYERAREA'].apply(lambda x: DeliveryArea.get_delivery_area_by_eic_code(x).area_code)
        df['SELLERAREA'] = df['SELLERAREA'].apply(lambda x: DeliveryArea.get_delivery_area_by_eic_code(x).area_code)

        return df

    def _get_np_trades(self, from_utc, to_utc):
        df = self.msdb_ro.query(self._np_query + f"""
            WHERE DELIVERYSTARTUTC >= '{from_utc.isoformat()}' AND DELIVERYENDUTC <= '{to_utc.isoformat()}'
        """)

        if len(df) == 0:
            return df

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

        if len(epex_trades) == 0:
            return np_trades_ind.reset_index() # bugfix if epex is empty, otherwise column types are messed up

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

        trades_grouped = trades.sort_values(by='TRADETIMEUTC').groupby(["BUYERAREA", "SELLERAREA", "DELIVERYSTARTUTC", "DELIVERYENDUTC"]).agg({
            "VOLPRICE": "sum",
            "VOLUME": "sum",
            "PRICE": "last",
        }).reset_index().rename(columns={"PRICE": "LAST_PRICE"})

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
            "VOLUME": "sum",
        }).reset_index()

        netborder["PRICE"] = netborder["VOLPRICE"] / netborder["VOLUME"]
        netborder_h["PRICE"] = netborder_h["VOLPRICE"] / netborder_h["VOLUME"]
        netborder_hh["PRICE"] = netborder_hh["VOLPRICE"] / netborder_hh["VOLUME"]
        netborder_q["PRICE"] = netborder_q["VOLPRICE"] / netborder_q["VOLUME"]

        netborder_h = netborder_h.drop(columns="VOLPRICE").reset_index().rename(columns={"DELIVERYSTARTUTC": "UTCTIME"})
        netborder_hh = netborder_hh.drop(columns="VOLPRICE").reset_index().rename(columns={"DELIVERYSTARTUTC": "UTCTIME"})
        netborder_q = netborder_q.drop(columns="VOLPRICE").reset_index().rename(columns={"DELIVERYSTARTUTC": "UTCTIME"})

        netborder_h["PRODUCTTYPE"] = "H"
        netborder_q["PRODUCTTYPE"] = "QH"
        netborder_hh["PRODUCTTYPE"] = "HH"

        return netborder.drop(columns="VOLPRICE").rename(columns={"DELIVERYSTARTUTC": "UTCTIME"}), netborder_h, netborder_hh, netborder_q

    def upload_netborder(self, netborder, netborder_h=None, netborder_hh=None, netborder_q=None):
        if len(netborder) == 0:
            return

        print("AMPLIFINO UPLOAD")
        NXTDatabase.energy().bulk_upsert(netborder, "XBID_TRADES", key_cols=["UTCTIME", "BUYERAREA", "SELLERAREA"], data_cols=["VOLUME", "PRICE"], moddate_col="CREATIONDATE")

        # Upload to smart
        print("SMART UPLOAD")
        self._upload_to_smart(netborder, netborder_h, netborder_hh, netborder_q)

    def _upload_to_smart(self, netborder, netborder_h=None, netborder_hh=None, netborder_qh=None):
        HexatradersDatabase.get_instance().bulk_upsert(df=netborder, table="traders.XBID_TRADES", key_cols=["UTCTIME", "BUYERAREA", "SELLERAREA"], data_cols=["VOLUME", "PRICE"], moddate_col="CREATIONDATE")
        if netborder_h is not None and len(netborder_h)>0:
            HexatradersDatabase.get_instance().bulk_upsert(netborder_h, "traders.XBID_TRADES_PER_PRODUCT", key_cols=["UTCTIME", "BUYERAREA", "SELLERAREA","PRODUCTTYPE"], data_cols=["VOLUME", "PRICE"], moddate_col="CREATIONDATE")
        if netborder_hh is not None and len(netborder_hh)>0:
            HexatradersDatabase.get_instance().bulk_upsert(netborder_hh, "traders.XBID_TRADES_PER_PRODUCT", key_cols=["UTCTIME", "BUYERAREA", "SELLERAREA","PRODUCTTYPE"], data_cols=["VOLUME", "PRICE"], moddate_col="CREATIONDATE")
        if netborder_qh is not None and len(netborder_qh)>0:
            HexatradersDatabase.get_instance().bulk_upsert(netborder_qh, "traders.XBID_TRADES_PER_PRODUCT", key_cols=["UTCTIME", "BUYERAREA", "SELLERAREA","PRODUCTTYPE"], data_cols=["VOLUME", "PRICE"], moddate_col="CREATIONDATE")

    def _filter_xbid_trades(self, trades, dt):
        mask_in_range = trades["TRADETIMEUTC"] >= trades["DELIVERYSTARTUTC"].dt.floor('H') - dt - datetime.timedelta(hours=1)
        #mask_recent = trades["TRADETIMEUTC"] >= datetime.datetime.now(pytz.utc).replace(tzinfo=None) - dt

        return trades[mask_in_range].copy()

    def _get_xbid_stats_from_trades(self, tag, trades):
        _, nb_h, nb_hh, nb_qh = self.calculate_netborder(trades)
        nb = pd.concat([nb_h, nb_hh, nb_qh], ignore_index=True)
        nb["TAG"] = tag
        return nb.rename(columns={"PRICE": "AVG_PRICE"})

    def get_xbid_stats_lt(self, trades):
        # trades before lt trades
        trades_all = trades[trades["TRADETIMEUTC"] <= trades["DELIVERYSTARTUTC"].dt.floor('H') - datetime.timedelta(hours=1, minutes=5)].copy()

        trades_1h = self._filter_xbid_trades(trades_all, datetime.timedelta(hours=1))
        trades_3h = self._filter_xbid_trades(trades_all, datetime.timedelta(hours=3))
        trades_6h = self._filter_xbid_trades(trades_all, datetime.timedelta(hours=6))

        stats_1h = self._get_xbid_stats_from_trades("1H", trades_1h)
        stats_3h = self._get_xbid_stats_from_trades("3H", trades_3h)
        stats_6h = self._get_xbid_stats_from_trades("6H", trades_6h)
        stats_all = self._get_xbid_stats_from_trades("ALL", trades_all)

        return pd.concat([stats_1h, stats_3h, stats_6h, stats_all], ignore_index=True)

    def upload_xbid_stats(self, netborder):
        NXTDatabase.energy().bulk_upsert(netborder, "XBID_STATS", key_cols=["UTCTIME", "PRODUCTTYPE", "TAG", "BUYERAREA", "SELLERAREA"], data_cols=["VOLUME", "AVG_PRICE", "LAST_PRICE"], moddate_col="CREATIONDATE")


if __name__ == "__main__":
    from_utc = datetime.datetime(2024,6,21,0,0)
    to_utc = from_utc + datetime.timedelta(hours=24)

    intraday_trades = IntradayTrades(region="Belgium")
    trades = intraday_trades.get_trades(from_utc, to_utc)
    stats = intraday_trades.get_xbid_stats_lt(trades)
    intraday_trades.upload_xbid_stats(stats)

    #netborder, netborder_h, netborder_hh, netborder_qh = intraday_trades.calculate_netborder(trades)
    #intraday_trades.upload_netborder(netborder, netborder_h, netborder_hh, netborder_qh)

    print(trades)





