import datetime

from sqlalchemy import text

from src.intraday.intraday_trades import IntradayTrades
from src.utils.database.nxtdatabase import NXTDatabase

if __name__ == "__main__":
    from_utc = datetime.datetime(2022, 12, 1)
    to_utc = datetime.datetime(2023, 1, 1)

    intraday_trades = IntradayTrades(region="Belgium")

    dt = from_utc
    while dt < to_utc:
        print(dt)
        ndt = min(dt + datetime.timedelta(days=30), to_utc)
        trades = intraday_trades.get_trades(dt, ndt)
        print(len(trades), "TRADES FETCHED")
        netborder, netborder_h, netborder_hh, netborder_q = intraday_trades.calculate_netborder(trades)
        print("NETBORDER CALCULATED", len(netborder))

        #netborder = NXTDatabase.energy().query(f"""
        #    SELECT *
        #    FROM XBID_TRADES
        #    WHERE UTCTIME >= '{dt.isoformat()}' AND UTCTIME < '{ndt.isoformat()}'
        #""")

        intraday_trades.upload_netborder(netborder, netborder_h, netborder_hh, netborder_q)

        dt = ndt