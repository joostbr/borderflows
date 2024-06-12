import datetime

from sqlalchemy import text

from src.intraday.intraday_trades import IntradayTrades
from src.utils.database.nxtdatabase import NXTDatabase

if __name__ == "__main__":
    from_utc = datetime.datetime(2024, 4, 20)
    to_utc = datetime.datetime(2024, 4, 24)

    intraday_trades = IntradayTrades(region="Belgium")

    dt = from_utc
    while dt < to_utc:
        print(dt)
        ndt = dt + datetime.timedelta(days=1)
        trades = intraday_trades.get_trades(dt, ndt)
        print(len(trades))
        netborder = intraday_trades.calculate_netborder(trades)
        print("NETBORDER CALCULATED", len(netborder))

        #netborder = NXTDatabase.energy().query(f"""
        #    SELECT *
        #    FROM XBID_TRADES
        #    WHERE UTCTIME >= '{dt.isoformat()}' AND UTCTIME < '{ndt.isoformat()}'
        #""")

        intraday_trades.upload_netborder(netborder)

        dt = ndt