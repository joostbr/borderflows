import datetime

from sqlalchemy import text

from src.intraday.intraday_trades import IntradayTrades
from src.utils.database.nxtdatabase import NXTDatabase


def upload_netborder(netborder):
    db = NXTDatabase.energy()

    from_utc_str = netborder["UTCTIME"].min().isoformat()
    to_utc_str = netborder["UTCTIME"].max().isoformat()

    with db.engine.begin() as conn:
        conn.execute(text(f"DELETE FROM XBID_TRADES WHERE UTCTIME >= '{from_utc_str}' AND UTCTIME <= '{to_utc_str}'"))
        netborder.to_sql("XBID_TRADES", conn, if_exists="append", index=False)

if __name__ == "__main__":
    from_utc = datetime.datetime(2024, 4, 20)
    to_utc = datetime.datetime(2024, 4, 22)

    intraday_trades = IntradayTrades()

    dt = from_utc
    while dt < to_utc:
        print(dt)
        ndt = dt + datetime.timedelta(days=1)
        trades = intraday_trades.get_trades(dt, ndt)
        print(len(trades))
        netborder = intraday_trades.calculate_netborder(trades)

        upload_netborder(netborder)

        dt = ndt