import datetime

from sqlalchemy import text

from src.intraday.intraday_trades import IntradayTrades
from src.utils.database.nxtdatabase import NXTDatabase
import time

if __name__ == "__main__":
    from_utc = datetime.datetime(2025, 1, 1)
    to_utc = datetime.datetime(2026, 1, 1)

    intraday_trades = IntradayTrades(region="Belgium")

    dt = from_utc
    while dt < to_utc:
        print(dt)
        ndt = min(dt + datetime.timedelta(days=30), to_utc)

        t = time.time()
        trades = intraday_trades.get_trades(dt, ndt)
        print(len(trades), "trades fetched in", time.time() - t)
        stats = intraday_trades.get_xbid_stats_lt(trades)
        intraday_trades.upload_xbid_stats(stats)
        print("STATS CALCULATED", len(stats))

        dt = ndt