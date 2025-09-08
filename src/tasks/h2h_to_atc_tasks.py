from datetime import datetime, timedelta

import pandas as pd
import pytz

from src.math.atc.atc_optimizer import ATCGraphOptimizer
from src.utils.database.msdb_elindus import HexatradersDatabase_RO, HexatradersDatabase
from src.utils.tasks.task_orchestrator import Task


class H2HToATCTask(Task):
    def __init__(self, **kwargs):
        super().__init__(task=self.convert, task_name="CONVERTING H2H TO ATC", **kwargs)

        self.optimizers = {"BE": ATCGraphOptimizer(["BE", "FR", "DE", "NL"])}
        self.msdb_ro = HexatradersDatabase_RO.get_instance()
        self.msdb = HexatradersDatabase.get_instance()

    def get_data(self, from_utc, to_utc):
        df = self.msdb_ro.query(f"""
                    WITH max_creation AS (
                        SELECT utctime, from_area, to_area, MAX(creationdate) AS max_creationdate
                        FROM nordpool.HUBTOHUBCAPACITIES
                        WHERE 
                            creationdate < DATEADD(minute, -65, utctime)
                            AND UTCTIME >= '{from_utc.isoformat()}'
                            AND UTCTIME < '{to_utc.isoformat()}'
                        group by from_area, to_area, utctime
                    )

                    SELECT h.*
                    FROM nordpool.HUBTOHUBCAPACITIES h
                    JOIN max_creation mc
                        ON h.utctime = mc.utctime and h.to_area = mc.to_area and h.from_area=mc.from_area and h.creationdate = mc.max_creationdate
                    WHERE 
                        h.UTCTIME >= '{from_utc.isoformat()}'
                        AND h.UTCTIME < '{to_utc.isoformat()}'
                                        """)

        df.loc[df["FROM_AREA"] == "AMP", "FROM_AREA"] = "DE"
        df.loc[df["TO_AREA"] == "AMP", "TO_AREA"] = "DE"

        return df

    def convert(self, data):
        atc_df = []
        for utctime, group in data.groupby("UTCTIME"):
            h2h_capacities = group.set_index(["FROM_AREA", "TO_AREA"])["OUT_CAPACITY"].to_dict()
            h2h_capacities.update(group.set_index(["TO_AREA", "FROM_AREA"])["IN_CAPACITY"].to_dict())

            for country, optimizer in self.optimizers.items():
                flows = optimizer.solve(h2h_capacities)

                records = [{
                    "UTCTIME": utctime,
                    "FROM_AREA": country,
                    "TO_AREA": c,
                    "IN_CAPACITY": flows.get((c, country), 0),
                    "OUT_CAPACITY": flows.get((country, c), 0),
                    "CREATIONDATE": max(group["CREATIONDATE"])
                } for c in optimizer.countries if c != country]

                atc_df.append(pd.DataFrame(records))
                print(utctime, optimizer.max_approx_error())

        return pd.concat(atc_df, ignore_index=True)

    def upload_data(self, fromutc=None, toutc=None):
        if fromutc is None:
            fromutc = datetime.now(pytz.utc) - timedelta(days=1)

        if toutc is None:
            toutc = datetime.now(pytz.utc) + timedelta(days=2)

        data = self.get_data(fromutc, toutc)
        df = self.convert(data)

        self.msdb.bulk_upsert(df, "traders.INTRADAY_ATC_CAPACITY_DERIVED", key_cols=["UTCTIME", "FROM_AREA", "TO_AREA"], data_cols=["IN_CAPACITY", "OUT_CAPACITY", "CREATIONDATE"])


if __name__ == "__main__":
    import dotenv
    dotenv.load_dotenv()

    H2HToATCTask(frequency=0).upload_data()