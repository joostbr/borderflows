from datetime import datetime, timedelta

from src.tasks.h2h_to_atc_tasks import H2HToATCTask
import dotenv

if __name__ == "__main__":
    dotenv.load_dotenv()
    ta = H2HToATCTask(frequency=0)

    startdt = datetime(2023, 1, 1)
    todt = datetime(2024, 1, 1)
    dt = startdt

    while dt < todt:
        print(dt)
        ndt = dt + timedelta(days=7)

        ta.upload_data(dt, ndt)

        dt = ndt