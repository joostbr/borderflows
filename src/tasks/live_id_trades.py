from src.intraday.live_intraday_trades import LiveIntradayTrades
from src.utils.tasks.task_orchestrator import Task


class LiveIntradayTradesTask(Task):
    def __init__(self):
        super().__init__(frequency=60, task=self.run, task_name=self.__str__())

        self.lit = LiveIntradayTrades()

    def run(self):
        trades = self.lit.get_live_trades()
        netborder = self.lit.calculate_netborder(trades)
        print(f"UPLOADING {len(netborder)} RECORDS")
        self.lit.upload_netborder(netborder)

    def __str__(self):
        return f'LiveIntradayTrades'