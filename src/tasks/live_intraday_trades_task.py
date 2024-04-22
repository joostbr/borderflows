from src.intraday.live_intraday_trades import LiveIntradayTrades
from src.utils.tasks.task_orchestrator import Task


class LiveIntradayTradesTask(Task):
    def __init__(self):
        super().__init__(frequency=60, task=self.run, task_name=self.__str__())

        self.lit = LiveIntradayTrades()

        self._prev_netborder = None

    def run(self):
        trades = self.lit.get_live_trades()
        netborder = self.lit.calculate_netborder(trades)

        # simplify netborder by only considering new trades
        if self._prev_netborder is not None:
            set_old = set(self._prev_netborder.itertuples(index=False))
            mask = [x not in set_old for x in netborder.itertuples(index=False)]
            netborder_simplified = netborder[mask].copy()
        else:
            netborder_simplified = netborder

        print(f"UPLOADING {len(netborder_simplified)} RECORDS")
        if not netborder_simplified.empty:
            self.lit.upload_netborder(netborder_simplified)
        self._prev_netborder = netborder

    def __str__(self):
        return f'LiveIntradayTrades'

if __name__ == "__main__":
    lit = LiveIntradayTradesTask()
    lit.run()