from src.intraday.live_intraday_trades import LiveIntradayTrades
from src.utils.tasks.task_orchestrator import Task
import time

class LiveIntradayTradesTask(Task):
    def __init__(self, region):
        self.region = region
        super().__init__(frequency=60, task=self.run, task_name=self.__str__())

        self.lit = LiveIntradayTrades(region)
        self._prev_netborder = None
        self._prev_netborder_h = None
        self._prev_netborder_qh = None
        self._prev_netborder_hh = None

    def _filter_new_trades(self, trades_df, prev_trades_df):
        if prev_trades_df is not None:
            set_old = set(prev_trades_df.itertuples(index=False))
            mask = [x not in set_old for x in trades_df.itertuples(index=False)]
            filtered_trades = trades_df[mask].copy()
        else:
            filtered_trades = trades_df
        return filtered_trades


    def run(self):
        trades = self.lit.get_live_trades()
        netborder, netborder_h, netborder_hh, netborder_qh = self.lit.calculate_netborder(trades)

        # simplify netborder inserts by selecting the new trades only
        netborder_filtered = self._filter_new_trades(netborder, self._prev_netborder)
        netborder_h_filtered = self._filter_new_trades(netborder_h, self._prev_netborder_h)
        netborder_hh_filtered = self._filter_new_trades(netborder_hh, self._prev_netborder_hh)
        netborder_qh_filtered = self._filter_new_trades(netborder_qh, self._prev_netborder_qh)

        print(f"UPLOADING {len(netborder_filtered),len(netborder_h_filtered),len(netborder_hh_filtered),len(netborder_qh_filtered)} RECORDS")
        if not netborder_filtered.empty:
            self.lit.upload_netborder(netborder_filtered, netborder_h=netborder_h_filtered, netborder_hh=netborder_hh_filtered, netborder_q=netborder_qh_filtered)
        self._prev_netborder = netborder
        self._prev_netborder_h = netborder_h
        self._prev_netborder_hh = netborder_hh
        self._prev_netborder_qh = netborder_qh


    def __str__(self):
        return f'LiveIntradayTrades[{self.region}]'

if __name__ == "__main__":
    lit = LiveIntradayTradesTask(region="Belgium")
    while True:
        print("running")
        lit.run()
        time.sleep(5)