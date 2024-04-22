from src.tasks.jao_tasks import NTCJAO, DABorderFlowsJAO, DANetpositionsJAO, ATCJAO
from src.tasks.live_intraday_trades_task import LiveIntradayTradesTask
from src.utils.tasks.task_orchestrator import TaskOrchestrator

if __name__ == "__main__":
    tasks = [
        LiveIntradayTradesTask(),
        DABorderFlowsJAO(frequency=3600),
        DANetpositionsJAO(frequency=3600),
        NTCJAO(frequency=3600),
        ATCJAO(frequency=3600)
    ]

    executor = TaskOrchestrator(tasks)
    executor.run()