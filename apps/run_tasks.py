from src.tasks.jao_tasks import NTCJAO, DABorderFlowsJAO, DANetpositionsJAO, ATCJAO
from src.tasks.live_intraday_trades_task import LiveIntradayTradesTask
from src.tasks.rnp_tasks import UploadUKBorderFlowsRNP
from src.utils.tasks.task_orchestrator import TaskOrchestrator

if __name__ == "__main__":
    tasks = [
        LiveIntradayTradesTask(),
        DABorderFlowsJAO(executiontime='14:00:00'),
        DANetpositionsJAO(executiontime='14:00:00'),
        NTCJAO(executiontime='14:00:00'),
        ATCJAO(executiontime='14:00:00'),
        UploadUKBorderFlowsRNP(frequency=15*60)
    ]

    executor = TaskOrchestrator(tasks)
    executor.run()