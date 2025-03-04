from src.tasks.jao_tasks import NTCJAO, DABorderFlowsJAO, DANetpositionsJAO, ATCJAO
from src.tasks.live_intraday_trades_task import LiveIntradayTradesTask
from src.tasks.rnp_tasks import UploadUKBorderFlowsRNP
from src.tasks.transnet_tasks import UploadPICASSOMOLTask, UploadPICASSOExchangedVolumesTask
from src.utils.tasks.task_orchestrator import TaskOrchestrator

if __name__ == "__main__":
    tasks = [
        LiveIntradayTradesTask(region="Belgium"),
        LiveIntradayTradesTask(region="Netherlands"),
        #DABorderFlowsJAO(executiontime='14:00:00'),
        #DANetpositionsJAO(executiontime='14:01:00'),
        #NTCJAO(executiontime='14:02:00'),
        #ATCJAO(executiontime='14:03:00'),
        #UploadUKBorderFlowsRNP(frequency=15*60)
        UploadPICASSOMOLTask(frequency=15*60),
        UploadPICASSOExchangedVolumesTask(frequency=15*60),
    ]

    executor = TaskOrchestrator(tasks)
    executor.run()