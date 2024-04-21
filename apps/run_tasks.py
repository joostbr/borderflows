from src.tasks.live_id_trades import LiveIntradayTradesTask
from src.utils.tasks.task_orchestrator import TaskOrchestrator

if __name__ == "__main__":
    tasks = [LiveIntradayTradesTask()]

    executor = TaskOrchestrator(tasks)
    executor.run()