import threading
import time
import traceback
import datetime
import random

import croniter as croniter
import pyodbc

from src.utils.constants import LOCALTZ


class NoDataException(Exception):
    pass


class RetryException(Exception):
    pass


class Task:

    # frequency in seconds; executiontime "%H:%M:%S" LOCAL TIME
    def __init__(self, frequency: int=None, executiontime: str=None, cron: str=None, task=None, task_name=None):
        self._frequency = frequency
        self._executiontime = executiontime
        self._cron = cron

        self._retry = False
        self._retry_count = 0

        if self._frequency is None and self._executiontime is None and self._cron is None:
            raise Exception("'frequency' and 'executiontime' can't be both None,"
                            " exactly one of the parameters should be initialized")
        if self._frequency is not None and self._executiontime is not None:
            raise Exception("'frequency' and 'executiontime' can't be both not None,"
                            " exactly one of the parameters should be initialized")

        self._task = task

        if task_name is None:
            task_name = task.__name__

        self.task_name = task_name

        if self._frequency is not None:
            self._next_execute = time.time() + random.randint(0, 120)
        elif self._executiontime is not None:
            ts = time.strptime(self._executiontime, '%H:%M:%S')
            today = time.mktime(datetime.datetime.now().date().timetuple()) \
                    + datetime.timedelta(hours=ts.tm_hour, minutes=ts.tm_min, seconds=ts.tm_sec).total_seconds()
            self._next_execute = today
            if today < time.time():
                self._next_execute += datetime.timedelta(days=1).total_seconds()
        elif self._cron is not None:
            self._next_execute = croniter.croniter(self._cron, datetime.datetime.now(LOCALTZ)).get_next(float)

    def get_name(self):
        return self.task_name

    def time_until_next_execution(self):
        return self._next_execute - time.time()
        # if not self._retry:
        #     return self._next_execute - time.time()
        # else:
        #     return -1

    def retry(self):
        if self._retry_count < 10:
            self._retry_count += 1
            self._retry = True
        else:
            self._retry_count = 0

    # If the task is in retry mode, _next_execute has already been updated
    def schedule_next_execution(self):
        if self._frequency is not None:
            self._next_execute += self._frequency
        elif self._executiontime is not None:
            self._next_execute += datetime.timedelta(days=1).total_seconds()
        elif self._cron is not None:
            self._next_execute = croniter.croniter(self._cron, datetime.datetime.now(LOCALTZ)).get_next(float)
        self._retry = False

        # if self._retry is not True:
        #     if self._frequency is not None:
        #         self._next_execute += self._frequency
        #     else:
        #         self._next_execute += datetime.timedelta(days=1).total_seconds()
        # self._retry = False

    def execute(self):
        self._retry = False
        print(f"EXECUTING {self.task_name}")
        status = self._task()
        print(f"EXECUTED {self.task_name}")

        return status


class TaskExecutor:

    def __init__(self, task):
        self._task = task

    def time_until_next_execution(self):
        return self._task.time_until_next_execution()

    def schedule_next_execution(self):
        self._task.schedule_next_execution()

    def execute(self):
        print(f"EXECUTING {self._task.get_name()}")
        try:
            self._task.execute()
        except Exception:
            traceback.print_exc()


class TaskOrchestrator:

    def __init__(self, tasks):
        self.tasks = tasks

    def get_closest_task(self):
        return min(self.tasks, key=lambda t: t.time_until_next_execution())

    def run(self):
        while True:

            closest_task = self.get_closest_task()

            if closest_task.time_until_next_execution() <= 0:
                t = threading.Thread(target=closest_task.execute)
                closest_task.schedule_next_execution()
                t.start()

            if self.get_closest_task().time_until_next_execution() - 1 > 0:
                print(f"sleeping for {self.get_closest_task().time_until_next_execution()}")
                time.sleep(max(self.get_closest_task().time_until_next_execution(), 0))
                print('woke up')
