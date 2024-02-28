from data_notif.csv_work import WorkCSV
from config import FILENAME_TASK_SCHEDULER
from loguru import logger
from google_table.google_tb_work import WorkGoogle
from datetime import datetime as dt
from datetime import timedelta as td


class Tasks:
    def __init__(self):
        self.tasks = WorkGoogle().get_tasks()
        self.prev_task_times = WorkCSV(FILENAME_TASK_SCHEDULER)

    def check_tasks_scheduler(self):
        """
        Проверяем возможность запуска задачи согласно заданного интервала времени.

        Находим задачу по ключу 'task_id' и проверяем прошёл ли заданный интервал 'task_interval'
        Добавлен в словарь ключ возможности запуска задачи `can_run_task`.
            Ключ принимает значение True, если интервал с последнего запуска уже прошёл.
            Ключ принимает значение False, если интервал с последнего запуска ещё не прошёл.
        """
        for i, task in enumerate(self.tasks):
            new_task = False
            delta = td(seconds=0)
            date_now = dt.now()

            # task_prev_start = self.prev_task_times.filter_tasks(task_id=task['task_id'])
            task_prev_start = self.prev_task_times.filter(type_filter='tasks', task_id=task['task_id'])

            # Получаем временя последнего запуска задачи. Если данных нет, то помечаем задачу новой.
            if task_prev_start:
                task_prev_start = task_prev_start[0]
                delta = date_now - dt.strptime(task_prev_start['task_last_start'], '%Y-%m-%d %H:%M:%S')
            else:
                new_task = True

            # Определяем возможность запуска задачи и подготавливаем данные по времени запуска в файл
            if delta.total_seconds() + 10 > float(task['task_interval']) or new_task:
                self.tasks[i]['can_run_task'] = True
                logger.warning(task_prev_start)
                self.prev_task_times.add_to_data(
                    type_data='tasks',
                    task_id=task['task_id'],
                    task_last_start=date_now.strftime('%Y-%m-%d %H:%M:%S')
                )
            else:
                self.tasks[i]['can_run_task'] = False
                self.prev_task_times.add_to_data(
                    type_data = 'tasks',
                    task_id=task_prev_start['task_id'],
                    task_last_start=task_prev_start['task_last_start']
                )

        # Записываем в файл данные по последнему запуску программы
        self.prev_task_times.add_data_file(mode="w")

        return self.tasks



