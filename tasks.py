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
            is_working_hours = False
            delta = td(seconds=0)
            date_now = dt.now()
            time_now = date_now.time()

            # Проверяем возможен ли запуск выполнения задачи в текущее время
            start_time = task['time_start']
            end_time = task['time_finish']
            if start_time < end_time:
                is_working_hours = (start_time <= time_now <= end_time)
            else:
                is_working_hours = (time_now <= end_time or time_now >= start_time)

            task_prev_start = self.prev_task_times.filter(type_filter='tasks', task_id=task['task_id'])

            # Получаем временя последнего запуска задачи. Если данных нет, то помечаем задачу новой.
            if task_prev_start:
                task_prev_start = task_prev_start[0]
                delta = date_now - dt.strptime(task_prev_start['task_last_start'], '%Y-%m-%d %H:%M:%S')

            else:
                new_task = True

            # Добавляем 10 секунд к промежутку между запусками, чтобы убрать погрешность на время выполнения скрипта
            time_interval_start = delta.total_seconds() + 10

            # Определяем возможность запуска задачи и подготавливаем данные по времени запуска в файл
            if (time_interval_start > float(task['task_interval']) or new_task) and is_working_hours:
                self.tasks[i]['can_run_task'] = True
                task_last_start = date_now.strftime('%Y-%m-%d %H:%M:%S')
                task_id = task['task_id']

            else:
                self.tasks[i]['can_run_task'] = False
                task_last_start = task_prev_start['task_last_start']
                task_id = task_prev_start['task_id']

            # Добавляем данные для дальнейшей записи
            self.prev_task_times.add_to_data(
                type_data='tasks',
                task_id=task_id,
                task_last_start=task_last_start
            )

        # Записываем в файл данные по последнему запуску программы
        self.prev_task_times.add_data_file(mode="w")

        return self.tasks



