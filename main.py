# Author Loik Andrey mail: loikand@mail.ru
from config import FILE_NAME_LOG
from services.notification import Notif
from services.supplier_reorder import ReOrder
from google_table.google_tb_work import WorkGoogle
from datetime import datetime as dt
from tasks import Tasks
from loguru import logger

# TODO Сделать проверку рабочего времени и запустить скрипт

# Задаём параметры логирования
logger.add(FILE_NAME_LOG,
           format="{time:DD/MM/YY HH:mm:ss} - {file} - {level} - {message}",
           level="INFO",
           rotation="1 week",
           compression="zip")


def main() -> None:

    logger.info(f"... Запуск программы")
    tasks = []
    wk_g = ''
    try:
        logger.info(f"Получаем список задач")
        # wk_g = WorkGoogle()
        # tasks = wk_g.get_tasks()
        wk_tasks = Tasks()
        tasks = wk_tasks.check_tasks_scheduler()
    except Exception as e:
        logger.error(f"Ошибка при получении списка задач: {e}")
        return
    logger.warning(f"Получили задачи {tasks}")  # TODO Удалить после тестов
    for task in tasks:
        if task['task_id'] == '1' and task['can_run_task']:
            try:
                logger.info(f"Запускаем выполнение задачи по отправке уведомлений")
                notif = Notif(
                    task_id=task['task_id'],
                    status_notif=task['status_id'],
                    repeat_notification=task['repeat'],
                    date_start=task['date_start']
                )
                notif.start_notif()

                # Добавляем запись о последнем запуске задачи в Google Sheets
                date_now = dt.now()
                WorkGoogle().set_tasks_last_start(
                    row=task['row_task_on_sheet'],
                    value=date_now.strftime('%Y-%m-%d %H:%M:%S')
                )
            except Exception as e:
                logger.error(f"Произошла ошибка при выполнении задачи по отправке уведомлений {e}")
        elif task['task_id'] == '2' and task['can_run_task']:
            logger.warning(f"Запускаем выполнение задачи по отправке Заказов поставщикам")
            sup_reorder = ReOrder(
                task_id=task['task_id'],
                status_reorder=task['status_id'],
                # repeat_notification=task['repeat'],
                date_start=task['date_start'],
                retry_count=task['retry_count']
            )
            sup_reorder.supplier_reorder()

            # Добавляем запись о последнем запуске задачи в Google Sheets
            date_now = dt.now()
            WorkGoogle().set_tasks_last_start(
                row=task['row_task_on_sheet'],
                value=date_now.strftime('%Y-%m-%d %H:%M:%S')
            )

        elif not task['can_run_task']:
            logger.info(f"Интервал времени для запуска задачи №{task['task_id']} - \"{task['task_name']}\" не прошёл.")
        else:
            logger.error(f"Нет алгоритма для отработка этой задачи. Необходимо связаться с заказчиком")
    logger.info(f"... Окончание работы программы")
    return


if __name__ == "__main__":
    main()
