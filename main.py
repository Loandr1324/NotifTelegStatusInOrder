# Author Loik Andrey mail: loikand@mail.ru
from config import FILE_NAME_LOG
from notification import Notif
from google_table.google_tb_work import WorkGoogle

from loguru import logger

# TODO Сделать проверку рабочего времени и запустить скрипт

# Задаём параметры логирования
logger.add(FILE_NAME_LOG,
           format="{time:DD/MM/YY HH:mm:ss} - {file} - {level} - {message}",
           level="INFO",
           rotation="1 month",
           compression="zip")


def main() -> None:

    logger.info(f"... Запуск программы")
    tasks = []
    wk_g = ''
    try:
        logger.info(f"Получаем список задач")
        wk_g = WorkGoogle()
        tasks = wk_g.get_tasks()
    except Exception as e:
        logger.error(f"Ошибка при получении списка задач из гугл таблицы: {e}")

    for task in tasks:
        try:
            logger.info(f"Запускаем выполнение задачи по отправке уведомлений")
            notif = Notif(
                task_id=task['task_id'],
                status_notif=task['status_id'],
                repeat_notification=task['repeat'],
                date_start=task['date_start'],
                wk_g=wk_g
            )
            notif.start_notif()
        except Exception as e:
            logger.error(f"Произошла ошибка при выполнении задачи по отправке уведомлений {e}")
    logger.info(f"... Окончание работы программы")
    return


if __name__ == "__main__":
    main()
