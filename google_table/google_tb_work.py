import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from config import AUTH_GOOGLE, CHAT_DEFAULT
from loguru import logger
import datetime as dt


class RWGoogle:
    """
    Класс для чтения и запись данных из(в) Google таблицы(у)
    """
    def __init__(self):
        self.client_id = AUTH_GOOGLE['GOOGLE_CLIENT_ID']
        self.client_secret = AUTH_GOOGLE['GOOGLE_CLIENT_SECRET']
        self._scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        self._credentials = ServiceAccountCredentials.from_json_keyfile_name(
            'google_table/credentials.json', self._scope
            # 'credentials.json', self._scope
        )
        self._credentials._client_id = self.client_id
        self._credentials._client_secret = self.client_secret
        self._gc = gspread.authorize(self._credentials)
        self.key_wb = AUTH_GOOGLE['KEY_WORKBOOK']

    def read_sheets(self) -> list[str]:
        """
        Получает данные по всем страницам Google таблицы и возвращает список страниц в виде списка строк
        self.key_wb: id google таблицы.
            Идентификатор таблицы можно найти в URL-адресе таблицы.
            Обычно идентификатор представляет собой набор символов и цифр
            после `/d/` и перед `/edit` в URL-адресе таблицы.
        :return: list[str].
            [
            'Имя 1-ой страницы',
            'Имя 2-ой страницы',
            ...
            'Имя последней страницы'
            ]
        """
        result = []
        try:
            worksheets = self._gc.open_by_key(self.key_wb).worksheets()
            result = [worksheet.title for worksheet in worksheets]
        except gspread.exceptions.APIError as e:
            logger.error(f"Ошибка при получении списка имён страниц: {e}")
        except Exception as e:
            logger.error(f"Ошибка при получении списка имён страниц: {e}")
        return result

    def read_sheet(self, worksheet_id: int) -> list[list[str]]:
        """
        Получает данные из страницы Google таблицы по её идентификатору и возвращает значения в виде списка списков
        self.key_wb: id google таблицы.
            Идентификатор таблицы можно найти в URL-адресе таблицы.
            Обычно идентификатор представляет собой набор символов и цифр
            после `/d/` и перед `/edit` в URL-адресе таблицы.
        :return: List[List[str].
        """
        sheet = []
        try:
            sheet = self._gc.open_by_key(self.key_wb).get_worksheet(worksheet_id)
        except gspread.exceptions.APIError as e:
            logger.error(f"Ошибка при получении списка настроек: {e}")
        except Exception as e:
            logger.error(f"Ошибка при получении списка имён страниц: {e}")
        return sheet.get_all_values()

    def save_cell(self, worksheet_id: int, row: int, col: int, value: str):
        """Записываем данные в ячейку"""
        try:
            sheet = self._gc.open_by_key(self.key_wb).get_worksheet(worksheet_id)
            return sheet.update_cell(row, col, value)

        except gspread.exceptions.APIError as e:
            logger.error(f"Ошибка при получении списка настроек: {e}")

        except Exception as e:
            logger.error(f"Ошибка при получении списка имён страниц: {e}")


class WorkGoogle:
    def __init__(self):
        self._rw_google = RWGoogle()
        self.users_notif = []

    def get_setting(self) -> dict:
        """
        Получаем вторую строку с первой страницы и возвращаем их в словаре с предварительно заданными ключами
        :return: dict
            ключи словаря[
            "work_open" - время начала работы магазина (скрипта),
            "work_close" - время окончания работы магазина (скрипта),
            "api" - данные для доступа по API (на данный момент не передаются, но колонка есть)
            ]
        """
        setting = self._rw_google.read_sheet(0)
        # values = self.read_sheet(AUTH_GOOGLE['KEY_WORKBOOK'], 0)
        params_head = ["work_open", "work_close", "auth_api"]
        return dict(zip(params_head, setting[1:][0]))

    def get_tasks(self) -> list[dict]:
        """
        Получаем вторую строку с первой страницы и возвращаем их в словаре с предварительно заданными ключами
        :return: dict
            ключи словаря [
            "task_id" - Номер события,
            "task_name" - Наименование события
            "time_start" - Время начала работы скрипта
            "time_finish" - Время окончания работы скрипта
            "task_interval" - Интервал запуска, сек
            "status_name" - Наименование статуса позиции
            "status_id" - Идентификатор статуса позиции
            "date_start" - Дата с которой загружаем заказы
            "repeat" - Требуется ли отправлять повторные уведомления
            "retry_count" - Количество попыток оформления заказов поставщикам
            "last_start" - последний старт задачи
            "temp_not1" - Шаблон первичного уведомления
            "temp_not2" - Шаблон повторного уведомления
            "allowed_suppliers" - Разрешённые идентификаторы поставщиков
            "type_notif" - Тип уведомления
            "select_alg" - Алгоритм работы
            "row_task_on_sheet" - Номер строки задачи на листе Google sheets
            ]
        """
        params_head = [
            "task_id", "task_name", "time_start", "time_finish", "task_interval", "status_name", "status_id",
            "date_start", "repeat", "retry_count", "last_start", "temp_not1", "temp_not2", "allowed_suppliers",
            "type_notif", "select_alg"
        ]
        notif = []
        i = 2  # Первоначальный Номер строки считываемой задачи
        tasks = self._rw_google.read_sheet(1)
        for val in tasks[1:]:
            params_user_notif = dict(zip(params_head, val))
            params_user_notif = self.convert_value(params_user_notif)
            params_user_notif['row_task_on_sheet'] = i
            notif += [params_user_notif]
            i += 1
        return notif

    def get_users_notif(self):
        """
        Получаем весь список пользователей по уведомлениям
        :return: list[dict]
            Возвращается список словарей с данными получателей уведомлений
            [
            {
            "task_id" - Идентификатор уведомления,
            'status_id': Идентификатор статуса по которому отправляется уведомления,
            'user_name': ФИО получателя уведомления,
            'user_id': Идентификатор пользователя на платформе ABCP,
            "manager_id": Идентификатор менеджера на платформе ABCP,
            'tel_chat_id': Идентификатор чата для отправки уведомления (бот должен быть в этом чате)
            },
            ...,
            ]
        """
        list_users_notif = self._rw_google.read_sheet(2)
        params_head = ["task_id", "status_id", "user_name", "user_id", "manager_id", "tel_chat_id", "reorder_auto"]
        for val in list_users_notif[1:]:
            params_user_notif = dict(zip(params_head, val))
            self.users_notif += [params_user_notif]

    def get_chat_id_notif(self,
                          task_id: str = '1',
                          status_id: str = '144931',
                          search_id: str = '',
                          client_id: str = '',
                          type_search: str = 'user_id') -> list:
        """
        Получаем список идентификаторов чатов телеграмм
        :return: list[str]
        """
        # Проверяем есть ли чат по соответствию менеджера и клиента
        user_notif = [v for v in self.users_notif if
                      v['task_id'] == task_id
                      and v['status_id'] == status_id
                      and search_id in v['manager_id']
                      and client_id in v['user_id']
                      ]
        # Если нет, то проверяем по типу поиска переданного в метод
        if not user_notif:
            user_notif = [v for v in self.users_notif if
                          v['task_id'] == task_id and v['status_id'] == status_id and search_id in v[type_search]]

        # Если нет, то проверяем по идентификатору клиента
        if not user_notif:
            user_notif = [v for v in self.users_notif if
                          v['task_id'] == task_id and v['status_id'] == status_id and client_id in v['user_id']]

        if not user_notif:
            return []

        logger.info(user_notif[0]['tel_chat_id'])
        chats_id = user_notif[0]['tel_chat_id'].replace(' ', '').split(',')
        return chats_id + CHAT_DEFAULT
        # return CHAT_DEFAULT  # TODO Удалить после тестов

    def get_supplier_params(self) -> list[dict]:
        """
        Получаем вторую строку с четвёртой страницы и возвращаем их в словаре с предварительно заданными ключами
        :return: list[dict]
            ключи словаря[
            "supplier_id" - идентификатор поставщика на платформе ABCP,
            "supplier_name" - наименование поставщика,
            "reorder_auto" - разрешение на оформление перезаказа,
            ... параметры для заказа (уникальные для каждого поставщика)
            ]
        """
        suppliers_params = []
        sheet_suppliers_setting = self._rw_google.read_sheet(3)
        params_head = ["supplier_id", "supplier_name", "reorder_auto"]
        date_now = dt.datetime.now().strftime('%Y-%m-%d')
        date_tomorrow = (dt.datetime.now() + dt.timedelta(days=1)).strftime('%Y-%m-%d')
        for val in sheet_suppliers_setting[1:]:
            row = dict(zip(params_head, val[:3]))
            row['params'] = {}
            params_order_list = val[3:19]
            params_position_list = val[19:]
            row['params']['orderParams'] = {
                params_order_list[i]:
                    int(params_order_list[i + 1])
                    if params_order_list[i + 1].isdigit() and not params_order_list[i + 1].startswith("000")
                    else params_order_list[i + 1]
                for i in range(0, len(params_order_list), 2) if params_order_list[i].lower() != 'нет'
            }

            row['params']['positionParams'] = {
                params_position_list[i]: int(params_position_list[i + 1]) if params_position_list[i + 1].isdigit() else
                params_position_list[i + 1]
                for i in range(0, len(params_position_list), 2) if params_position_list[i].lower() != 'нет'
            }

            if 'shipmentDate' in row['params']['orderParams']:
                row['params']['orderParams']['shipmentDate'] = date_tomorrow
            if 'shipmentDateDelivery' in row['params']['orderParams']:
                row['params']['orderParams']['shipmentDateDelivery'] = date_tomorrow

            row['reorder_auto'] = self.convert_yes_no_to_bool(str(row['reorder_auto']))

            suppliers_params += [row]
        return suppliers_params

    def get_user_reorder_auto(self) -> list[dict]:
        """
        Получаем вторую строку с пятой страницы и возвращаем их в словаре с предварительно заданными ключами
        :return: dict
            ключи словаря[
            "user_id" - Идентификатор пользователя на платформе ABCP,
            "manager_id" - Идентификатор менеджера на платформе ABCP,
            "user_name" - ФИО пользователя,
            "user_reorder_auto" - разрешение на оформление автоперезаказа
            ]
        """
        users_reorder_auto = []
        sheet_users_reorder_auto = self._rw_google.read_sheet(4)
        params_head = ["user_id", "manager_id", "user_name", "user_reorder_auto"]

        for val in sheet_users_reorder_auto[1:]:
            row = dict(zip(params_head, val))
            row['user_reorder_auto'] = self.convert_yes_no_to_bool(str(row['user_reorder_auto']))
            users_reorder_auto += [row]
        return users_reorder_auto

    def set_tasks_last_start(self, row: int, value: str) -> None:
        """Записываем данные о последнем запуске задачи
        :param row: Номер строки задачи
        :param value: Значение даты последнего запуска в формате '%Y-%m-%d %H:%M:%S'
        """
        self._rw_google.save_cell(1, row, 11, value)

    @staticmethod
    def convert_date(date: str) -> datetime.datetime:
        """
        Преобразуем дату полученной из Google таблицы в необходимый формат
        Если дата больше года, то берем заказы за последние 364 дня.
        :param date: Строка с датой в формате '%d.%m.%Y'
        :return: Дата в формате datetime.datetime(2023, 11, 1, 0, 0)
        """
        date_start = dt.datetime.strptime(date, '%d.%m.%Y')
        if (dt.datetime.utcnow() - date_start).days > 365:
            date_start = dt.datetime.utcnow() - dt.timedelta(days=364)
        return date_start

    @staticmethod
    def convert_time(time: str) -> datetime.time:
        """
        Преобразуем время полученное из Google таблицы в необходимый формат
        :param time: Строка с датой в формате '%H-%M'
        :return: Дата в формате datetime.time(HH, MM)
        """
        return datetime.datetime.strptime(time, '%H-%M').time()

    @staticmethod
    def convert_date_time(date_time: str) -> datetime.datetime:
        """
        Преобразуем время полученное из Google таблицы в необходимый формат
        :param date_time: Строка с датой в формате '%Y-%m-%d %H:%M:%S'
        :return: Дата в формате datetime.datetime(2023, 11, 1, 0, 0, 0)
        """
        return datetime.datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S') if date_time else "2024-01-01 00:00:01"

    @staticmethod
    def convert_yes_no_to_bool(value: str) -> bool:
        """
        Преобразуем значения
        Если дата больше года, то берем заказы за последние 364 дня.
        :param value: Строка со значением повтора уведомления. Допустимые значения: "да" или "нет"
        :return: bool
        """
        value = value.lower()
        return True if value == "да" else False if value == "нет" else None

    def convert_value(self, dict_params: dict) -> dict:
        """
        Преобразовывает значения словаря полученной задачи 'date_start', 'last_start', 'time_start', 'time_finish' и
        'repeat' в нужный формат
        'date_start' -> datetime.datetime
        'last_start' -> datetime.datetime
        'time_start' -> datetime.time
        'time_finish -> datetime.time
        'repeat' -> bool
        :param dict_params: Словарь с ключами 'date_start', 'last_start', 'time_start', 'time_finish' и 'repeat'
        :return: Преобразованный словарь
        """
        dict_params['date_start'] = self.convert_date(dict_params['date_start'])
        dict_params['last_start'] = self.convert_date_time(dict_params['last_start'])
        dict_params['time_start'] = self.convert_time(dict_params['time_start'])
        dict_params['time_finish'] = self.convert_time(dict_params['time_finish'])
        dict_params['repeat'] = self.convert_yes_no_to_bool(dict_params['repeat'])
        return dict_params
