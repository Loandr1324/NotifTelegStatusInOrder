import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from config import AUTH_GOOGLE
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
        return dict(zip(params_head,setting[1:][0]))

    def get_tasks(self) -> list[dict]:
        """
        Получаем вторую строку с первой страницы и возвращаем их в словаре с предварительно заданными ключами
        :return: dict
            ключи словаря [
            "task_id" - Номер события,
            "task_name" - Наименование события
            "task_interval" - Интервал запуска, сек
            "status_name" - Наименование статуса позиции
            "status_id" - Идентификатор статуса позиции
            "date_start" - Дата с которой загружаем заказы
            "repeat" - Требуется ли отправлять повторные уведомления
            "temp_not1" - Шаблон первичного уведомления
            "temp_not2" - Шаблон повторного уведомления
            ]
        """
        params_head = [
            "task_id", "task_name", "task_interval",
            "status_name", "status_id", "date_start", "repeat", "temp_not1", "temp_not2"
        ]
        notif = []
        tasks = self._rw_google.read_sheet(1)
        for val in tasks[1:]:
            params_user_notif = dict(zip(params_head, val))
            params_user_notif = self.convert_value(params_user_notif)
            notif += [params_user_notif]
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
            'tel_chat_id': Идентификатор чата для отправки уведомления (бот должен быть в этом чате)
            },
            ...,
            ]
        """
        list_users_notif = self._rw_google.read_sheet(2)
        params_head = ["task_id", "status_id", "user_name", "user_id", "tel_chat_id"]
        for val in list_users_notif[1:]:
            params_user_notif = dict(zip(params_head, val))
            self.users_notif += [params_user_notif]

    def get_chat_id_notif(self, task_id: str, status_id: str, user_id: str):
        """

        :return:
        """
        user_notif = list(filter(lambda v:
                                 v['task_id'] == task_id and
                                 v['status_id'] == status_id and
                                 v['user_id'] == user_id,
                                 self.users_notif))
        logger.info(user_notif[0]['tel_chat_id'])
        chats_id = user_notif[0]['tel_chat_id'].replace(' ', '').split(',')
        return chats_id

    @staticmethod
    def convert_date_notif(date: str) -> datetime.datetime:
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
    def convert_repeat_notif(repeat_value: str) -> bool:
        """
        Преобразуем значения
        Если дата больше года, то берем заказы за последние 364 дня.
        :param repeat_value: Строка со значением повтора уведомления. Допустимые значения: "да" или "нет"
        :return: bool
        """
        repeat_value = repeat_value.lower()
        return True if repeat_value == "да" else False if repeat_value == "нет" else None

    def convert_value(self, dict_params: dict) -> dict:
        """
        Преобразовывает значения словаря полученной задачи 'date_start' и 'repeat' в нужный формат
        'date_start' -> datetime.datetime
        'repeat' -> bool
        :param dict_params: Словарь с ключами 'date_start' и 'repeat'
        :return: Преобразованный словарь
        """
        dict_params['date_start'] = self.convert_date_notif(dict_params['date_start'])
        dict_params['repeat'] = self.convert_repeat_notif(dict_params['repeat'])
        return dict_params
