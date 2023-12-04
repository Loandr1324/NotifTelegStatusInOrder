from telegram.send_teleg import *
import datetime
import asyncio
from aioabcpapi import Abcp
from data_notif.csv_work import WorkCSV
from loguru import logger
from config import AUTH_API

host, login, password = AUTH_API['HOST_API'], AUTH_API['USER_API'], AUTH_API['PASSWORD_API']
api = Abcp(host, login, password)


class Notif:
    def __init__(self, task_id, status_notif, repeat_notification, date_start, wk_g):
        self.user_notif = dict.fromkeys(['id', 'full_name', 'type_order', 'type_msg', 'chats_id'])
        self.order = None
        self.product = None
        self.db_order_is_notif = False
        self.db_product_is_notif = False
        self.task_id = task_id
        self.status_notif = status_notif
        self.statuses_notif = None
        self.list_managers = None
        self.repeat_notification = repeat_notification
        self.work_csv = WorkCSV()
        self.telegram = NotifTelegram()
        self.work_google = wk_g
        self.date_start = date_start

    async def staff_notif(self):
        """
        Асинхронно получает список менеджеров из API ABCP и сохраняет его в атрибуте `list_managers`.
        """
        try:
            self.list_managers = await api.cp.admin.staff.get()
        except Exception as e:
            logger.error(f"Ошибка при получении списка менеджеров: {e}")

    async def get_order_by_status_notif(self):
        """
        Асинхронно получает список заказов с определенным статусом для уведомлений из API ABCP и возвращает его.
        :return: Список заказов с определенным статусом для уведомлений.
        """
        orders = []
        try:
            orders = await api.cp.admin.orders.get_orders_list(
                status_code=self.status_notif, date_created_start=self.date_start, format='p'
            )
        except Exception as e:
            logger.error(f"Ошибка при получении списка заказов: {e}")
        return orders

    def create_message(self):
        """
        Создает текст сообщения на основе переданного заказа и текущего статуса уведомления и сохраняет его
        в аттрибут self.message
        :raises ValueError: Если сообщение для передаваемого статуса не определено.
        """
        try:
            self.telegram.message_sup_order_cancel(
                self.order['number'], self.product, self.user_notif
            )
        except Exception as e:
            logger.error(f"Ошибка при создании сообщения: {e}")

    def get_chat_id_for_notif(self):
        """
        Получает список chat_id Telegram для уведомлений на основе
        текущего статуса уведомления и идентификатора пользователя.
        :return: Список chat_id Telegram для уведомлений.
        """
        self.user_notif['chats_id'] = self.work_google.get_chat_id_notif(
            self.task_id,
            self.status_notif,
            self.user_notif['id']
        )

    def send_message_telegram(self):
        """
        Отправляет сообщение в чат Telegram для каждого chat_id из списка chats_id.
        """
        any_result = False
        for chat_id in self.user_notif['chats_id']:
            logger.info(f'Отправляем сообщение в чат {chat_id}')
            result = self.telegram.send_massage_chat(chat_id)
            any_result = result or any_result
        # Записываем в таблицу отправленных уведомлений, если хотя бы одно уведомление отправлено
        if any_result and not self.db_product_is_notif:
            self.add_to_data_notif_position()

    def get_notif_db(self):
        """
        Получает информацию о наличии отправленных уведомлений в базе данных.
        """
        # Получаем из БД отправленные уведомления по товару
        self.db_product_is_notif = self.work_csv.filter(
            id_order=self.order['number'],
            id_position=self.product['id'],
            id_status=self.status_notif
        )

    def add_to_data_notif_position(self):
        """Добавляем данные по позиции в список отправленных уведомлений"""
        self.work_csv.add_to_data(
            id_status=self.status_notif,
            id_order=self.order['number'],
            id_position=self.product['id'],
            date_notif=datetime.datetime.utcnow()
        )

    def check_last_notif_position(self):
        """
        Проверяет возможность отправки уведомления по текущей позиции.
        :return: True, если уведомление уже было отправлено ранее, False, если уведомление не было отправлено.
        """
        result = True
        self.get_notif_db()
        # Если нет в базе уведомлений товаров и заказов
        if not self.db_product_is_notif:
            # Отправляем уведомления в телеграм и получаем результат
            self.user_notif['msg_type'] = "primary"
            result = False
        elif self.db_product_is_notif and self.repeat_notification:
            self.user_notif['msg_type'] = "secondary"
            result = False
        return result

    async def get_user_notif(self):
        """
        Асинхронно получает идентификатор пользователя для уведомлений на основе текущего заказа.
        """
        if self.order['managerId'] != '0':
            user_id = list(filter(lambda v: v['id'] == self.order['managerId'], self.list_managers))
            self.user_notif['id'] = user_id[0]['contractorId']
            self.user_notif['full_name'] = self.order['userName']
            self.user_notif['type_order'] = 'user'
        else:
            # user_id = list(filter(lambda v: v['contractorId'] == self.order['userId'], self.list_managers))
            self.user_notif['id'] = self.order['userId']
            self.user_notif['full_name'] = self.order['userName']
            self.user_notif['type_order'] = 'stock'

    async def send_notif_by_status(self):
        """
        Асинхронно отправляет уведомления в Telegram в соответствии с текущим статусом уведомления.
        """
        # получаем список заказов по статусу для уведомлений
        orders = {}
        try:
            logger.info(f"Получаем список заказов с необходимыми статусами")
            orders = await self.get_order_by_status_notif()
        except Exception as e:
            logger.error(f"Ошибка при получении заказов с платформы ABCP: {e}")
        logger.info(f"Получили для уведомлений {orders['count']} заказ(ов)")

        if not orders['items']:
            return logger.info("Заказов для уведомления по указанному статусу нет")

        logger.info(f"Получаем актуальный список менеджеров с платформы ABCP")
        await self.staff_notif()

        logger.info(f"Считываем из Google таблицы соответствия менеджеров и чатов для уведомлений")
        self.work_google.get_users_notif()

        for self.order in orders['items']:
            logger.info('Обрабатываем позиции заказа №' + self.order['number'])
            products_notif = list(filter(
                lambda v: v['statusCode'] == self.status_notif, self.order['positions']))
            for self.product in products_notif:
                # Проверяем можно ли отправить уведомление по этому заказу
                if self.check_last_notif_position():
                    logger.info(f"Не требуется отправка сообщения по заказу №{self.order['number']} "
                                f"со статусом: {self.status_notif}")
                    continue

                await self.get_user_notif()  # Определяем менеджера для отправки уведомлений по позиции

                # Получаем чаты отправки уведомлений
                self.get_chat_id_for_notif()
                logger.info(f"Определили менеджера  и чат для отправки уведомления {self.user_notif}")

                # Создаём текст сообщения согласно статуса и заказа с позициями
                self.create_message()
                # Отправляем сообщения по статусу
                self.send_message_telegram()
                logger.info(f"Сообщение отправлено по заказу №{self.order['number']} со статусом: {self.status_notif}")
        self.work_csv.add_data_file()
        await api.close()

    def start_notif(self):
        asyncio.run(self.send_notif_by_status())
