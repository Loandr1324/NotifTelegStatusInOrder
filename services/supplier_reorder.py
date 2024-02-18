from telegram.send_teleg import *
from datetime import datetime as dt
import asyncio
from api_abcp.abcp_work import WorkABCP
from data_notif.csv_work import WorkCSV
from loguru import logger
from config import FILENAME_REORDER_ERROR
from google_table.google_tb_work import WorkGoogle


class ReOrder:
    def __init__(self, task_id, status_reorder, date_start, retry_count):
        self.work_abcp: WorkABCP = WorkABCP()
        self.work_google = WorkGoogle()
        self.work_csv_error = WorkCSV(FILENAME_REORDER_ERROR)
        self.telegram = NotifTelegram()
        self.task_id: str = task_id
        self.status_reorder: str = status_reorder
        self.date_start: str = date_start
        self.retry_count: int = int(retry_count)
        self.orders = {}
        self.positions_reorder_suppliers = {}
        self.allowed_users = []
        self.allowed_managers = []
        self.all_suppliers_for_reorder = []
        self.allowed_suppliers = []
        self.blocked_positions_suppliers = {}
        self.reorder_error = {}
        self.reorder_by_item_suppliers = []
        self.error_positions_notify = []

    def get_users_manager_for_order(self):
        """Получаем список менеджеров, для которых разрешёно оформлять заказы поставщикам из базы данных"""
        logger.info("Получаем список менеджеров для которых разрешёно оформлять заказы поставщикам")
        users_reorder = self.work_google.get_user_reorder_auto()
        self.allowed_users = {user['user_id'] for user in users_reorder if user['user_reorder_auto']}
        self.allowed_managers = {
            user['manager_id']: [
                user['user_id'], user['user_name']
            ] for user in users_reorder if user['user_reorder_auto']
        }

    def get_suppliers_for_order(self):
        """
        Получаем список поставщиков, для которых разрешено оформлять заказы,
        с параметрами для оформления заказа поставщику из базы данных
        """
        logger.info(f"Получаем список поставщиков для которых разрешено оформлять заказы поставщикам")
        self.all_suppliers_for_reorder = self.work_google.get_supplier_params()
        self.allowed_suppliers = {
            supplier['supplier_id'] for supplier in self.all_suppliers_for_reorder if supplier['reorder_auto']
        }

    def add_count_error_position(self, supplier_id):
        """Добавляем данные по предыдущим ошибкам позиций поставщиков"""
        logger.info("Добавляем данные по предыдущим ошибкам позиций поставщиков")
        positions = []
        for position in self.positions_reorder_suppliers[supplier_id]['positions']:
            last_error_position = self.work_csv_error.filter(
                type_filter='reorder_error',
                supplier_id=supplier_id,
                position_id=position['id'])

            if not last_error_position:
                position['count_error'] = '0'
            else:
                delta = dt.now() - dt.strptime(last_error_position[0]['data_error'], '%Y-%m-%d %H:%M:%S')
                position['count_error'] = last_error_position[0]['count_error']

                if delta.total_seconds() < 300 or int(last_error_position[0]['count_error']) >= self.retry_count:
                    # Передаём последние данные об ошибке для дальнейшей записи в БД
                    self.work_csv_error.add_to_data(
                        type_data='reorder_error',
                        supplier_id=supplier_id,
                        position_id=last_error_position[0]['position_id'],
                        count_error=last_error_position[0]['count_error'],
                        data_error=last_error_position[0]['data_error'],
                        text_error=last_error_position[0]['text_error'],
                    )
                    continue

                elif int(last_error_position[0]['count_error']) < self.retry_count:
                    position['count_error'] = last_error_position[0]['count_error']
            positions.append(position)

        if positions:
            self.positions_reorder_suppliers[supplier_id]['positions'] = positions
        else:
            del self.positions_reorder_suppliers[supplier_id]

    def check_position_for_error(self):
        """Добавляем данные по предыдущим ошибкам по поставщикам"""
        for supplier in list(self.positions_reorder_suppliers):
            self.add_count_error_position(supplier)

    async def sort_position_for_order(self, our_stock):
        """
        Разделяет позиции полученных заказов на разрешенные к заказу и формирует словарь с данными для заказов.
        Ключами в словаре являются идентификаторы поставщиков на платформе ABCP.
        Для каждого ключа доступен словарь с позициями для оформления заказа и соответствующими параметрами.
        Игнорируем свой склад.

        Структура словаря self.positions_reorder_suppliers:
        {
            '[Идентификатор поставщика на платформе ABCP]': {
                'positions': [Список данных по каждой позиции]:,
                'params': {
                    'orderParams': {Словарь с параметрами заказа и их значениями},
                    'positionParams': {Словарь с параметрами позиций и их значениями}
                }
                'data_message':
            },
            ...
        }

        Так же создаём словарь с заблокированными поставщиками для дальнейшего запроса параметров для заказа
        self.blocked_positions_suppliers:
        {
            '[Идентификатор поставщика на платформе ABCP]': {
                'id_positions': [Список идентификаторов позиций для заказа]}
        }

        :param our_stock: list[str - Идентификаторы своих складов на платформе ABCP]
        """
        logger.info(f"Разделяем позиции, по разрешённым поставщикам")
        for order in self.orders['items']:
            # Проверяем присутствует ли менеджер в списке разрешённых
            if (order['managerId'] == '0' and order['userId'] not in self.allowed_users) or (
                    order['managerId'] != '0' and order['managerId'] not in self.allowed_managers):
                continue

            # Выбираем идентификатор и имя пользователя
            if order['managerId'] != '0':
                user_id = self.allowed_managers[order['managerId']][0]
                user_name = self.allowed_managers[order['managerId']][1]
                type_search = 'manager'
            else:
                user_id = order['userId']
                user_name = order['userName'][10:]
                type_search = 'user'

            # Подставляем данные для заказа по позициям
            for position in order['positions']:
                # Проверяем соответствие статуса позиции статусу для заказа
                if position['statusCode'] != self.status_reorder:
                    continue
                # Проверяем присутствует ли поставщик в списке разрешённых
                if position['distributorId'] not in self.allowed_suppliers:
                    # Записываем в лог запрещённых поставщиков
                    if position['distributorId'] not in our_stock:
                        logger.error(
                            f"Такого поставщика нет в списке "
                            f"разрешённых {position['distributorId']} - {position['distributorName']}"
                        )
                        self.blocked_positions_suppliers.setdefault(
                            position['distributorId'], []
                        ).append(position['id'])
                    continue

                # Добавляем позиции поставщика в словарь для заказа
                self.positions_reorder_suppliers.setdefault(
                    position['distributorId'], {'positions': []}
                )['positions'].append({
                    'id': position['id'],
                    'order': order['number'],
                    'brand': position['brand'],
                    'number': position['number'],
                    'description': position['description'],
                    'distributorName': position['distributorName'],
                    'userId': user_id,
                    'userName': user_name,
                    'type_search_user': type_search
                })

    def add_param_for_reorder(self):
        """Добавляем параметры для оформления Заказов поставщику"""
        for supplier in self.all_suppliers_for_reorder:
            supplier_id = supplier['supplier_id']
            supplier_data = self.positions_reorder_suppliers.get(supplier_id)
            if supplier_data:
                supplier_data['orderParams'] = supplier['params']['orderParams']
                positions = supplier_data['positions']
                position_for_order = []
                for position in positions:
                    item = {'id': position['id']}
                    item.update(supplier['params']['positionParams'])
                    if 'comment' in item:
                        item['comment'] = position['order']
                    position_for_order.append(item)

                # Обновление данных в словаре self.positions_reorder_suppliers
                self.positions_reorder_suppliers[supplier_id] = supplier_data
                self.positions_reorder_suppliers[supplier_id]['position_for_order'] = position_for_order

    async def send_orders_to_supplier(self):
        """Отправка заказов поставщику и запись ошибок при отправке"""
        for supplier in self.positions_reorder_suppliers:
            logger.info(f" Оформляем заказы пакетами по поставщику: {supplier}")
            result = await self.work_abcp.create_order_supplier(
                order_params=self.positions_reorder_suppliers[supplier]['orderParams'],
                positions=self.positions_reorder_suppliers[supplier]['position_for_order']
            )
            if 'error' in result:
                self.reorder_error[supplier] = self.positions_reorder_suppliers[supplier]
                self.reorder_error[supplier]['error'] = result['error']
            else:
                logger.info(f"Оформлен заказ поставщику пакетом позиций: {supplier}")
                logger.info(f"Результат оформления: {result}")

    async def work_to_error_supplier(self):
        """Обработка ошибок при отправке заказов поставщику пакетами и попытка отправки по позициям"""

        for supplier in self.reorder_error:
            for i, position in enumerate(self.reorder_error[supplier]['positions']):
                if int(position['count_error']) < self.retry_count - 1:
                    count_error = str(int(position['count_error']) + 1)
                    # Заполняем данные об ошибке для дальнейшей записи в БД
                    self.work_csv_error.add_to_data(
                        type_data='reorder_error',
                        supplier_id=supplier,
                        position_id=position['id'],
                        count_error=count_error,
                        data_error=dt.now().strftime('%Y-%m-%d %H:%M:%S'),
                        text_error=self.reorder_error[supplier]['error']
                    )

                else:
                    logger.info(f" Оформляем заказы по позициям по поставщику: {supplier} и позиции: {position['id']}")
                    if self.reorder_error[supplier]['position_for_order'][i]['id'] == position['id']:
                        result = await self.work_abcp.create_order_supplier(
                            order_params=self.reorder_error[supplier]['orderParams'],
                            positions=self.reorder_error[supplier]['position_for_order'][i]
                        )
                        if 'error' in result:
                            self.work_csv_error.add_to_data(
                                type_data='reorder_error',
                                supplier_id=supplier,
                                position_id=position['id'],
                                count_error=str(self.retry_count),
                                data_error=dt.now().strftime('%Y-%m-%d %H:%M:%S'),
                                text_error=result['error']
                            )
                            # Добавляем позиции в список для уведомлений менеджерам
                            position['error'] = result['error']
                            self.error_positions_notify.append(position)

                        else:
                            logger.info(f"Оформлен заказ поставщику: {supplier} по позиции: {position['id']}")
                            logger.info(f"Результат оформления: {result}")

        self.work_csv_error.add_data_file(mode="w")
        return

    def send_error_message(self):
        """
        Оправляем сообщение об ошибке менеджерам в телеграм
        :return:
        """
        logger.info(f"Считываем из Google таблицы соответствия менеджеров и чатов для уведомлений")
        self.work_google.get_users_notif()

        for position in self.error_positions_notify:
            # Получаем чаты для уведомлений по позиции
            user_notif = {'chats_id': self.work_google.get_chat_id_notif(
                task_id='', status_id='', search_id=position['userId'], type_search=position['type_search_user']
            )}
            logger.info(f"Отправляем уведомление об ошибке при оформление позиции у поставщика менеджеру")
            # Создаём текст сообщения согласно статуса и заказа с позициями
            # self.create_message()
            user_notif['msg_type'] = 'error_reorder'
            try:
                self.telegram.message_sup_order_cancel(
                    position['order'], position, user_notif
                )
            except Exception as e:
                logger.error(f"Ошибка при создании сообщения: {e}")
                return

            # Отправляем сообщения по статусу
            for chat_id in user_notif['chats_id']:
                logger.info(f'Отправляем сообщение в чат {chat_id}')
                result = self.telegram.send_massage_chat(chat_id)

    async def reorder(self):
        """Оформляем заказы поставщикам по заданному статусу"""
        # Получаем список заказов для оформления заказов поставщикам
        self.orders = await self.work_abcp.get_order_by_status(status=self.status_reorder, date_create=self.date_start)

        # Если заказов нет, то завершаем асинхронную сессию и возвращаемся
        if self.orders['count'] == '0':
            return await self.work_abcp.api_abcp.close()

        # Задаём список менеджеров, которым разрешено оформление заказов поставщикам
        self.get_users_manager_for_order()

        # Задаём список поставщиков, по которым разрешено оформлять заказы
        self.get_suppliers_for_order()

        # Разделяем позиции заказов и создаём словарь с данными для оформления заказов
        await self.sort_position_for_order(['1599030'])

        logger.info(f"Поставщики для заказа {self.positions_reorder_suppliers}")
        logger.error(f"Заблокированные поставщики {self.blocked_positions_suppliers}")

        # Проверяем позиции для заказа на ранее допущенные ошибки при оформлении заказов и подставляем количество ошибок
        self.check_position_for_error()
        logger.info(f"Поставщики для заказа после проверки на ошибки {self.positions_reorder_suppliers}")

        # Добавляем параметры по поставщикам для оформления заказа
        self.add_param_for_reorder()

        # Оформляем заказы у поставщиков пакетами
        await self.send_orders_to_supplier()

        # Отрабатываем ошибки
        if self.reorder_error:
            await self.work_to_error_supplier()
            if self.error_positions_notify:
                self.send_error_message()

        await self.work_abcp.api_abcp.close()

    def supplier_reorder(self):
        asyncio.run(self.reorder())
