from telegram.send_teleg import *
import datetime
import asyncio
from aioabcpapi import Abcp
from data_notif.csv_work import WorkCSV
from loguru import logger
from config import AUTH_API, FILENAME_DATA_NOTIF
from google_table.google_tb_work import WorkGoogle


class WorkABCP:
    def __init__(self):
        self.api_abcp = Abcp(AUTH_API['HOST_API'], AUTH_API['USER_API'], AUTH_API['PASSWORD_API'])

    async def get_order_by_status(self, status, date_create):
        """
        Асинхронно получает список заказов с определенным статусом для уведомлений из API ABCP.
        :return: Словарь с данными по заказам:
            ['count'] -> количество полученных заказов
            ['items'] -> данные по заказам
        """
        orders = {}
        try:
            logger.info(f"Получаем список заказов по статусу {status}")
            orders = await self.api_abcp.cp.admin.orders.get_orders_list(
                status_code=status, date_created_start=date_create, format='p'
            )
            logger.info(f"Получили по статусу {status} - {orders['count']} заказа(ов)")
        except Exception as e:
            logger.error(f"Ошибка при получении заказов с платформы ABCP: {e}")

        if orders.get('count', '0') == '0':
            orders['count'] = '0'
            orders['items'] = []
            logger.info(f"Заказов по указанному статусу {status} нет")
        return orders

    async def create_order_supplier(self, order_params: dict, positions: list[dict]):
        """
        Оформление заказов поставщику.
        Передаются параметры необходимые для оформления Заказа у конкретного поставщика
        Параметры, которые есть у поставщика можно получить методом из документации
        Source: https://www.abcp.ru/wiki/API.ABCP.Admin#.D0.9E.D1.82.D0.BF.D1.80.D0.B0.D0.B2.D0.BA.D0.B0_online-.D0.B7.D0.B0.D0.BA.D0.B0.D0.B7.D0.B0_.D0.BF.D0.BE.D1.81.D1.82.D0.B0.D0.B2.D1.89.D0.B8.D0.BA.D1.83
        :param order_params: словарь параметров заказа для оформления у конкретного поставщика
        :param positions: Список словарей по каждой позиции, где по ключу "id": Идентификатор позиции,
            остальные ключи это параметры для оформления заказа у конкретного поставщика
            по позиции значения которых заранее заполнены
        """

        try:
            # raise Exception({
            #     "errorCode": 200,
            #     "errorMessage": "Позиция 151056635 уже была отправлена поставщику в заказ ранее."
            # })  # Используем вызов ошибки при тестах
            result = await self.api_abcp.cp.admin.orders.send_online_order(
                order_params=order_params,
                positions=positions
            )
            return result
        except Exception as ex:
            logger.warning(ex)
            return {'error': ex}
