import telepot
from telepot.namedtuple import InlineKeyboardMarkup, InlineKeyboardButton
from config import TOKEN_BOT
from loguru import logger
import time

"""
Ссылка на документацию по html форматированию сообщения в телеграмм
https://core.telegram.org/bots/api#html-style
"""


class NotifTelegram:
    def __init__(self):
        self.TOKEN = TOKEN_BOT
        self.message = dict.fromkeys(['text', 'keyboard'])
        self.count_msg = 0

    def message_sup_order_cancel(self, num_order: str, position: dict, user_notif: dict) -> None:
        """
        Формируем сообщение для уведомления менеджеров при отмене позиций поставщиком
        param: num_order: str - номер заказа;
        return: dict {
            'text': текст сообщения: str
            'keyboard: клавиатура к сообщению: str'
            };
        """

        row1 = ''
        if user_notif['msg_type'] == 'primary':
            if user_notif['type_order'] == 'user':
                row1 = "🔴 <b>Отказ поставщика (клиент)</b>\n\n"
            elif user_notif['type_order'] == 'stock':
                row1 = "🟡 <b>Отказ поставщика (склад)</b>\n\n"
            elif user_notif['type_order'] == 'new_order':
                row1 = "🟠 <b>Заказ ждёт команды на сборку</b>\n\n"

        elif user_notif['msg_type'] == 'secondary':
            if user_notif['type_order'] == 'user':
                row1 = "🔴 <b>Отказ поставщика (клиент) (ПОВТОР)</b>\n\n"
            elif user_notif['type_order'] == 'stock':
                row1 = "🟡 <b>Отказ поставщика (склад) (ПОВТОР)</b>\n\n"
            elif user_notif['type_order'] == 'new_order':
                row1 = "🟠 <b>Заказ ждёт команды на сборку (ПОВТОР)</b>\n\n"

        elif user_notif['msg_type'] == 'error_reorder':
            row1 = "❌ <b>Ошибка отправки заказа поставщику</b>\n\n"

        url_cp_client = f'https://cpv1.pro/'
        url_order = f'{url_cp_client}?page=orders&id_order={num_order}'
        row2 = f'<b>Заказ: </b><a href="{url_order}"><u>№ {num_order}</u></a>\n'

        if user_notif['type_order'] == 'new_order':
            row3, row4, row5 = '\n', '', ''
        else:
            url_client_site = f'https://az23.ru/'
            url_search = f'{url_client_site}search/{position["brand"]}/{position["number"]}'
            row3 = f'<b>Позиция: </b><a href="{url_search}"><u>{position["brand"]} {position["number"]}</u></a>\n\n'

            row4 = f'<code>{position["description"]}</code>\n'
            row5 = f'<b>Поставщик: </b><code>{position["distributorName"]}</code>\n\n'

        if user_notif['msg_type'] == 'error_reorder':
            row6 = f'<b>Описание ошибки: </b><code>{position["error"]}</code>\n'
            row7 = f'<i>{position["userName"]}</i>'
        elif user_notif['type_order'] == 'new_order':
            row6 = f'<b>Клиент: </b>\n'
            row7 = (f'<i>{user_notif["full_name"]}</i>\n\n '
                    f'⚠️ <i>Необходимо перейти в заказ и позициям '
                    f'<b>из наличия</b> установить статус "Есть в наличии"</i>')
        else:
            row6 = f'<b>Клиент: </b>\n'
            row7 = f'<i>{user_notif["full_name"]}</i>'

        self.message['text'] = row1 + row2 + row3 + row4 + row5 + row6 + row7

        # Создаём клавиатуру для сообщения
        # self.message['keyboard'] = InlineKeyboardMarkup(inline_keyboard=[
        #     [InlineKeyboardButton(text="Перейти к заказу", url=url_order)],
        # ])

    def send_massage_chat(self, chat_id: str) -> bool:
        """Отправляем полученное сообщение в чат бот"""
        logger.info(chat_id)

        telegram_bot = telepot.Bot(self.TOKEN)
        try:
            telegram_bot.sendMessage(
                chat_id, self.message['text'],
                parse_mode="HTML",
                reply_markup=self.message['keyboard'],
                disable_web_page_preview=True)

            self.count_msg += 1
            if self.count_msg == 19:
                logger.warning(f"Отправлено {self.count_msg} сообщений в телеграм. Делаем паузу 60 сек...")
                time.sleep(60)
                self.count_msg = 0
            return True
        except Exception as e:
            logger.error('Отправка уведомления в телеграм была неудачна. Описание ошибки:')
            logger.error(e)
            return False
