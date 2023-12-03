import csv
from config import FILENAME_DATA_NOTIF
from loguru import logger
import datetime


class ReadWriteCSV:
    def __init__(self):
        self.file_name = FILENAME_DATA_NOTIF

    def read_csv(self):
        """
        Считываем csv файл
        :return: list[dict]
        """
        try:
            with open(self.file_name, encoding='utf-8') as r_file:
                # Создаем объект DictReader, указываем символ-разделитель ","
                return [notif for notif in csv.DictReader(r_file, delimiter=",")]
        except BaseException as be:
            logger.error(be)
            return []

    def add_to_csv(self, data: list[dict]) -> bool:
        """
        Записываем данные в файл
        :type data: object
        :return:
        """
        try:
            with open(self.file_name, mode="a", encoding='utf-8', newline='') as w_file:
                names = ['id_status', 'id_order', 'id_position', 'date_notif']
                file_writer = csv.DictWriter(w_file, delimiter=",", lineterminator="\r", fieldnames=names)
                if w_file.tell() == 0:
                    file_writer.writeheader()
                for row in data:
                    file_writer.writerow(row)
                return True
        except BaseException as be:
            logger.error(be)
            return False


class WorkCSV:
    def __init__(self):
        self.csv_rw = ReadWriteCSV()
        self._data = self.csv_rw.read_csv()
        self._new_data = []

    def filter(self, id_status, id_order, id_position):
        """
        Получаем данные из файла csv согласно заданных параметров
        """
        return list(filter(
            lambda v: v["id_status"] == id_status and v["id_order"] == id_order and v["id_position"] == id_position,
            # self._data)
            self._data)
        )

    def add_to_data(self, id_status: str, id_order: str, id_position: str, date_notif: datetime.datetime):
        """Добавляем данные по позиции в список"""
        self._new_data += [{
            'id_status': id_status,
            'id_order': id_order,
            'id_position': id_position,
            'date_notif': date_notif
        }]

    def add_data_file(self):
        """
        Сохраняем все накопленные данные в файл для хранения
        """
        self.csv_rw.add_to_csv(self._new_data)
