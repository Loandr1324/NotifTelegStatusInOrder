import csv
from loguru import logger


class ReadWriteCSV:
    def __init__(self, file_name):
        self.file_name = file_name

    def read_csv(self):
        """
        Считываем csv файл
        :return: list[dict]
        """
        try:
            with open(self.file_name, encoding='utf-8') as r_file:
                # Создаем объект DictReader, указываем символ-разделитель ","
                return [row for row in csv.DictReader(r_file, delimiter=",")]
        except BaseException as be:
            logger.error(be)
            return []

    def add_to_csv(self, data: list[dict], mode="a") -> bool:
        """
        Записываем данные в файл
        :param mode: режим записи в файл
        :type data: object
        :return:
        """
        if not data:
            return False
        try:
            with open(self.file_name, mode=mode, encoding='utf-8', newline='') as w_file:
                names = data[0].keys()
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
    def __init__(self, file_name):
        self.csv_rw = ReadWriteCSV(file_name)
        self._data = self.csv_rw.read_csv()  # Загружаем данные из файла
        self._new_data = []

    def filter(self, **kwargs):
        """
        Фильтруем данные из файла csv согласно заданных параметров
        """
        if kwargs['type_filter'] == 'notif':
            return list(filter(
                lambda v: v["id_status"] == kwargs['id_status'] and
                        v["id_task"] == kwargs['id_task'] and
                        v["id_order"] == kwargs['id_order'] and
                        v["id_position"] == kwargs['id_position'],
                        self._data))

        elif kwargs['type_filter'] == 'tasks':
            return list(filter(lambda v: v['task_id'] == kwargs['task_id'], self._data))

        elif kwargs['type_filter'] == 'reorder_error':
            return list(filter(lambda v: v['supplier_id'] == kwargs['supplier_id'] and
                               v['position_id'] == kwargs['position_id']
                               , self._data))

        else:
            return []

    def add_to_data(self, **kwargs):
        """Добавляем данные по позиции в список"""
        if kwargs['type_data'] == 'notif':
            self._new_data += [{
                'id_task': kwargs['id_task'],
                'id_status': kwargs['id_status'],
                'id_order': kwargs['id_order'],
                'id_position': kwargs['id_position'],
                'date_notif': kwargs['date_notif']
            }]
        elif kwargs['type_data'] == 'tasks':
            self._new_data += [{
                'task_id': kwargs['task_id'],
                'task_last_start': kwargs['task_last_start']
            }]
        elif kwargs['type_data'] == 'reorder_error':
            self._new_data += [{
                'supplier_id': kwargs['supplier_id'],
                'position_id': kwargs['position_id'],
                'count_error': kwargs['count_error'],
                'data_error': kwargs['data_error'],
                'text_error': kwargs['text_error']
            }]

    def add_data_file(self, mode="a"):
        """
        Сохраняем все накопленные данные в файл для хранения
        """
        self.csv_rw.add_to_csv(self._new_data, mode)
