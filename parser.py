import csv
import datetime
import json
import logging
import os
import random
import re
import time
import traceback
from typing import List, Optional

import requests
from bs4 import BeautifulSoup
from requests import RequestException, Timeout, TooManyRedirects

error_logger = logging.getLogger('error_logger')
error_logger.setLevel(logging.ERROR)
event_logger = logging.getLogger('event_logger')
event_logger.setLevel(logging.DEBUG)

CONFIG_PATH = 'config.json'


class ZootovaryParser:
    main_fieldnames = [
        'price_datetime', 'price', 'price_promo', 'sku_status', 'sku_barcode', 'sku_article', 'sku_name',
        'sku_category', 'sku_country', 'sku_weight_min', 'sku_volume_min', 'sku_quantity_min', 'sku_link',
        'sku_images'
    ]
    cats_fieldnames = ['name', 'id', 'parent_id']
    domain = 'https://zootovary.ru'
    log_error_file_name = 'error.log'
    log_event_file_name = 'event.log'

    def __init__(self):
        self.results: List[dict] = list()
        self.config = self.get_config()

    def _prepare_to_work(self):
        """
        Создание папок для дальнейшей работы и частичная проверка данных файла конфигурации
        Можно было еще добавить валидацию файла конфигурации
        """
        logs_dir = self.config.get('logs_dir')
        if not logs_dir:
            logs_dir = 'log'
            self.config['logs_dir'] = logs_dir
        if not os.path.exists(logs_dir):
            os.mkdir(logs_dir)

        output_directory = self.config.get('output_directory')
        if not output_directory:
            output_directory = 'out'
            self.config['output_directory'] = output_directory
        if not os.path.exists(output_directory):
            os.mkdir(output_directory)

        self._setup_loggers()

        max_retries = self.config.get('max_retries')
        if not max_retries:
            max_retries = 1
            self.config['max_retries'] = max_retries
            event_logger.info(f'set max_retries = {max_retries}')

        restart = self.config.get('restart')
        if not restart:
            restart = {
                "restart_count": 3,
                "interval_m": 0.2
            }
            self.config['restart'] = restart
            event_logger.info(f'set restart = {restart}')

    def _log_error(self, message: str):
        """
        Функция логирования ошибок (как пример, в коде не используется)

        :param message: дополнительное сообщение для лога
        """
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if message:
            text = f'{now}\n' \
                   f'{message}\n' \
                   f'{traceback.format_exc()}\n\n'
        else:
            text = f'{now}\n' \
                   f'{traceback.format_exc()}\n\n'
        log_path = os.path.join(self.config.get('logs_dir'), self.log_error_file_name)
        with open(log_path, 'a') as log_error_file:
            log_error_file.write(text)

    def _setup_loggers(self):
        """
        Установка логгеров для вывода в консоль и записи в файл
        """
        # С logging много не работал, поэтому, я думаю, получилось не самое элегантное решение
        # Обычно пишу свою функцию для записи в файл лога об ошибках файл, например функция _log_error,
        # которую оставил, как пример

        strfmt = '[%(asctime)s] [%(levelname)s] %(message)s'
        datefmt = '%Y-%m-%d %H:%M:%S'
        formatter = logging.Formatter(fmt=strfmt, datefmt=datefmt)

        stdout_handler = logging.StreamHandler()
        # stdout_handler.setLevel(logging.INFO)
        stdout_handler.setFormatter(formatter)

        error_log_path = os.path.join(self.config.get('logs_dir'), self.log_error_file_name)
        error_file_handler = logging.FileHandler(error_log_path)
        # error_file_handler.setLevel(logging.ERROR)
        error_file_handler.setFormatter(formatter)

        error_logger.addHandler(error_file_handler)
        error_logger.addHandler(stdout_handler)

        event_log_path = os.path.join(self.config.get('logs_dir'), self.log_event_file_name)
        event_file_handler = logging.FileHandler(event_log_path)
        # event_file_handler.setLevel(logging.INFO)
        event_file_handler.setFormatter(formatter)
        event_logger.addHandler(event_file_handler)

        event_logger.addHandler(stdout_handler)

    @staticmethod
    def get_config() -> dict:
        with open(CONFIG_PATH, 'r', encoding='utf-8') as config_file:
            config = json.load(config_file)
        return config

    def write_csv(self, data: List[dict], fieldnames: List[str], file_name: str = None):
        """
        Запись данных в файл csv
        :param data: данные для записи
        :param fieldnames: названия столбцов
        :param file_name: имя файла сохранения
        """
        if not fieldnames:
            fieldnames = self.main_fieldnames
        if not file_name:
            file_name = f'results_{time.time_ns()}.csv'
        output_directory = self.config['output_directory']
        save_path = os.path.join(output_directory, file_name)
        with open(save_path, 'w', encoding='utf-8', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames, delimiter=';')
            writer.writeheader()
            writer.writerows(data)

    def _get_source(self, url, params: dict = None, path: str = None) -> Optional[str]:
        """
        Функция получения html кода страницы

        :param url: ссылка для запроса
        :param params: параметры для запроса
        :param path: путь для чтения файла с текстом ответа (используется для разработки)
        :return: тест ответа в случаи успеха и None в другом случаи
        """
        event_logger.debug(f'GET URL: {url}')
        if path:
            with open(path, 'r', encoding='utf-8') as f:
                source = f.read()
            return source
        max_retries = self.config.get('max_retries')
        headers = self.config.get('headers')
        while max_retries > 0:
            try:
                response = requests.get(url, headers=headers, params=params)
                event_logger.debug(f'RESPONSE URL: {response.url}')
                if response.ok and response.status_code == 200:
                    return response.text
                max_retries -= 1
            except (RequestException, Timeout, TooManyRedirects, RequestException) as ex:
                max_retries -= 1
                error_logger.error(ex, exc_info=True)

    def _make_delay(self):
        """
        Функция для искусственной задержки

        """
        delay_range_s = self.config.get('delay_range_s')
        if delay_range_s == 0:
            return
        # стандартная задержка
        if delay_range_s is None:
            min_delay = 1
            max_delay = 3
        else:
            min_delay, max_delay = delay_range_s
        delay = random.uniform(min_delay, max_delay)
        event_logger.debug(f'MAKE DELAY: {delay}')
        time.sleep(delay)

    def _start_parser(self):
        """
        Функция запуска парсинга
        """
        categories = self.config.get('categories')
        # если нет категорий, то парсинг по всем категориям
        if not categories:
            # данные по категориям
            categories = self.parse_cats()
            for category in categories:
                # парсинг идет только по подкатегориям, так как в основной категории могут быть не все товары
                # и если парсить и главную и подкатегорию, то будет лишняя работа, так как товары будут повторяться
                # так же можно парсить только по основным категориям, если там точно все товары, что будет даже проще,
                # так как может быть, что есть основная категория, у которой нет подкатегорий
                if category.get('parent_id'):
                    cat_url = self.domain + category.get('link')
                    event_logger.debug(f'START CATEGORY: {cat_url}')
                    self.get_items(cat_url)
        # иначе парсинг по каждой категории из файла конфигурации
        else:
            for category in categories:
                # преобразования id категории в url
                url = f'{self.domain}/catalog/{category}/'
                event_logger.debug(f'START CATEGORY: {url}')
                self.get_items(url)
        # запись полученных данных в файл
        self.write_csv(self.results, self.main_fieldnames)

    def _need_to_append_results(self, new_result: dict) -> bool:
        """
        Проверка нужно ли добавлять товар в общий список

        :param new_result: полученные данные товара
        :return: True если надо добавлять, False если не надо
        """
        # пробегается по всем собранным в процессе парсинга товарам и проверяет  на совпадение sku_article и sku_barcode
        for result in self.results:
            if result.get('sku_article') == new_result.get('sku_article') and new_result.get('sku_article') and \
                    result.get('sku_barcode') == new_result.get('sku_barcode') and new_result.get('sku_barcode'):
                return False
        return True

    def parse_cats(self, write_csv=False) -> Optional[List[dict]]:
        """
        Парисинг id категорий с главной страницы сайта

        :param write_csv: если True, то данные сохраняются в файл 'categories.csv'
        :return: список словарей с данными о категориях
        """
        cats_data = []
        source = self._get_source(self.domain, path='start_page.html')
        if not source:
            event_logger.warning(f'NO SOURCE IN: {self.domain}')
            return
        soup = BeautifulSoup(source, 'lxml')
        main_cats = soup.find('div', {'id': 'catalog-menu'}).find_all('li', {'class': 'lev1'})
        for main_cat in main_cats:
            main_cat_data = main_cat.find('a', {'class': 'catalog-menu-icon'})
            main_cat_name = main_cat_data.find('span').getText(strip=True)
            main_cat_link = main_cat_data.get('href')
            main_cat_id = main_cat_link.split('/catalog')[-1][1:-1]
            cats_data.append(
                dict(
                    name=main_cat_name,
                    id=main_cat_id,
                    parent_id=None,
                    link=main_cat_link
                )
            )
            sub_cats = main_cat.find('ul', {'class': 'catalog-cols'}).find_all('a')
            for sub_cat in sub_cats:
                sub_cat_name = sub_cat.getText(strip=True)
                sub_cat_link = sub_cat.get('href')
                sub_cat_id = sub_cat_link.split('/catalog')[-1][1:-1]
                cats_data.append(
                    dict(
                        name=sub_cat_name,
                        id=sub_cat_id,
                        parent_id=main_cat_id,
                        link=sub_cat_link
                    )
                )
        if write_csv:
            self.write_csv(cats_data, self.cats_fieldnames, file_name='categories.csv')
        return cats_data

    def _extract_items(self, soup: BeautifulSoup):
        """
        Функция сбора данных о товарах

        :param soup: объект soup
        """
        # поиск товаров
        items = soup.find_all('div', {'class': 'catalog-item-top'})
        for item in items:
            link = item.find('a', {'class': 'name'}).get('href')
            # ссылка на товар
            item_url = self.domain + link
            # получение данных о товаре
            self.get_item_data(item_url)
            # задержка
            self._make_delay()

    def get_items(self, url: str):
        """
        Общая функция сбора товаров из категории
        (можно было реализовать немного иначе, но решил так)

        :param url: ссылка на категорию
        """
        source = self._get_source(url)
        if not source:
            event_logger.warning(f'NO SOURCE IN: {url}')
            return
        # self.write_file(source, 'items.html')
        soup = BeautifulSoup(source, 'lxml')
        # получаем данные о товарах на первой странице
        self._extract_items(soup)
        # ищем последнюю страницу в навигации по страницам
        try:
            last_page_link = soup.find('div', {'class': 'navigation'}).find_all('a')[-1].get('href')
        except AttributeError as e:
            return
        try:
            last_page_number = int(last_page_link.split('PAGEN_1=')[-1])
        except (ValueError, AttributeError) as e:
            return
        # итерация по страницам с товарами
        for page_number in range(2, last_page_number + 1):
            params = {
                'PAGEN_1': page_number
            }
            event_logger.debug(f'GET NEXT PAGE: {page_number} OF {last_page_number} IN CATEGORY {url}')
            source = self._get_source(url, params=params)
            if not source:
                event_logger.warning(f'NO SOURCE IN: {url}')
                continue
            soup = BeautifulSoup(source, 'lxml')
            # получаем данные о товарах на странице с номером page_number
            self._extract_items(soup)

    def get_item_data(self, item_url: str):
        """
        Получение данных о товаре

        :param item_url: ссылка на товар
        """
        # print(item_url)
        source = self._get_source(item_url)
        if not source:
            event_logger.warning(f'NO SOURCE IN: {item_url}')
            return
        soup = BeautifulSoup(source, 'lxml')
        price_datetime = datetime.datetime.now()
        try:
            sku_name = soup.find('h1').getText(strip=True)
        except AttributeError:
            sku_name = ''

        # sku_quantity_min (штук в пачке) решил искать в названии с помощью регулярок
        quantity_pattern = '\d{1,5}(\ )?(штук|шт)'
        search = re.search(quantity_pattern, sku_name.lower())
        if search:
            try:
                main_sku_quantity_min = search.group().replace('штук', '').replace('шт', '').strip()
            except AttributeError:
                main_sku_quantity_min = None
        else:
            main_sku_quantity_min = None
        main_div = soup.find('div', {'class': 'catalog-element'})
        if not main_div:
            event_logger.warning(f'no main_div on {item_url}')
            return
        try:
            sku_country = main_div.find(
                'div', {'class': 'catalog-element-offer-left'}
            ).find('p').getText(strip=True).split(':')[-1].strip()
        except AttributeError:
            sku_country = None
        try:
            sku_categories = soup.find('ul', {'class': 'breadcrumb-navigation'}).find_all('li')
            categories = []
            for category in sku_categories:
                if category.find('span'):
                    continue
                categories.append(category.getText(strip=True))
            sku_category = '|'.join(categories)
        except AttributeError:
            sku_category = None

        sku_link = item_url
        try:
            images = soup.find('div', {'class': 'catalog-element-pictures'}).find_all('a')
            sku_images = ','.join([self.domain + image.get('href') for image in images])
        except (AttributeError, TypeError):
            sku_images = None

        # вариации товара (разный вес)
        try:
            offers = main_div.find('table', {'class': 'tg22 b-catalog-element-offers-table'}).find_all(
                'tr', {'class': 'b-catalog-element-offer'}
            )
        except AttributeError:
            offers = []
        for offer in offers:
            # вариация представлена как строка в таблице с колонками, в которых лежат необходимые данные
            # шаблон товаров везде одинаковый и всегда есть определенное число колонок
            columns = offer.find_all('td')
            if not columns:
                continue
            try:
                sku_article = columns[0].getText(strip=True).split(':')[-1]
            except (IndexError, AttributeError):
                sku_article = None
            try:
                # не самый хороший вариант искать элемент по стилям, но, считаю, здесь он более оптимальный
                sku_barcode = columns[1].find('b', {'style': 'color:#c60505;'}).getText(strip=True)
            except (IndexError, AttributeError):
                sku_barcode = None
            try:
                # вес или объем фасовки товара
                packing: str = columns[2].getText(strip=True).split(':')[-1]
                # число в упаковке так же можно получить из фасовки
                if packing.__contains__('х'):
                    offer_sku_quantity_min = packing.split('х')[0]
                    sku_quantity_min = offer_sku_quantity_min
                else:
                    sku_quantity_min = main_sku_quantity_min
                # если в столбце с данными о фасовке есть "г" - граммы (кг или г), то это вес
                if packing.__contains__('г'):
                    sku_weight_min = packing.split('х')[-1]
                    sku_volume_min = None
                # если в столбце с данными о фасовке есть "л" - литры (мл или л), то это объем
                elif packing.__contains__('л'):
                    sku_volume_min = packing.split('х')[-1]
                    sku_weight_min = None
                else:
                    sku_volume_min = None
                    sku_weight_min = None
            except (IndexError, AttributeError):
                sku_weight_min = None
                sku_volume_min = None
                sku_quantity_min = main_sku_quantity_min

            try:
                # сначала получаем элемент, в котором лежат цены
                price_element = columns[4].find('span')
                if not price_element:
                    price = None
                    price_promo = None
                else:
                    # цену так же пришлось искать использую стили элементов
                    if price_element.get('style').__contains__('color:#c60505'):
                        price_promo = price_element.getText(strip=True)
                        price = columns[4].find('s', {'style': 'color:#000000;'}).getText(strip=True)
                    else:
                        price = price_element.getText(strip=True)
                        price_promo = None
            except (IndexError, AttributeError):
                price = None
                price_promo = None

            # наличие товара проверяется по наличию кнопки покупки,
            # так же можно проверять по наличию кнопки уведомления о поступлении
            buy_button = columns[5].find('div', {'class': 'buybuttonarea'})
            notify_button = columns[5].find('div', {'class': 'notavailbuybuttonarea'})
            # товар в наличии, если есть кнопка покупки
            sku_status = '1' if buy_button else '0'

            res = dict(
                price_datetime=price_datetime,
                sku_name=sku_name,
                sku_country=sku_country,
                sku_link=sku_link,
                sku_quantity_min=sku_quantity_min,
                sku_images=sku_images,
                sku_article=sku_article,
                sku_barcode=sku_barcode,
                sku_weight_min=sku_weight_min,
                sku_volume_min=sku_volume_min,
                price=price,
                sku_category=sku_category,
                price_promo=price_promo,
                sku_status=sku_status
            )
            # проверка надо ли добавлять этот товар
            # т.е. нет ли уже товара с таким же артикулом и шрихкодом
            if self._need_to_append_results(res):
                self.results.append(res)

    def start_parser(self):
        """
        Главная функция запуска парсинга
        """
        self._prepare_to_work()
        restart = self.config.get('restart')
        restart_count = restart.get('restart_count')
        interval_m = restart.get('interval_m')
        interval_s = interval_m * 60
        event_logger.info('START PARSING')
        for try_count in range(1, restart_count + 1):
            try:
                self._start_parser()
                break
            except Exception as e:
                error_logger.error(f'MAIN ERROR {e}', exc_info=True)
                if try_count == restart_count:
                    event_logger.warning(f'BAD TRY {try_count}')
                    break
                event_logger.warning(f'BAD TRY {try_count}, SLEEP {interval_m} m')
                time.sleep(interval_s)
        event_logger.info('END PARSING')


def main():
    zootovary_parser = ZootovaryParser()
    zootovary_parser.start_parser()


if __name__ == '__main__':
    main()
