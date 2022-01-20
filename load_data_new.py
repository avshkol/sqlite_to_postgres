from __future__ import annotations
import os
import random
from dotenv import load_dotenv
from pathlib import Path
import sqlite3

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2 import Error

from settings import *
from db_classes import *


def cheking(table_name:str, table_batch:pd.DataFrame) -> pd.DataFrame:
    '''Обработка батчей на предмет пропусков и совпадений'''

    if table_name == 'genre':
        table_batch.fillna('', inplace=True)
    elif table_name == 'person':
        table_batch['birth_date'].fillna('1900-01-01', inplace=True)
    elif table_name == 'film_work':
        # У нас есть дубли по названию и не определены даты
        # Название+дата созадния имеет уникальный индекс
        # Введем условные случайные даты с 1010 до 1099 года -
        # в любом случае, в это время фильмов не было и даты потребуют замены
        random.seed(12345)
        # table_batch_new = table_batch
        for i, row in table_batch.iterrows():
            if row['creation_date'] == None:
                table_batch.loc[i,'creation_date'] = f'10{random.randint(10,99)}-01-01'

        table_batch['rating'].fillna(0.0, inplace=True)
        table_batch['certificate'].fillna('', inplace=True)
        table_batch['file_path'].fillna('', inplace=True)
        table_batch['type'].fillna('', inplace=True)
        table_batch['created_at'].fillna('2021-06-16 20:14:09.702729+00', inplace=True)
        table_batch['updated_at'].fillna('2021-06-16 20:14:09.702729+00', inplace=True)
    elif table_name == 'genre_film_work':
        pass
    elif table_name == 'person_film_work':
        pass
    else:
        pass
    return table_batch


def save_to_postgres(table_name:str, postgres_cursor, table_batch:pd.DataFrame):
    '''Сохранение в postgres батча'''
    if table_name == 'genre':  # Переносим в базу таблицу genre
        data = []
        for index, item in table_batch.iterrows():
            genres_item = Genre(str(item['id']), str(item['name']), str(item['description']),
                             str(item['created_at']), str(item['updated_at']))
            data.append(genres_item)
        execute_batch(
            postgres_cursor,
            "INSERT INTO genre (id, name, description, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)",
            [
                (item.id, item.name, item.description,
                 item.created_at, item.updated_at)
                for item in data
            ],
            page_size=int(BATCH_SIZE/5),
            )

    elif table_name == 'film_work':  # Переносим в базу таблицу film_work
        data = []
        for index, item in table_batch.iterrows():
            film_works_item = FilmWork(str(item['id']), str(item['title']), str(item['description']),
                                        str(item['creation_date']), str(item['certificate']),
                                        str(item['file_path']),
                                        item['rating'], str(item['type']),
                                        str(item['created_at']), str(item['updated_at']))
            data.append(film_works_item)
        execute_batch(
            postgres_cursor,
            '''INSERT INTO film_work (id, title, description, creation_date, certificate, 
                        file_path, rating, type, created_at, updated_at) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
            [
                (item.id, item.title, item.description,
                 item.creation_date, item.certificate, item.file_path,
                 item.rating, item.type,
                 item.created_at, item.updated_at)
                for item in data
            ],
            page_size=int(BATCH_SIZE/5),
            )

    elif table_name == 'person':  # Переносим в базу таблицу person
        data = []
        for index, item in table_batch.iterrows():
            persons_item = Person(str(item['id']), str(item['full_name']), str(item['birth_date']),
                                   str(item['created_at']), str(item['updated_at']))
            data.append(persons_item)
        execute_batch(
            postgres_cursor,
            "INSERT INTO person (id, full_name, birth_date, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)",
            [
                (item.id, item.full_name, item.birth_date,
                 item.created_at, item.updated_at)
                for item in data
            ],
            page_size=int(BATCH_SIZE/5),
            )

    elif table_name == 'genre_film_work':  # Переносим в базу таблицу genre_film_work
        data = []
        for index, item in table_batch.iterrows():
            genre_film_works_item = GenreFilmWork(str(item['id']), str(item['film_work_id']),
                                                   str(item['genre_id']), str(item['created_at']))
            data.append(genre_film_works_item)
        execute_batch(
            postgres_cursor,
            "INSERT INTO genre_film_work (id, film_work_id, genre_id, created_at) VALUES (%s, %s, %s, %s)",
            [
                (item.id, item.film_work_id, item.genre_id, item.created_at)
                for item in data
            ],
            page_size=int(BATCH_SIZE/5),
            )

    elif table_name == 'person_film_work':  # Переносим в базу таблицу person_film_work
        data = []
        for index, item in table_batch.iterrows():
            person_film_works_item = PersonFilmWork(str(item['id']), str(item['film_work_id']),
                                                     str(item['person_id']), str(item['role']),
                                                     str(item['created_at']))
            data.append(person_film_works_item)
        execute_batch(
            postgres_cursor,
            '''INSERT INTO person_film_work (id, film_work_id, person_id, role, created_at) 
            VALUES (%s, %s, %s, %s, %s)''',
            [
                (item.id, item.film_work_id, item.person_id, item.role, item.created_at)
                for item in data
            ],
            page_size=int(BATCH_SIZE/5),
            )
    else:
        pass



def load_table_from_sqlite(table_name: str, con: sqlite3.dbapi2.Connection,
                           cursor, BATCH_SIZE: int):
    """Загрузка списка таблиц из SQLite батчами
    и вызов функции выгрузки в postgres
    """

    cursor.execute(f'SELECT count(*) FROM {table_name};')
    table_count = cursor.fetchone()[0]
    print(table_count)

    position = 0
    while position < table_count:
        # - Загрузим батч из sqlite
        table_select = f'SELECT * FROM {table_name} LIMIT {position}, {BATCH_SIZE};'
        cursor.execute(table_select)
        table_batch = pd.read_sql(table_select, con)
        table_batch = cheking(table_name, table_batch)  # Обработка загруженной порции
        logging.info(f'Загружена из sqlite таблица {table_name} с позиции {position}')

        # - Выгрузим батч в postgres
        save_to_postgres(table_name, postgres_cursor, table_batch)
        logging.info(f'Сохранена в postgres таблица {table_name} с позиции {position}')
        position += BATCH_SIZE



if __name__ == '__main__':
    logging.basicConfig(filename=LOG_FILE_NAME, filemode=FILE_MODE, level=LOG_LEVEL)
    logging.info(f'Запуск перевода данных из sqlite в postgres. Размер батча {BATCH_SIZE}')

    # Подключение к sqlite
    try:
        con = sqlite3.connect(SQLITE_DB_NAME)
        cursor = con.cursor()
        logging.info(f'Открыта sqlite {SQLITE_DB_NAME}')
    except sqlite3.Error as error:
        logging.info(f"Ошибка {error} при открытии sqlite {SQLITE_DB_NAME}")

    # Подключение к postgres
    load_dotenv()
    env_path = Path(__file__).parent/'env.env'
    load_dotenv(dotenv_path=env_path)

    dsn = {
        'dbname': os.environ.get('DB_NAME'),  # 'movies_database',
        'user': os.environ.get('DB_USER'),  # 'postgres',
        'password': os.environ.get('DB_PASSWORD'),  # 'avshavsh',
        'host': os.environ.get('DB_HOST', '127.0.0.1'),  # '0.0.0.0',#'127.0.0.1' - это альтернативная, без Докера
        'port': os.environ.get('DB_PORT', 5433),  # Используется, поскольку 5432 занят
        'options': '-c search_path=content'
    }

    try:
        postgres_conn = psycopg2.connect(**dsn)
        postgres_cursor = postgres_conn.cursor()
        # Анализ БД
        postgres_cursor.execute('''select * from pg_tables where schemaname='content';''')
        result = postgres_cursor.fetchall()
        logging.info('Анализ успешности подключения к БД:')
        for string in result:
            logging.info(f'Таблица {string}')
        postgres_cursor.close()
        postgres_conn.close()
    except (Exception, Error) as error:
        logging.info(f'Ошибка подключения к postgres {error}')

    # Очистим таблицы от старых данных
    postgres_conn = psycopg2.connect(**dsn)
    postgres_cursor = postgres_conn.cursor()
    postgres_cursor.execute("""TRUNCATE person CASCADE;
                        TRUNCATE film_work CASCADE;
                        TRUNCATE genre_film_work CASCADE;
                        TRUNCATE genre CASCADE;
                        TRUNCATE person_film_work CASCADE;
                        """)

    # Пройдем по всем таблицам БД батчами размером BATCH_SIZE:
    # - Загрузим батч из sqlite
    # - Выгрузим батч в postgres
    for table_name in TABLES:
        load_table_from_sqlite(table_name, con, cursor, BATCH_SIZE)
        logging.info(f'Загружена таблица {table_name}')

    # Закрытие sqlite
    if con:
        con.close()
        logging.info("Соединение с SQLite закрыто")

    # Закрытие postgres
    if postgres_conn:
        postgres_cursor.close()
        postgres_conn.close()
        logging.info("Соединение с PostgreSQL закрыто")