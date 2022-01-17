from __future__ import annotations
import os
from dotenv import load_dotenv
from pathlib import Path
import sqlite3

#import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from psycopg2 import Error

from settings import *
from db_classes import *


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    # postgres_saver = PostgresSaver(pg_conn)
    # sqlite_loader = SQLiteLoader(connection)

    # data = sqlite_loader.load_movies()
    # postgres_saver.save_all_data(data)


# Синтаксис LIMIT такой:
# LIMIT <смещение>, <количество строк>
# есть два варианта: один выбирает первые <сколько-там-указано> записей,
# а другой выбирает диапазон. Итак — что если нам нужна третья “страница” данных,
# записи с 61 по 90? С использованием LIMIT, в MySQL это будет выглядеть вот так:
# SELECT * FROM table_name LIMIT 60, 30
# 'SELECT * FROM ' + table_name + ' LIMIT ' + i + ' , ' + BATCH_SIZE







def select(sql: str, con: sqlite3.dbapi2.Connection) -> pd.DataFrame:
    """Выполнить запрос на выборку из SQLite

    Принимает строку SQL-выборки и класс Connection
    Возвращает датафрейм с выборкой
    """

    return pd.read_sql(sql, con)


def print_isinstance(table_name: str, df: pd.DataFrame) -> None:
    '''Проверяем, загружена/создана ли таблица и печатаем 5 строк и инфо

    На входе имя таблицы и датафрейм
    '''

    if df is not None:
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)

        print()
        print(table_name, 'загружена/создана:')
        print(df.head())
        print()
        print(df.info())

    else:
        print(table_name, 'не загружена/не создана!')
        input('Enter for continue')


def load_from_sqlite(table_name: str) -> None:
    """Загрузка списка таблиц из SQLite

    Проверка наличия нужных таблиц
    """

    try:
        con = sqlite3.connect(SQLITE_DB_NAME)
        cursor = con.cursor()
        logging.info("Подключен к SQLite", SQLITE_DB_NAME)

        sqlite_select_query = '''select * from sqlite_master where type = 'table' '''
        cursor.execute(sqlite_select_query)
        tables = cursor.fetchall()
        logging.info("Таблицы", tables)
        cursor.close()

    except sqlite3.Error as error:
        logging.info("Ошибка при работе с SQLite", error)
    finally:
        if con:
            con.close()
            logging.info("Соединение с SQLite закрыто")


def load_table_from_sqlite(table_name: str, table_select: str, cursor, batch_size: int):
    """Загрузка таблицы из SQLite

    """
    cursor.execute(table_select)
    logging.info("Выборка полей из таблицы", table_name)

    save_to_postgres()


    global genre
    genre = select('''select * from genre''', con)
    genre.fillna('', inplace=True)
    print_isinstance('genre', genre)

    global film_work
    film_work = select('''select * from film_work''', con)
    # Принимаем нелегкое решение удалить дубликаты по названию - название+дата созадния имеет уникальный индекс
    film_work.drop_duplicates(subset=['title'], inplace=True)
    # Заполняем отсутствующие
    film_work['creation_date'].fillna('1900-01-01', inplace=True)
    film_work['rating'].fillna(0.0, inplace=True)
    film_work['certificate'].fillna('', inplace=True)
    film_work['file_path'].fillna('', inplace=True)
    film_work['type'].fillna('', inplace=True)
    film_work['created_at'].fillna('2021-06-16 20:14:09.702729+00', inplace=True)
    film_work['updated_at'].fillna('2021-06-16 20:14:09.702729+00', inplace=True)
    print_isinstance('film_work', film_work)

    global person
    person = select('''select * from person''', con)
    person['birth_date'].fillna('1900-01-01', inplace=True)
    print_isinstance('person', person)

    global genre_film_work
    genre_film_work = select('''select * from genre_film_work''', con)
    # Удаляем записи с id, не имеющие ссылок на film_work (в т.ч. удаленные дубли)
    fake_id = set(genre_film_work['film_work_id']) - set(film_work['id'])
    print('fake_id для удаления', len(fake_id))
    genre_film_work = genre_film_work[~genre_film_work['film_work_id'].isin(fake_id)]
    print_isinstance('genre_film_work', genre_film_work)

    global person_film_work
    person_film_work = select('''select * from person_film_work''', con)
    # Удаляем записи с id, не имеющие ссылок на film_work (в т.ч. удаленные дубли)
    fake_id = set(person_film_work['film_work_id']) - set(film_work['id'])
    print('fake_id для удаления', len(fake_id))
    person_film_work = person_film_work[~person_film_work['film_work_id'].isin(fake_id)]
    print_isinstance('person_film_work', person_film_work)

    con.close()


def id_test() -> None:
    '''Проверим корректность id

     Отсутствие лишних и уникальность пар в таблицах многие-ко-многим
     '''

    print('кол-во уникальных id в film_work', len(set(film_work['id'])))
    print('кол-во уникальных id film_work в genre_film_work', len(set(genre_film_work['film_work_id'])))
    print('кол-во различий id film_work в genre_film_work',
          set(film_work['id']).difference(set(genre_film_work['film_work_id'])))
    print('кол-во уникальных id в genre', len(set(genre['id'])))
    print('кол-во уникальных id genre_film_work в genre_film_work', len(set(genre_film_work['genre_id'])))
    print('кол-во пересечений id genre в genre_film_work',
          set(genre['id']).difference(set(genre_film_work['genre_id'])))
    print('кол-во уникальных id в genre_film_work', len(set(genre_film_work['id'])))
    print('кол-во уникальных film_work_id-genre_id в genre_film_work',
          len(set(genre_film_work['film_work_id'] + genre_film_work['genre_id'])))
    print('кол-во уникальных id в person', len(set(person['id'])))
    print('кол-во уникальных id film_work_id в person_film_work', len(set(person_film_work['film_work_id'])))
    print('кол-во пересечений id person в person_film_work',
          set(person['id']).difference(set(person_film_work['person_id'])))
    print('кол-во уникальных id в person_film_work', len(set(person_film_work['id'])))
    print('кол-во уникальных film_work_id-person_id-role в person_film_work',
          len(set(person_film_work['film_work_id'] + person_film_work['person_id'] + person_film_work['role'])))


def print_table_limit_5(cur: 'cursor', table_name: str) -> None:
    '''Печать выборки из 5 первых строк таблицы и кол-ва строк

    Принимает на входе курсор на БД и имя таблицы в БД
    '''

    print('Выборка из таблицы', table_name)
    cur.execute('SELECT * FROM ' + table_name + ' LIMIT 5')
    print(cur.fetchall())

    print('Кол-во строк в', table_name)
    cur.execute('SELECT COUNT(id) FROM ' + table_name)
    print(cur.fetchall())


def save_to_postgres() -> None:
    '''Сохраним данные в БД Postgres

    Таблицы и индексы уже созданы'''
    load_dotenv()
    env_path = Path(__file__)/ 'env.env'
    load_dotenv(dotenv_path=env_path)

    dsn = {
        'dbname': os.environ.get('DB_NAME'), #'movies_database',
        'user': os.environ.get('DB_USER'), #'postgres',
        'password': os.environ.get('DB_PASSWORD'), #'avshavsh',
        'host': os.environ.get('DB_HOST', '127.0.0.1'),  # '0.0.0.0',#'127.0.0.1' - это альтернативная, без Докера
        'port': os.environ.get('DB_PORT', 5433), # Используется, поскольку 5432 занят
        'options': '-c search_path=content'
    }

    try:
        with psycopg2.connect(**dsn) as conn, conn.cursor() as cursor:
            # Анализ БД
            cursor.execute('''select * from pg_tables where schemaname='content';''')
            result = cursor.fetchall()
            print('Анализ успешности подключения к БД:')
            for string in result:
                print(string)

            cursor.execute("""TRUNCATE person CASCADE;
            TRUNCATE film_work CASCADE;
            TRUNCATE genre_film_work CASCADE;
            TRUNCATE genre CASCADE;
            TRUNCATE person_film_work CASCADE;
            """)

            # Переносим в базу таблицу genre

            data = []
            for index, item in genre.iterrows():
                genres_item = Genres(str(item['id']), str(item['name']), str(item['description']),
                                     str(item['created_at']), str(item['updated_at']))

                data.append(genres_item)

            execute_batch(
                cursor,
                "INSERT INTO genre (id, name, description, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)",
                [
                    (item.id, item.name, item.description,
                     item.created_at, item.updated_at)
                    for item in data
                ],
                page_size=100,
            )
            print_table_limit_5(cursor, 'genre')

            # Переносим в базу таблицу film_work

            data = []
            for index, item in film_work.iterrows():
                film_works_item = FilmWorks(str(item['id']), str(item['title']), str(item['description']),
                                             str(item['creation_date']), str(item['certificate']),
                                             str(item['file_path']),
                                             item['rating'], str(item['type']),
                                             str(item['created_at']), str(item['updated_at']))

                data.append(film_works_item)

            execute_batch(
                cursor,
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
                page_size=100,
            )
            print_table_limit_5(cursor, 'film_work')

            # Переносим в базу таблицу person

            data = []
            for index, item in person.iterrows():
                persons_item = Persons(str(item['id']), str(item['full_name']), str(item['birth_date']),
                                       str(item['created_at']), str(item['updated_at']))

                data.append(persons_item)

            execute_batch(
                cursor,
                "INSERT INTO person (id, full_name, birth_date, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)",
                [
                    (item.id, item.full_name, item.birth_date,
                     item.created_at, item.updated_at)
                    for item in data
                ],
                page_size=100,
            )
            print_table_limit_5(cursor, 'person')

            # Переносим в базу таблицу genre_film_work

            data = []
            for index, item in genre_film_work.iterrows():
                genre_film_works_item = GenreFilmWorks(str(item['id']), str(item['film_work_id']),
                                                         str(item['genre_id']), str(item['created_at']))

                data.append(genre_film_works_item)

            execute_batch(
                cursor,
                "INSERT INTO genre_film_work (id, film_work_id, genre_id, created_at) VALUES (%s, %s, %s, %s)",
                [
                    (item.id, item.film_work_id, item.genre_id, item.created_at)
                    for item in data
                ],
                page_size=100,
            )
            print_table_limit_5(cursor, 'genre_film_work')

            # Переносим в базу таблицу person_film_work

            data = []
            for index, item in person_film_work.iterrows():
                person_film_works_item = PersonFilmWorks(str(item['id']), str(item['film_work_id']),
                                                           str(item['person_id']), str(item['role']),
                                                           str(item['created_at']))

                data.append(person_film_works_item)

            execute_batch(
                cursor,
                '''INSERT INTO person_film_work (id, film_work_id, person_id, role, created_at) 
                VALUES (%s, %s, %s, %s, %s)''',
                [
                    (item.id, item.film_work_id, item.person_id, item.role, item.created_at)
                    for item in data
                ],
                page_size=100,
            )
            print_table_limit_5(cursor, 'person_film_work')

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Соединение с PostgreSQL закрыто")


if __name__ == '__main__':
    logging.basicConfig(filename=LOG_FILE_NAME, filemode=FILE_MODE, level=LOG_LEVEL)
    logging.info('Запуск перевода данных из sqlite в postgres. Размер батча', BATCH_SIZE)

    load_from_sqlite()
    try:
        con = sqlite3.connect(SQLITE_DB_NAME)
        cursor = con.cursor()
        logging.info('Открыта sqlite', SQLITE_DB_NAME)
    except sqlite3.Error as error:
        logging.info("Ошибка", error, "при открытии sqlite", table_name)

    for table_name in TABLES:
        load_table_from_sqlite(table_name, table_select, cursor, BATCH_SIZE)

    if con:
        con.close()
        logging.info("Соединение с SQLite закрыто")

    id_test()
    save_to_postgres()
