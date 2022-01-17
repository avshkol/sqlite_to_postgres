from __future__ import annotations

import pandas as pd
import sqlite3
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2 import Error
from dataclasses import dataclass


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


def load_from_sqlite() -> None:
    """Загрузка данных из SQLite

    Загружаем все нужные таблицы из db.sqlite
    Таблицы остаются в глобальных переменных

    В таблицах film_work удаляются дубли,
    а также записи со ссылками из удаленных дублей в genre_film_work и person_film_work
    """

    con = sqlite3.connect('db.sqlite')

    tables = select('''select * from sqlite_master where type = 'table' ''', con)
    print_isinstance('tables', tables)

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

    dsn = {
        'dbname': 'movies_database',
        'user': 'postgres',
        'password': 'avshavsh',
        'host': 'localhost',  # '0.0.0.0',#'127.0.0.1' - это альтернативная, без Докера
        'port': 5433,  # Используется, поскольку 5432 занят
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
            @dataclass(frozen=True)
            class genres:
                __slots__ = ('id', 'name', 'description', 'created_at', 'updated_at')
                id: str
                name: str
                description: str
                created_at: str
                updated_at: str

            data = []
            for index, item in genre.iterrows():
                genres_item = genres(str(item['id']), str(item['name']), str(item['description']),
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
            @dataclass(frozen=True)
            class film_works:
                __slots__ = ('id', 'title', 'description', 'creation_date', 'certificate',
                             'file_path', 'rating', 'type', 'created_at', 'updated_at')
                id: str
                title: str
                description: str
                creation_date: str
                certificate: str
                file_path: str
                rating: float
                type: str
                created_at: str
                updated_at: str

            data = []
            for index, item in film_work.iterrows():
                film_works_item = film_works(str(item['id']), str(item['title']), str(item['description']),
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
            @dataclass(frozen=True)
            class persons:
                __slots__ = ('id', 'full_name', 'birth_date', 'created_at', 'updated_at')
                id: str
                full_name: str
                birth_date: str
                created_at: str
                updated_at: str

            data = []
            for index, item in person.iterrows():
                persons_item = persons(str(item['id']), str(item['full_name']), str(item['birth_date']),
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
            @dataclass(frozen=True)
            class genre_film_works:
                __slots__ = ('id', 'film_work_id', 'genre_id', 'created_at')
                id: str
                film_work_id: str
                genre_id: str
                created_at: str

            data = []
            for index, item in genre_film_work.iterrows():
                genre_film_works_item = genre_film_works(str(item['id']), str(item['film_work_id']),
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
            @dataclass(frozen=True)
            class person_film_works:
                __slots__ = ('id', 'film_work_id', 'person_id', 'role', 'created_at')
                id: str
                film_work_id: str
                person_id: str
                role: str
                created_at: str

            data = []
            for index, item in person_film_work.iterrows():
                person_film_works_item = person_film_works(str(item['id']), str(item['film_work_id']),
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
    load_from_sqlite()
    id_test()
    save_to_postgres()
