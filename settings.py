import logging

# Настройки логгирования
LOG_FILE_NAME = "process.log"
FILE_MODE = "w"
LOG_LEVEL = logging.INFO

# Размер порции передачи данных
BATCH_SIZE = 500

SQLITE_DB_NAME = 'db.sqlite'

TABLES = ['genre', 'film_work', 'person', 'genre_film_work', 'person_film_work']
