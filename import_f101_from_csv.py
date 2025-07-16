import os
import logging
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv

# Настройка директорий
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
CONFIG_DIR = os.path.join(BASE_DIR, 'config')

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(CONFIG_DIR, exist_ok=True)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, 'f101_import.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv(os.path.join(CONFIG_DIR, '.env'))

# Параметры подключения к БД
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'bank_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres'),
    'port': os.getenv('DB_PORT', '5432')
}

def create_connection():
    """Создание подключения к PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        logger.info("Успешное подключение к базе данных")
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к базе данных: {e}")
        raise

def log_import_start(conn):
    """Логирование начала импорта"""
    query = """
    INSERT INTO logs.etl_logs 
    (table_name, start_time, status, records_processed) 
    VALUES (%s, %s, %s, %s)
    RETURNING log_id;
    """
    with conn.cursor() as cursor:
        try:
            cursor.execute(query, ('dm.dm_f101_round_f_v2', datetime.now(), 'started', 0))
            log_id = cursor.fetchone()[0]
            conn.commit()
            return log_id
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка при логировании начала импорта: {e}")
            raise

def log_import_end(conn, log_id, status, records_processed=0, error_msg=None):
    """Логирование завершения импорта"""
    query = """
    UPDATE logs.etl_logs 
    SET end_time = %s, status = %s, records_processed = %s, error_message = %s
    WHERE log_id = %s;
    """
    with conn.cursor() as cursor:
        try:
            cursor.execute(query, (datetime.now(), status, records_processed, error_msg, log_id))
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка при логировании завершения импорта: {e}")
            raise

def create_table_copy(conn):
    """Создание копии таблицы формы 101"""
    query = """
    CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f_v2 
    (LIKE dm.dm_f101_round_f INCLUDING DEFAULTS INCLUDING CONSTRAINTS)
    """
    with conn.cursor() as cursor:
        try:
            cursor.execute(query)
            conn.commit()
            logger.info("Таблица dm.dm_f101_round_f_v2 успешно создана")
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка при создании таблицы: {e}")
            raise

def import_f101_from_csv():
    """Импорт данных формы 101 из CSV"""
    conn = None
    try:
        conn = create_connection()
        
        # Создание копии таблицы
        create_table_copy(conn)
        
        log_id = log_import_start(conn)
        error_msg = None
        records_imported = 0
        
        # Чтение CSV файла
        input_file = os.path.join(DATA_DIR, 'f101_round_data_modified.csv')
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Файл {input_file} не найден")
        
        df = pd.read_csv(input_file)
        
        # Очистка таблицы перед импортом
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE dm.dm_f101_round_f_v2")
            conn.commit()
        
        # Преобразование DataFrame в список кортежей
        data_tuples = [tuple(None if pd.isna(x) else x for x in row) for row in df.to_numpy()]
        
        # Получение списка колонок
        columns = df.columns.tolist()
        columns_str = ', '.join([f'"{col}"' for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Формирование SQL-запроса для вставки
        insert_query = f"""
        INSERT INTO dm.dm_f101_round_f_v2 ({columns_str})
        VALUES ({placeholders})
        """
        
        # Выполнение массовой вставки
        with conn.cursor() as cursor:
            extras.execute_batch(
                cursor,
                insert_query,
                data_tuples,
                page_size=1000
            )
        
        records_imported = len(data_tuples)
        logger.info(f"Успешно импортировано {records_imported} записей в dm.dm_f101_round_f_v2")
        log_import_end(conn, log_id, 'completed', records_imported)
    
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Ошибка при импорте данных: {error_msg}")
        if conn and 'log_id' in locals():
            log_import_end(conn, log_id, 'failed', 0, error_msg)
        raise
    
    finally:
        if conn:
            conn.close()
            logger.info("Соединение с базой данных закрыто")

if __name__ == "__main__":
    import_f101_from_csv()