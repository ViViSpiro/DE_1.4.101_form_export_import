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
        logging.FileHandler(os.path.join(LOGS_DIR, 'f101_export.log')),
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

def log_export_start(conn):
    """Логирование начала экспорта"""
    query = """
    INSERT INTO logs.etl_logs 
    (table_name, start_time, status, records_processed) 
    VALUES (%s, %s, %s, %s)
    RETURNING log_id;
    """
    with conn.cursor() as cursor:
        try:
            cursor.execute(query, ('dm.dm_f101_round_f', datetime.now(), 'started', 0))
            log_id = cursor.fetchone()[0]
            conn.commit()
            return log_id
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка при логировании начала экспорта: {e}")
            raise

def log_export_end(conn, log_id, status, records_processed=0, error_msg=None):
    """Логирование завершения экспорта"""
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
            logger.error(f"Ошибка при логировании завершения экспорта: {e}")
            raise

def export_f101_to_csv():
    """Экспорт данных формы 101 в CSV"""
    conn = None
    try:
        conn = create_connection()
        log_id = log_export_start(conn)
        error_msg = None
        records_exported = 0
        
        # Получение данных из таблицы
        query = "SELECT * FROM dm.dm_f101_round_f"
        df = pd.read_sql(query, conn)
        
        # Сохранение в CSV
        output_file = os.path.join(DATA_DIR, 'f101_round_data.csv')
        df.to_csv(output_file, index=False, encoding='utf-8')
        records_exported = len(df)
        
        logger.info(f"Успешно экспортировано {records_exported} записей в {output_file}")
        log_export_end(conn, log_id, 'completed', records_exported)
    
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Ошибка при экспорте данных: {error_msg}")
        if conn and 'log_id' in locals():
            log_export_end(conn, log_id, 'failed', 0, error_msg)
        raise
    
    finally:
        if conn:
            conn.close()
            logger.info("Соединение с базой данных закрыто")

if __name__ == "__main__":
    export_f101_to_csv()