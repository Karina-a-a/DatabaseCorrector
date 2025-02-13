import logging
from sqlalchemy import create_engine, MetaData, Table, select, insert, update
from sqlalchemy.exc import SQLAlchemyError

# Настройка логирования
logging.basicConfig(
    filename="../db_sync.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


class DatabaseCorrector:
    """
    Класс для коррекции второй базы данных по образцу первой.
    """

    def __init__(self, reference_db_url, target_db_url):
        """
        :param reference_db_url: строка подключения к эталонной (образцовой) БД
        :param target_db_url: строка подключения к БД, которую нужно скорректировать
        """
        self.reference_db_url = reference_db_url
        self.target_db_url = target_db_url
        self.reference_engine = None
        self.target_engine = None
        self.reference_metadata = MetaData()
        self.target_metadata = MetaData()

    def connect_to_databases(self):
        """Устанавливает соединения с базами данных."""
        try:
            self.reference_engine = create_engine(self.reference_db_url)
            self.target_engine = create_engine(self.target_db_url)
            self.reference_metadata.reflect(bind=self.reference_engine)
            self.target_metadata.reflect(bind=self.target_engine)
            logging.info("Соединение с базами данных успешно установлено.")
        except SQLAlchemyError as e:
            logging.error(f"Ошибка подключения к БД: {e}")
            raise

    def close_connections(self):
        """Закрывает соединения с базами данных."""
        try:
            if self.reference_engine:
                self.reference_engine.dispose()
            if self.target_engine:
                self.target_engine.dispose()
            logging.info("Соединения с базами данных закрыты.")
        except SQLAlchemyError as e:
            logging.error(f"Ошибка при закрытии соединений: {e}")

    def correct_table(self, table_name, key_column):
        """
        Корректирует таблицу во второй БД на основе эталонной БД.
        :param table_name: имя таблицы
        :param key_column: ключевое поле (идентификатор)
        """
        try:
            reference_table = Table(table_name, self.reference_metadata, autoload_with=self.reference_engine)
            target_table = Table(table_name, self.target_metadata, autoload_with=self.target_engine)

            with self.reference_engine.connect() as ref_conn, self.target_engine.connect() as target_conn:
                transaction = target_conn.begin()  # Открываем транзакцию

                try:
                    ref_data = ref_conn.execute(select(reference_table)).fetchall()
                    target_data = target_conn.execute(select(target_table)).fetchall()

                    ref_dict = {row[key_column]: row._asdict() for row in ref_data}
                    target_dict = {row[key_column]: row._asdict() for row in target_data}

                    for key, ref_row in ref_dict.items():
                        if key not in target_dict:
                            # Добавляем запись, если её нет во второй БД
                            insert_stmt = insert(target_table).values(ref_row)
                            target_conn.execute(insert_stmt)
                            logging.info(f"Добавлена новая запись в {table_name}: {ref_row}")

                        elif ref_row != target_dict[key]:
                            # Обновляем запись, если она отличается
                            update_stmt = update(target_table).where(target_table.c[key_column] == key).values(ref_row)
                            target_conn.execute(update_stmt)
                            logging.info(f"Обновлена запись в {table_name}, ключ {key}: {ref_row}")

                    transaction.commit()  # Фиксируем изменения
                    logging.info(f"Коррекция таблицы {table_name} завершена.")
                except SQLAlchemyError as e:
                    transaction.rollback()
                    logging.error(f"Ошибка при коррекции таблицы {table_name}: {e}")
                    raise

        except SQLAlchemyError as e:
            logging.error(f"Ошибка при коррекции таблицы {table_name}: {e}")

    def correct_database(self, tables):
        """
        Корректирует переданные таблицы во второй БД по эталонной.
        :param tables: словарь {таблица: ключевое поле}
        """
        try:
            self.connect_to_databases()

            for table, key_column in tables.items():
                self.correct_table(table, key_column)

            logging.info("Коррекция базы данных завершена.")
        except Exception as e:
            logging.error(f"Ошибка при коррекции базы данных: {e}")
        finally:
            self.close_connections()


if __name__ == "__main__":
    # Пример использования
    reference_db = ""   
    target_db = "" 
    
    tables_to_correct = {
        "users": "id",
        "orders": "order_id"
    }

    corrector = DatabaseCorrector(reference_db, target_db)
    corrector.correct_database(tables_to_correct)
