**DatabaseCorrector**

DatabaseCorrector – это Python-скрипт для автоматической коррекции одной базы данных на основе эталонной.

- Добавляет недостающие записи

- Обновляет измененные данные

- Безопасно работает с существующими записями

- Ведет логирование всех изменений

**Установка и запуск**
1. Клонирование репозитория
   
 *git clone https://github.com/your-username/DatabaseCorrector.git* 
 
 *cd DatabaseCorrector* 
  
2. Установка зависимостей

 *pip install sqlalchemy* 

  
3. Настройка подключения к БД
   
Откройте main.py и укажите параметры подключения:

*reference_db = "postgresql://user:password@localhost/reference_db"*

*target_db = "postgresql://user:password@localhost/target_db"*

* reference_db – эталонная (образцовая) БД
* target_db – БД, которую нужно скорректировать


4. Запуск коррекции
python database_corrector.py


**Использование**

**Синхронизация всех таблиц**

Добавьте таблицы в формате {название: ключевое поле}:

*tables_to_sync = {"users": "id", "orders": "order_id"}*

*corrector.correct_database(tables_to_sync)*

**Синхронизация одной таблицы**

*corrector.connect_to_databases()*

*corrector.correct_table("users", "id")*

*corrector.close_connections()*
