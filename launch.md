## Необходимо скачать
*   Скачанные JDBC-драйверы в папке `./spark-jars/`:
    *   `postgresql-42.6.0.jar`
    *   `clickhouse-jdbc-0.4.6.jar`

1.  **Запустить сервисы (PostgreSQL, Spark, ClickHouse):**
    Из корневой директории проекта:
    ```bash
    docker-compose up --build -d
    ```

2. **Инициализация clickhouse базы данных:**
   ```bash
   docker exec -it clickhouse clickhouse-client < /app/init-db-clickhouse/init-db-clickhouse.sql 
   ```

3.  **Преобразование в снежинку:**
    ```bash
    docker exec -it spark bash
    spark-submit --jars /app/jars/postgresql-42.7.5.jar /app/spark-apps/etl_postgres_star_schema.py
    ```

4.  **Загрузка данных в Clickhouse и работа отчетов**
    ```bash
    spark-submit \
      --jars /app/jars/postgresql-42.6.0.jar,/app/spark-jars/clickhouse-jdbc-0.4.6.jar \
      /app/spark-apps/etl_clickhouse_reports.py
    ```

---
