# Задание 1 — Yandex DataTransfer

**Цель:** Перенести данные из YDB в Object Storage с помощью DataTransfer.

<details>
  <summary>Пункт 1. Создадим базу данных Managed Service for YDB</summary>

  ![image](https://github.com/user-attachments/assets/9e34b78b-1c2c-4ee0-b6ab-f5cac62a56fe)
</details>

<details>
  <summary>Пункт 2. Создадим бакет Object Storage.</summary>

  ![image](https://github.com/user-attachments/assets/bdbeb996-5857-499d-b0cf-7886db75f2e8)
</details>

<details>
  <summary>Пункт 3. Создадим сервисный аккаунт с ролями storage.editor и ydb.editor.</summary>

  ![image](https://github.com/user-attachments/assets/a579f606-5d3e-45d2-baaf-444b12ce0110)
</details>

<details>
  <summary>Пункт 4. Создадим таблицу и загрузим данные в YDB.</summary>

1. Создадим таблицу `transactions` со схемой:

   ```sql
   CREATE TABLE transactions (
     Customer_ID        Uint64,
     Name               Utf8,
     Surname            Utf8,
     Gender             Utf8,
     Birthdate          Utf8,
     Transaction_Amount Double,
     Date               Date,
     Merchant_Name      Utf8,
     Category           Utf8,
     PRIMARY KEY (Customer_ID, Date)
   );
   ```

2. Загрузим данные из `data.csv` в таблицу `transactions`:

   ```bash
   ydb --endpoint grpcs://ydb.serverless.yandexcloud.net:2135 \
       --database /ru-central1/b1gpr9g9kp75o3g7kv72/etn8bdk8duv4jk3vtp4q \
       --sa-key-file authorized_key.json \
       import file csv \
         --path transactions \
         --columns Customer_ID,Name,Surname,Gender,Birthdate,Transaction_Amount,Date,Merchant_Name,Category \
         --delimiter "," \
         --skip-rows 1 \
         --null-value "" \
         --verbose \
         data.csv
   ```

</details>


<details>
  <summary>Пункт 5. Создадим трансфер в Object Storage из YDB с Yandex DataTransfer</summary>

  **Источник:**
  ![image](https://github.com/user-attachments/assets/7329afad-64b1-45c5-81cb-a90989f8178c)

  **Приемник:**
  ![image](https://github.com/user-attachments/assets/62779a5f-a11b-46ec-8ac4-bf2e49159cc8)

  **Трансфер:**
  ![image](https://github.com/user-attachments/assets/6c932bc9-b744-4bd8-abec-27fb35b524e0)
  ![image](https://github.com/user-attachments/assets/43c856c0-cbc8-4d1a-a4ae-db312607474a)

  Данные появились в Object Storage, всё верно:
  ![image](https://github.com/user-attachments/assets/0d694099-5276-4003-bfc7-355a9b8d6c41)
</details>

**✅ Задание 1 завершено**
# Задание 2 — Автоматизация обработки CSV-данных в Yandex Data Processing через Apache Airflow

**Цель:** Поднять пайплайн, который на входе берёт CSV из Object Storage, чистит и трансформирует данные с помощью PySpark, объединяет имя и фамилию в одно поле, отбрасывает пустые строки, добавляет дату обработки и сохраняет результат в Parquet — всё это автоматически, через DAG в Managed Airflow.

<details>
  <summary>Пункт 1. Подготовка инфраструктуры</summary>

  Кластер Managed Service for Apache Airflow:
  ![image](https://github.com/user-attachments/assets/c5c9f684-ed56-4d5f-aa1c-54950cda3619)

  Кластер Metastore:
  ![image](https://github.com/user-attachments/assets/c477f99f-f755-42f7-bafc-fa281ef5a287)
</details>

<details>
  <summary>Пункт 2. PySpark-скрипт для обработки данных</summary>

Мы написали `process_csv.py`, который:

1. Читает `s3a://etlexam/data.csv`
2. Фильтрует строки, где отсутствуют `Name` или `Surname`
3. Кастит колонки (`Customer_ID`, `Transaction_Amount`) и парсит даты (`Birthdate`, `Date`)
4. Объединяет `Name` + `Surname` → `FullName`
5. Добавляет колонку `processing_date = current_date()`
6. Дропает все оставшиеся строки с `NULL`
7. Сохраняет чистый DataFrame в Parquet по пути `s3a://etlexam/transactions_clean`

Скрипт лежит в `scripts/process_csv.py` в бакете.

</details>

<details>
  <summary>Пункт 3. DAG для автоматизации в Airflow</summary>

Файл `dags/data_processing_dag.py`:

* Первый таск создаёт кластер Yandex Data Proc.
* Второй таск запускает `process_csv.py`.
* Третий таск удаляет кластер (`ALL_DONE`).

В результате каждый день (по расписанию) пайплайн автоматически:

1. Создаёт Spark-кластер
2. Обрабатывает `data.csv`
3. Пишет `transactions_clean` в бакет
4. Удаляет кластер

</details>

<details>
  <summary>Пункт 4. Результаты выполнения</summary>

После успешного запуска DAG в бакете `etlexam` появилась папка:

```
transactions_clean/
 ├── _SUCCESS
 └── part-00000-...-c000.snappy.parquet
```

Файл `part-00000-...-c000.snappy.parquet` содержит чистые данные со схемой:

| Customer\_ID | FullName        | Transaction\_Amount | Birthdate  | Date       | Merchant\_Name         | Category | processing\_date |
| ------------ | --------------- | ------------------- | ---------- | ---------- | ---------------------- | -------- | ---------------- |
| 752858       | Sean Rodriguez  | 35.47               | 2002-10-20 | 2023-04-03 | Smith-Russell          | Cosmetic | 2025-06-19       |
| 26381        | Michelle Phelps | 2552.72             | 1985-10-24 | 2023-07-17 | Peck, Spence and Young | Travel   | 2025-06-19       |
| …            | …               | …                   | …          | …          | …                      | …        | …                |

  DAG запустился успешно:

![image](https://github.com/user-attachments/assets/6da726fb-b235-45a7-8fe8-1e210eb68ed0)

  В Object Storage появились нужные файлы и логи:
  
![image](https://github.com/user-attachments/assets/c68f7274-7fb4-4f9b-80d4-1f41394569c8)

</details>

**✅ Задание 2 выполнено.**

# Задание 3 — Работа с топиками Apache Kafka® с помощью PySpark-заданий в Yandex Data Processing

**Цель:** сквозной пайплайн: Spark-джоб пачками отправляет очищенный Parquet в Kafka, второй Spark-стрим забирает сообщения и пишет их в PostgreSQL.

<details>
<summary>1. Инфраструктура</summary>

Подняты кластеры Data Proc, Kafka, Postgres. Настроен сервисный аккаунты и создан бакет.

</details>

<details>
<summary>2. PySpark-скрипты</summary>

| Файл | Действие                                                                                                                                   |
| ---- |--------------------------------------------------------------------------------------------------------------------------------------------|
| `kafka-write.py` | читает `s3a://etlexam/transactions_clean`, каждые **100** строк пока не кончится ⇒ JSON ⇒ Kafka                                            |
| `kafka-read-stream.py` | создаёт `transactions_stream` (JDBC DDL) и стримом пишет данные из кафки в Postgres (`trigger = 1 s`, checkpoint очереди в Object Storage) |

</details>

<details>
<summary>3. Созданы и выполнены задачи в Data Proc</summary>

Запись (Задание завершено, т.к. настроено завершение по окончаю данных)

![image](https://github.com/user-attachments/assets/d75d16c8-0305-4da0-9f0a-844c030051b8)

Чтение (Задание остановлено, т.к. настроено бесконечное чтение из кафки, работает до того, пока его не остановят руками)

![image](https://github.com/user-attachments/assets/211b10c6-7243-4c6c-a28a-775a78bf8749)


</details>

<details>
<summary>4. Итог</summary>

Задачи выполнились, данные из отчищенного parquet появились в базе Postgres.

![image](https://github.com/user-attachments/assets/40a57970-01a3-4bcc-8488-3d5729c4ea75)


</details>

**✅ Задание 3 выполнено.**
