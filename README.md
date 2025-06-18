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

**Задание 1 завершено**

# Задание 2 — Автоматизация работы с Yandex Data Processing при помощи Apache AirFlow

**Цель:** Автоматизировать обработку данных из Object Storage с помощью PySpark и Apache Airflow в Yandex Data Processing.

<details>
  <summary>Пункт 1. Подготовим инфраструктуру по инструкции</summary>

  Кластер Managed Service for Apache Airflow:
  ![image](https://github.com/user-attachments/assets/c5c9f684-ed56-4d5f-aa1c-54950cda3619)

  Кластер Metastore:
  ![image](https://github.com/user-attachments/assets/c477f99f-f755-42f7-bafc-fa281ef5a287)
</details>

<details>
  <summary>Пункт 2. Подготовим PySpark-задание</summary>

  Подготовим скрипты `create-table.py` и `Data-Processing-DAG.py` и разместим их в нашем Object Storage в соответствующих папках. Для `Data-Processing-DAG.py` пришлось изменить под более "легкую" конфигурацию кластера.

  Скрипты находятся в папке task2.
</details>

<details>
  <summary>Пункт 3. Запустим DAG через веб-интерфейс и проверим результат выполнения</summary>

  Спустя 7 попыток и правок DAG запустился успешно:
  ![image](https://github.com/user-attachments/assets/c5093ce7-685b-4c7e-a2d5-fd9368cdde5e)

  В Object Storage в папке countries появились нужные файлы и логи:
  ![image](https://github.com/user-attachments/assets/a88562a6-3491-49bb-8efe-0d242931e3b8a)
  ![image](https://github.com/user-attachments/assets/1b589eaa-e2c4-4695-b53c-6fd2542abc1f)
</details>

**Задание 2 завершено**
