# Задание 1 — Yandex DataTransfer

**Цель:** Перенести данные из YDB в Object Storage с помощью DataTransfer.

## Пункт 1. Создадим базу данных Managed Service for YDB

![image](https://github.com/user-attachments/assets/9e34b78b-1c2c-4ee0-b6ab-f5cac62a56fe)

## Пункт 2. Создим бакет Object Storage.

![image](https://github.com/user-attachments/assets/bdbeb996-5857-499d-b0cf-7886db75f2e8)

## Пункт 3. Создим сервисный аккаунт с ролями storage.editor и ydb.editor.

![image](https://github.com/user-attachments/assets/a579f606-5d3e-45d2-baaf-444b12ce0110)

## Пункт 4. Создадим и загрузим данные в базу. 

Скрипты ```create_table.yql``` и ```upsert_all_data.yql```

## Пункт 5. Создадим трансфер в Object Storage из YDB с Yandex DataTransfer

Источник

![image](https://github.com/user-attachments/assets/7329afad-64b1-45c5-81cb-a90989f8178c)

Приемник

![image](https://github.com/user-attachments/assets/62779a5f-a11b-46ec-8ac4-bf2e49159cc8)

Трансфер

![image](https://github.com/user-attachments/assets/6c932bc9-b744-4bd8-abec-27fb35b524e0)
![image](https://github.com/user-attachments/assets/43c856c0-cbc8-4d1a-a4ae-db312607474a)

Данные появились в Obj Storage, все верно

![image](https://github.com/user-attachments/assets/0d694099-5276-4003-bfc7-355a9b8d6c41)
![image](https://github.com/user-attachments/assets/d743e6f2-0328-45e8-88a8-212f16e174f1)

**Задание 1 завершено**

# Задание 2 — Автоматизация работы с Yandex Data Processing при помощи Apache AirFlow

**Цель:** Автоматизировать обработку данных из Object Storage с помощью PySpark и Apache Airflow в Yandex Data Processing.

## Пункт 1. Подготовим инфраструктуру по инструкции

Кластер Managed Service for Apache Airflow

![image](https://github.com/user-attachments/assets/c5c9f684-ed56-4d5f-aa1c-54950cda3619)

Кластер Metastore

![image](https://github.com/user-attachments/assets/c477f99f-f755-42f7-bafc-fa281ef5a287)

## Пункт 2. Подготовим PySpark-задание

Подготовим скрипты ```create-table.py``` и ```Data-Processing-DAG.py``` и разместим их в нашем ObjectStorage в соответсвущих папках. Для Data-Processing-DAG.py пришлось изменить под более "легкую" конфигурацию кластера.
(В гите в папке task2)

## Пункт 3. Запустим даг через веб интерфейс и проверим результат выполнения

Спустя 7 попыток и правок Даг запустился успешно:

![image](https://github.com/user-attachments/assets/c5093ce7-685b-4c7e-a2d5-fd9368cdde5e)

В Object Storage в папке countries появились нужные файлы и логи

![image](https://github.com/user-attachments/assets/a88562a6-3491-49bb-8efe-0d242931e3b8)

![image](https://github.com/user-attachments/assets/1b589eaa-e2c4-4695-b53c-6fd2542abc1f)

**Задание 2 завершено**
