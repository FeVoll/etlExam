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

*Задание 1 завершено*


