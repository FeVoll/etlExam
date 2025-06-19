from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, current_date, concat_ws, trim
)
from pyspark.sql.types import LongType, DoubleType

# ─── Пути в Object Storage ───────────────────
SOURCE = "s3a://etlexam/data.csv"          # CSV-файл
TARGET = "s3a://etlexam/transactions_clean"  # Папка-выход Parquet

spark = (
    SparkSession.builder
    .appName("Transactions CSV → Parquet (clean)")
    .getOrCreate()
)

try:
    # -------------------------------------------------------------------------
    # Читаем CSV
    # -------------------------------------------------------------------------
    print(f"► Читаем CSV из {SOURCE}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(SOURCE)
    )

    # -------------------------------------------------------------------------
    # Фильтр строк с пустыми Name / Surname
    # -------------------------------------------------------------------------
    df = df.filter(
        col("Name").isNotNull() & (trim(col("Name")) != "") &
        col("Surname").isNotNull() & (trim(col("Surname")) != "")
    )

    # -------------------------------------------------------------------------
    # Создаём «чистые» колонки и приводим типы
    # -------------------------------------------------------------------------
    df = (
        df
        # числовые / денежные
        .withColumn("Customer_ID",        col("Customer ID").cast(LongType()))
        .withColumn("Transaction_Amount", col("Transaction Amount").cast(DoubleType()))
        # даты
        .withColumn("Birthdate", to_date(col("Birthdate"), "yyyy-MM-dd"))
        .withColumn("Date",      to_date(col("Date"),      "yyyy-MM-dd"))
        # новый FullName
        .withColumn("FullName",  concat_ws(" ", col("Name"), col("Surname")))
        # переименование колонки-сорца
        .withColumnRenamed("Merchant Name", "Merchant_Name")
        # дата обработки
        .withColumn("processing_date", current_date())
    )

    # -------------------------------------------------------------------------
    # Оставляем и упорядочиваем ровно 9 нужных полей
    # -------------------------------------------------------------------------
    keep_cols = [
        "Customer_ID",        # bigint
        "FullName",           # string
        "Gender",             # string
        "Transaction_Amount", # double
        "Birthdate",          # date
        "Date",               # date
        "Merchant_Name",      # string
        "Category",           # string
        "processing_date"     # date
    ]

    df_clean = (
        df.select(*keep_cols)  # отбрасываем всё лишнее
          .na.drop()           # на всякий случай убираем записи с NULL
    )

    # -------------------------------------------------------------------------
    # Быстрая проверка и запись
    # -------------------------------------------------------------------------
    print("Итоговая схема:")
    df_clean.printSchema()
    print("Пример строк:")
    df_clean.show(5, truncate=False)

    print(f"Записываем Parquet в {TARGET}")
    (
        df_clean
        .write
        .mode("overwrite")
        .parquet(TARGET)
    )
    print("Готово")

except Exception as exc:
    # вывод полной трассировки для отладки
    import traceback, sys
    traceback.print_exc(file=sys.stderr)
    print("Ошибка:", exc)
finally:
    spark.stop()
