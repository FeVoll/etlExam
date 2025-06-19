from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_date, concat_ws, trim
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.utils import AnalysisException

# === Spark session ===
spark = SparkSession.builder \
    .appName("Transactions CSV ETL with FullName") \
    .getOrCreate()

# === Пути ===
source_path = "s3a://etlexam/data.csv"
target_path = "s3a://etlexam/transactions_clean"

try:
    print(f"Чтение данных из: {source_path}")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(source_path)

    print("Схема исходных данных:")
    df.printSchema()

    # 1) Убираем строки, где Name или Surname пустые или NULL
    df = df.filter(
        col("Name").isNotNull() & (trim(col("Name")) != "") &
        col("Surname").isNotNull() & (trim(col("Surname")) != "")
    )

    # 2) Приведение типов, формат дат, добавление FullName и processing_date
    df = (
        df
        .withColumn("Customer_ID",        col("Customer ID").cast(IntegerType()))
        .withColumn("Transaction_Amount", col("Transaction Amount").cast(DoubleType()))
        .withColumn("Birthdate",          to_date(col("Birthdate"), "yyyy-MM-dd"))
        .withColumn("Date",               to_date(col("Date"),      "yyyy-MM-dd"))
        .withColumn("FullName",           concat_ws(" ", col("Name"), col("Surname")))
        .withColumn("processing_date",    current_date())
    )

    # 3) Удаляем оставшиеся строки с любыми другими NULL
    df_clean = df.na.drop()

    print("Схема после преобразований:")
    df_clean.printSchema()

    print("Первые 5 строк после очистки и объединения имени:")
    df_clean.select("Customer_ID", "FullName", "Transaction_Amount", "Date", "Category", "processing_date") \
            .show(5, truncate=False)

    print(f"Запись в Parquet: {target_path}")
    df_clean.write.mode("overwrite").parquet(target_path)

    print("Данные успешно сохранены в Parquet.")

except Exception as e:
    print("Общая ошибка:", e)
finally:
    spark.stop()
