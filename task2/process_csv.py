from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_date
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.utils import AnalysisException

# === Spark session ===
spark = SparkSession.builder.appName("TwizzlerData CSV to Parquet ETL").getOrCreate()

# === Пути ===
source_path = "s3a://etlexam/TwizzlerData.csv"
target_path = "s3a://etlexam/output/TwizzlerData_clean.parquet"

try:
    print(f"Чтение данных из: {source_path}")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(source_path)

    print("Схема исходных данных:")
    df.printSchema()

    # Преобразования:
    #  - парсинг временной метки
    #  - кастинг числовых полей
    #  - добавление даты обработки
    df = (
        df
        .withColumn("Timestamp", to_timestamp(col("Timestamp"), "M/d/yyyy H:mm:ss"))
        .withColumn("TwizzlerRating",      col("TwizzlerRating").cast(IntegerType()))
        .withColumn("SleepHours",          col("SleepHours").cast(DoubleType()))
        .withColumn("WorkHoursPerWeek",    col("WorkHoursPerWeek").cast(IntegerType()))
        .withColumn("processing_date",     current_date())
    )

    print("Схема после преобразований:")
    df.printSchema()

    # Удаление строк с любыми null
    df_clean = df.na.drop()

    print("Пример данных после чистки:")
    df_clean.show(5, truncate=False)

    print(f"Запись в Parquet: {target_path}")
    df_clean.write.mode("overwrite").parquet(target_path)

    print("✅ Данные успешно сохранены в Parquet.")

except AnalysisException as ae:
    print("❌ Ошибка анализа данных:", ae)
except Exception as e:
    print("❌ Общая ошибка:", e)
finally:
    spark.stop()
