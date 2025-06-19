from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType, DateType
)

# ─── Kafka ──────────────────────────────────────────────────
BOOTSTRAP = "rc1b-u621afv7ehqmg2a2.mdb.yandexcloud.net:9091"
TOPIC     = "dataproc-kafka-topic"
KUSER     = "user1"
KPASS     = "password1"
# ────────────────────────────────────────────────────────────

# ─── Postgres ───────────────────────────────────────────────
PG_HOST = "rc1b-8ebjpb2gsdokfo30.mdb.yandexcloud.net"
PG_PORT = 6432
PG_DB   = "db1"
PG_USER = "user1"
PG_PASS = "Volodin1!"
PG_URL  = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_TABLE = "transactions_stream"
# ────────────────────────────────────────────────────────────

# ─── JSON schema ────────────────────────────────────────────
schema = StructType([
    StructField("Customer_ID",        LongType(),   True),
    StructField("FullName",           StringType(), True),
    StructField("Gender",             StringType(), True),
    StructField("Transaction_Amount", DoubleType(), True),
    StructField("Birthdate",          DateType(),   True),
    StructField("Date",               DateType(),   True),
    StructField("Merchant_Name",      StringType(), True),
    StructField("Category",           StringType(), True),
    StructField("processing_date",    DateType(),   True),
])
# ────────────────────────────────────────────────────────────


def ensure_table(spark: SparkSession) -> None:
    """DDL через JVM-JDBC (без psycopg2). Выполняется один раз на драйвере."""
    ddl = """
    CREATE TABLE IF NOT EXISTS transactions_stream (
        customer_id        BIGINT,
        fullname           TEXT,
        gender             TEXT,
        transaction_amount DOUBLE PRECISION,
        birthdate          DATE,
        date               DATE,
        merchant_name      TEXT,
        category           TEXT,
        processing_date    DATE,
        PRIMARY KEY (customer_id, date)
    );
    """
    jvm = spark._jvm
    props = jvm.java.util.Properties()
    props.setProperty("user", PG_USER)
    props.setProperty("password", PG_PASS)
    conn = jvm.java.sql.DriverManager.getConnection(PG_URL, props)
    stmt = conn.createStatement()
    stmt.execute(ddl)
    stmt.close()
    conn.close()


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("kafka-to-postgres-stream")
        .getOrCreate()
    )

    # 0) табличка
    ensure_table(spark)

    # 1) Kafka stream
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("kafka.security.protocol", "SASL_SSL")
           .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
           .option("kafka.sasl.jaas.config",
                   f'org.apache.kafka.common.security.scram.ScramLoginModule required '
                   f'username="{KUSER}" password="{KPASS}";')
           .option("startingOffsets", "latest")
           .load())

    parsed = (raw.selectExpr("CAST(value AS STRING) AS js")
                    .select(from_json(col("js"), schema).alias("d"))
                    .select("d.*")
                    .withColumn("Birthdate", to_date(col("Birthdate")))
                    .withColumn("Date",      to_date(col("Date")))
                    .withColumn("processing_date", to_date(col("processing_date"))))

    # 2) writer
    def write_jdbc(df, _):
        (df.write
           .jdbc(PG_URL, PG_TABLE,
                 mode="append",
                 properties={
                     "user": PG_USER,
                     "password": PG_PASS,
                     "driver": "org.postgresql.Driver"
                 }))

    (parsed.writeStream
           .foreachBatch(write_jdbc)
           .option("checkpointLocation", "s3a://etlexam/kafka-to-pg-checkpoint")
           .trigger(processingTime="1 seconds")
           .start()
           .awaitTermination())


if __name__ == "__main__":
    main()
