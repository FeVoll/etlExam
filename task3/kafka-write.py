import logging
from math import ceil

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, row_number, concat_ws,
    to_json, struct
)

# ── параметры ────────────────────────────────────────────────
PARQUET_PATH  = "s3a://etlexam/transactions_clean"
KAFKA_SERVERS = "rc1b-u621afv7ehqmg2a2.mdb.yandexcloud.net:9091"
KAFKA_TOPIC   = "dataproc-kafka-topic"
KAFKA_USER    = "user1"
KAFKA_PASS    = "password1"
BATCH_SIZE    = 100
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("parquet→kafka")

def main() -> None:
    spark = (
        SparkSession.builder
        .appName("parquet-to-kafka-batches-key")
        .getOrCreate()
    )

    df = spark.read.parquet(PARQUET_PATH)
    window = Window.orderBy(lit(1))
    df = df.withColumn("_row_id", row_number().over(window) - 1)

    df = df.withColumn(
        "key",
        concat_ws("-", col("Customer_ID"), col("Date").cast("string"))
    )

    total   = df.count()
    batches = ceil(total / BATCH_SIZE)
    log.info("Всего строк: %d, будет %d пакетов по %d", total, batches, BATCH_SIZE)

    current = 0
    while current < total:
        batch_df = (
            df.filter((col("_row_id") >= current) &
                      (col("_row_id") <  current + BATCH_SIZE))
              .drop("_row_id")
        )


        json_df = batch_df.select(
            col("key").cast("string"),
            to_json(struct(*[c for c in batch_df.columns if c != "key"])).alias("value")
        )

        # Kafka-sink
        (json_df.write
               .format("kafka")
               .option("kafka.bootstrap.servers", KAFKA_SERVERS)
               .option("topic", KAFKA_TOPIC)
               .option("kafka.security.protocol", "SASL_SSL")
               .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
               .option(
                   "kafka.sasl.jaas.config",
                   f'org.apache.kafka.common.security.scram.ScramLoginModule required '
                   f'username="{KAFKA_USER}" password="{KAFKA_PASS}";')
               .save())

        bn = current // BATCH_SIZE + 1
        log.info("пакет %d/%d отправлен (%d строк)", bn, batches, batch_df.count())
        current += BATCH_SIZE

    log.info("Готово! Отправили все %d строк без ошибок.", total)
    spark.stop()

if __name__ == "__main__":
    main()
