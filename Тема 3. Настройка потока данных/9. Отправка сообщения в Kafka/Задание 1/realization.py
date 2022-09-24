from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType

TOPIC_NAME_91 = '' # Укажите здесь название Вашего топика student.topic.cohort<номер когорты>.<username>.out

def spark_init(test_name) -> SparkSession:
    pass


postgresql_settings = {
    'user': 'master',
    'password': 'de-master-password'
}


def read_marketing(spark: SparkSession) -> DataFrame:
    pass


kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-admin\" password=\"de-kafka-admin-2022\";'
}


def read_client_stream(spark: SparkSession) -> DataFrame:
    pass


def join(user_df, marketing_df) -> DataFrame:
    pass

if __name__ == "__main__":
    spark = spark_init('join stream')
    spark.conf.set("spark.sql.streaming.checkpointLocation", "test_query")
    client_stream = read_client_stream(spark)
    marketing_df = read_marketing(spark)
    result = join(client_stream, marketing_df)

    query = (result
             .writeStream
             .outputMode("append")
             .format("kafka")
             .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
             .options(**kafka_security_options)
             .option("topic", TOPIC_NAME_91)
             .trigger(processingTime="15 seconds")
             .option("truncate", False)
             .start())

    while query.isActive:
        print(f"query information: runId={query.runId}, "
              f"status is {query.status}, "
              f"recent progress={query.recentProgress}")
        sleep(30)

    query.awaitTermination()