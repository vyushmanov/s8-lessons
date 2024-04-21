import os
from time import sleep
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql import SparkSession, DataFrame

topic_in = 'student.topic.cohort18.nat_in'
topic_out = 'student.topic.cohort18.nat_out'



# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )
    
postgresql_settings = {
    'user': 'jovyan',
    'password': 'jovyan',
    'url': 'jdbc:postgresql://localhost:5432/de',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'public.subscribers_feedback',
}

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";'
}


app_name = 'RestaurantSubscribeStreamingService'
def spark_init(app_name) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", 
        spark_jars_packages).getOrCreate()
    )
    return spark
	
	
# читаем из топика Kafka сообщения с акциями от ресторанов 
def restaurant_read_stream(spark: SparkSession) -> DataFrame:
    df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .option('kafka.security.protocol', 'SASL_SSL') \
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
    .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";') \
    .option('subscribe', topic_in) \
    .load()

    return df
    
if __name__ == "__main__":
    spark = spark_init(app_name)
    df = restaurant_read_stream(spark)

    
    query = (df
             .writeStream
             .outputMode("append")
             .format("console")
             .option("truncate", False)
             .trigger(once=True)
             .start())
    query.awaitTermination()   
    
    
