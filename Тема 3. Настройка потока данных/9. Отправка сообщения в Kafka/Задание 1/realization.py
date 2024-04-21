from datetime import datetime
from time import sleep

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

TOPIC_NAME_91 = 'student.topic.cohort18.vyushmanov.out'  # Это топик, в который Ваше приложение должно отправлять сообщения. Укажите здесь название Вашего топика student.topic.cohort<номер когорты>.<username>.out
TOPIC_NAME_IN = 'student.topic.cohort18.vyushmanov' # Это топик, из которого Ваше приложение должно читать сообщения. Укажите здесь название Вашего топика student.topic.cohort<номер когорты>.<username>

# При первом запуске ваш топик student.topic.cohort<номер когорты>.<username>.out может не существовать в Kafka и вы можете увидеть такие сообщения:
# ERROR: Topic student.topic.cohort<номер когорты>.<username>.out error: Broker: Unknown topic or partition
# Это сообщение говорит о том, что тест начал проверять работу Вашего приложение, но так как Ваше приложение ещё не отправило туда сообщения, то топик ещё не создан. Нужно подождать несколько минут.

kafka_security_options = {
    'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
    'subscribe': TOPIC_NAME_IN
}

def spark_init(app_name) -> SparkSession:
    spark_jars_packages = ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
        , "org.postgresql:postgresql:42.4.0"
    ])
    spark = (SparkSession.builder
             .master("local")
             .appName(app_name)
             .config("spark.jars.packages", spark_jars_packages)
             .getOrCreate()
             )
    return spark


def read_marketing(spark: SparkSession) -> DataFrame:
    db_pass = 'de-student'
    postgresql_settings = {
        'url': 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de'
        , 'driver': 'org.postgresql.Driver'
        , 'dbtable': 'public.marketing_companies'
        , 'user': 'student'
        , 'password': db_pass
    }
    df = (spark.read
          .format('jdbc')
          .options(**postgresql_settings)
          .load())
    return df


def read_client_stream(spark: SparkSession) -> DataFrame:
    kafka_security_options = {
        'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
        'kafka.security.protocol': 'SASL_SSL',
        'kafka.sasl.mechanism': 'SCRAM-SHA-512',
        'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
        'subscribe': TOPIC_NAME_IN
    }

    df = (spark.readStream
          .format('kafka')
          .options(**kafka_security_options)
          .load())

    schema_true = StructType([
        StructField("lat", DoubleType()),
        StructField("client_id", StringType()),
        StructField("lon", DoubleType()),
        StructField("timestamp", TimestampType())
    ])
    df_res = (df.withColumn('value', F.col('value').cast(StringType()))
              .withColumn('key', F.col('key').cast(StringType()))
              .withColumn('event', F.from_json(F.col('value'), schema_true))
              .select('event.client_id', 'timestamp', 'event.lat', 'event.lon')
              )
    df_dedup = (df_res
                .withWatermark('timestamp', '10 minute')
                .dropDuplicates(['client_id', 'lat', 'lon'])
                )
    return df_dedup


def join(user_df, marketing_df) -> DataFrame:
    res = (user_df.crossJoin(marketing_df)
            .withColumn("correction_timestamp", F.date_format(F.col('timestamp'), '1970-01-01 HH:mm:ss'))
            .withColumn("end_time_corr", F.when(F.col("end_time") == '1970-01-01 00:00:00', '1970-01-02 00:00:00'))
            # .where('correction_timestamp > start_time and correction_timestamp < end_time_corr')
            .withColumn("distance", 2
                       * 6371
                       * F.asin(
        F.sqrt(
            F.pow(F.sin((F.radians(F.col('lat')) - F.radians(F.col('point_lat'))) / F.lit(2)), 2)
            + (F.cos(F.radians(F.col('point_lat')))
               * F.cos(F.radians(F.col('lat')))
               * F.pow(F.sin((F.radians(F.col('lon')) - F.radians(F.col('point_lon'))) / F.lit(2)), 2)
               )
        )
    )
                       )
            .withColumn('currentTS', F.current_timestamp())
            .where('distance < 1')
            .selectExpr(["client_id"
                           , "distance as distance"
                           , "id as adv_campaign_id"
                           , "name as adv_campaign_name"
                           , "description as adv_campaign_description"
                           , "substring(string(start_time), 12, 5) as adv_campaign_start_time"
                           , "substring(string(end_time), 12, 5) as adv_campaign_end_time"
                           , "point_lat as adv_campaign_point_lat"
                           , "point_lon as adv_campaign_point_lon"
                           , "currentTS as created_at"
                        ])
            .withColumn('value'
                        ,F.to_json(F.struct(F.col('client_id')
                                            ,F.col('distance')
                                            ,F.col('adv_campaign_id')
                                            ,F.col('adv_campaign_name')
                                            ,F.col('adv_campaign_description')
                                            ,F.col('adv_campaign_start_time')
                                            ,F.col('adv_campaign_end_time')
                                            ,F.col('adv_campaign_point_lat')
                                            ,F.col('adv_campaign_point_lon')
                                            ,F.col('created_at')
                                            )))
            .selectExpr(['value'])
           )
    return res

def run_query(df):
    return (df
            .writeStream
            .outputMode("append")
            .format("kafka")
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
            .option("kafka.security.protocol", 'SASL_SSL')
            .option("tokafka.sasl.mechanismpic", 'SCRAM-SHA-512')
            .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";')
            .option("topic", TOPIC_NAME_91)
            .option("checkpointLocation", "test_query")
            .trigger(processingTime="15 seconds")
            .start())


if __name__ == "__main__":
    spark = spark_init('join stream')
    client_stream = read_client_stream(spark)
    marketing_df = read_marketing(spark)
    output = join(client_stream, marketing_df)
    query = run_query(output)

    while query.isActive:
        print(f"query information: runId={query.runId}, "
              f"status is {query.status}, "
              f"recent progress={query.recentProgress}")
        sleep(30)

    query.awaitTermination()
