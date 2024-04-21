from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

# необходимая библиотека с идентификатором в maven
# вы можете использовать ее с помощью метода .config и опции "spark.jars.packages"
# spark_jars_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

# настройки security для кафки
# вы можете использовать из с помощью метода .options(**kafka_security_options)
kafka_security_options = {
    'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
    'subscribe': 'student.topic.cohort18.vyushmanov'
    }

def spark_init() -> SparkSession:
    spark_jars_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
    spark = (SparkSession.builder
                .master("local")
                .appName('test connect to kafka')
                .config("spark.jars.packages", spark_jars_packages)
                .getOrCreate()
            )
    return spark


def load_df(spark: SparkSession, kafka_security_options) -> DataFrame:
    raw_df  = (spark.readStream
                .format('kafka')
                .options(**kafka_security_options)
                .load())
    return raw_df


def transform(df: DataFrame) -> DataFrame:
    schema_true = StructType([StructField("client_id", StringType()),
                    StructField("timestamp" , DoubleType()),
                    StructField("lat" , DoubleType()),
                    StructField("lon" , DoubleType())])
    transform_df = (df.withColumn('value', F.col('value').cast(StringType()))\
        .withColumn('key', F.col('key').cast(StringType()))\
            .withColumn('event', F.from_json(F.col('value'), schema_true))\
                .select('event.client_id','event.timestamp', 'event.lon', 'event.lat')
        )
    return transform_df

spark = spark_init()

source_df = load_df(spark, kafka_security_options)

output_df = transform(source_df)

output_df.printSchema()


query = (output_df
         .writeStream
         .outputMode("append")
         .format("console")
         .option("truncate", False)
         .trigger(once=True)
         .start())
try:
    query.awaitTermination()
finally:
    query.stop()
