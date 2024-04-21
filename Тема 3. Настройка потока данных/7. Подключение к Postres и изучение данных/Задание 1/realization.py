from pyspark.sql import SparkSession

spark = (
        SparkSession.builder.appName('table rows count')
        .config("spark.jars.packages", "org.postgresql:postgresql:42.4.0")
        .getOrCreate()
    )

db_pass = 'de-student'
df = (spark.read
        .format('jdbc')
        .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de')
        .option('driver', 'org.postgresql.Driver')
        .option('dbtable', 'public.marketing_companies')
        .option('user', 'student')
        .option('password', db_pass)
        .load())
df.count()