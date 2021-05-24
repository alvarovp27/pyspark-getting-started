from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

import config
from pyspark_getting_started import word_count, read_mongodb

"""
Create SparkContext.

setMaster should be removed when run on cluster.
"""

example_data = \
    """
    Lorem ipsum dolor sit amet, consectetur adipiscing elit. Morbi mollis, velit sit amet pretium sagittis, erat mauris egestas nisl, eget facilisis lectus est et magna. Proin interdum libero a libero pretium vestibulum. Maecenas placerat, lacus lacinia tincidunt mattis, orci sapien consequat tortor, in volutpat neque ligula maximus nibh. Nullam tempor orci dolor, a condimentum massa porttitor a. Donec id ipsum et orci mollis condimentum. In feugiat mollis justo ut tincidunt. Suspendisse pharetra lacus feugiat mollis aliquet. Phasellus pretium egestas sagittis. Morbi tempus risus nec justo blandit, venenatis maximus felis interdum. Etiam ultrices vel dui vel vehicula. Quisque sit amet eros vehicula, eleifend tortor ac, elementum velit. In hac habitasse platea dictumst. Vestibulum faucibus suscipit erat, at sollicitudin metus consequat sit amet.
    Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Curabitur et feugiat erat, in rhoncus ante. Integer a magna et lectus lobortis vestibulum eget a mauris. Sed varius ante vitae diam tristique rutrum. Donec quis mauris eu eros faucibus accumsan quis a magna. Cras at libero vitae mi pellentesque mollis. Aliquam at diam metus. Suspendisse potenti. Mauris consequat eget neque ullamcorper molestie. Nam mollis erat quam, sed pellentesque dolor lacinia eu. Nam eget mauris in nisl aliquam efficitur. In pretium diam mauris, eget elementum justo ultrices at.
    """


def get_spark_context():
    conf = SparkConf().setAppName('pyspark_getting_started') \
        .setMaster("local[*]")

    # conf = conf.set("spark.executor.memory", "4g") \
    #    .set("spark.driver.memory", "4g") \
    #    .set("spark.executor.cores", 4) \
    #    .set("spark.driver.cores", 1)

    sc = SparkContext(conf=conf)

    return sc


def get_spark_session():
    spark_session_builder = SparkSession \
        .builder \
        .appName("pyspark_getting_started") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.mongodb.input.uri", config.mongo_uri) \
        .config("spark.mongodb.output.uri", config.mongo_uri) \
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")

        #.config("spark.sql.execution.arrow.pyspark.enabled", "false")

    # spark_session_builder = spark_session_builder \
    #     .config("spark.executor.memory", "4g") \
    #     .config("spark.driver.memory", "4g") \
    #     .config("spark.executor.cores", 4) \
    #     .config("spark.driver.cores", 1) \

    return spark_session_builder.getOrCreate()

# word_count.apply(get_spark_context(), example_data)
read_mongodb.apply(get_spark_session())
