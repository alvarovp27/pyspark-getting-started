
"""
Receives a SparkSession object previously configured:
- "spark.mongodb.input.uri"
- "spark.mongodb.output.uri"
"""


def apply(spark):

    spark.read.format("mongo").load().show()

    spark.stop()
