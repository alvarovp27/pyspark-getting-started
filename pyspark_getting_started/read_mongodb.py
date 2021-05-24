import pandas
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pyspark.sql.functions as f

"""
Receives a SparkSession object previously configured:
- "spark.mongodb.input.uri"
- "spark.mongodb.output.uri"
"""

# a pdf might be empty, and that could mean several things
# https://stackoverflow.com/questions/24652417/how-to-check-if-pandas-series-is-empty
def is_empty_nans(pdf):
    return pdf.dropna(how='all').empty

def apply(spark: SparkSession):

    # key: the id of this group
    # pdf: the values inside this group
    @pandas_udf("id string, temperature double", PandasUDFType.GROUPED_MAP)
    def mean_custom(pdf: pandas.DataFrame):
        key = pdf['id'].iloc[0]
        mean = pdf['temperature'].mean()
        #print(key)
        #print(mean)
        return pd.DataFrame([[key] + [mean]])

    @pandas_udf("id string, avgTempTanque double, avgTempPasteurizador double", PandasUDFType.GROUPED_MAP)
    def avg_temperature_by_sensor(pdf: pandas.DataFrame):
        key = pdf['id'].iloc[0]

        tanque = pdf.loc[pdf['sensor'] == 'tanque']['avgTemperature']
        pasteurizador = pdf.loc[pdf['sensor'] == 'pasteurizador']['avgTemperature']
        #pasteurizador = pdf.filter(like="tanque")
        print(tanque)
        print(pasteurizador)
        #print(avg_temp)

        # workaround: cast panda series to float to avoid troubles with spark arrow (https://spark.apache.org/docs/2.4.0/sql-pyspark-pandas-with-arrow.html)

        #return pd.DataFrame([[key] + [None if tanque is None else float(tanque)] + [None if pasteurizador is None else float(pasteurizador)]])

        return pd.DataFrame([[key] + [float(tanque) if not is_empty_nans(tanque) else None] + [float(pasteurizador) if not is_empty_nans(pasteurizador) else None]])


    ds = spark.read.format("mongo").load() #.show()

    ds.show()

    basic_udf_example = ds.groupBy("id").apply(mean_custom)

    # basic_udf_example.show()

    full_grouped = ds.groupBy("id", "sensor").agg(f.avg("temperature").alias("avgTemperature")).groupBy("id").apply(avg_temperature_by_sensor)

    full_grouped.show()



    #spark.stop()
