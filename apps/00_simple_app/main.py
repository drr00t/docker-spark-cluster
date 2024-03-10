from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
from datetime import datetime, date
import os
from pyspark.sql import Row

os.environ['PYSPARK_PYTHON'] = "./app.pex"

def init_spark():
    builder = SparkSession.builder.appName("00_SimpleApp")
    
    # builder.master("spark://192.168.2.16:7077")
    
    builder.config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.executorEnv.PEX_ROOT","./app.pex") \
        .config("spark.files", "app.pex") 
    
    sql = builder.getOrCreate()

    sc = sql.sparkContext
    sc.setLogLevel('WARN')

    return sql


def main():
    sql = init_spark()
    df = sql.createDataFrame([
        Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
        Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
    ], schema='a long, b double, c string, d date, e timestamp')

    df.show(truncate=False)
    sql.stop()


if __name__ == '__main__':
    main()
