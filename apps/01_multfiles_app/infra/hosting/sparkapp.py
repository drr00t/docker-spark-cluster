from abc import abstractmethod
from typing import NoReturn
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,date_format
from datetime import datetime, date
import os
import pandas as pd
from pyspark.sql import Row

from apps.app02_hosting.infra.baseapp import BaseApplication

os.environ['PYSPARK_PYTHON'] = "./environment/bin/python"


class SparkApplication(BaseApplication[SparkSession]):
  # _spk_session: SparkSession
  # _spk_context:  SparkContext
  _spk_builder:  SparkSession.Builder

  def __init__(self)-> None:
    builder = SparkSession.builder\
       .appName("app-01")  #.appName(self.app_name)
    
    # if self.master:
    #     builder.master(self.master)
    builder.master("spark://localhost:7077")

    # if self.enable_hive_support:
    #     builder.enableHiveSupport()

    # if self.config:
    #     for key, value in self.config.items():
    #         builder.config(key, value)
    builder \
      .config("spark.jars", "/opt/spark-apps/postgresql-42.2.22.jar") \
      .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
      .config("spark.archives","pyspark_venv.tar.gz#environment")

  def start(self)-> None:
    session: SparkSession = self._builder.getOrCreate()
    context: SparkContext = self._spk_session.sparkContext
    
    context.setLogLevel('WARN')

    self.execute(session, context)

  @abstractmethod
  def execute(self, session: SparkSession, context:  SparkContext) -> None:
        pass

  # def run(self) -> None:
    # self._spk_session = SparkSession.builder\
    # .appName("app-01")\

    # .master("spark://localhost:7077")\

    # .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    # .config("spark.archives","pyspark_venv.tar.gz#environment")\

    # .getOrCreate()
  # sc = self._spk_session.sparkContext
  # sc.setLogLevel('WARN')


# def init_spark():
#   sql = SparkSession.builder\
#     .appName("app-01")\
#     .master("spark://localhost:7077")\
#     .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
#     .config("spark.archives","pyspark_venv.tar.gz#environment")\
#     .getOrCreate()
#   sc = sql.sparkContext
#   sc.setLogLevel('WARN')

#   return sql,sc

# def main():
#   sql,sc = init_spark()
#   df = sql.createDataFrame([
#       Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
#       Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
#       Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
#   ], schema='a long, b double, c string, d date, e timestamp')

#   df.printSchema()
  # file = "/opt/spark-data/B63-2011-04-03_2011-05-03.csv"
  # df = sql.read.load(file,format = "csv", inferSchema="true", sep=",", header="true"
  #     )\
  #     .withColumn("report_hour",date_format(col("timestamp"),"yyyy-MM-dd HH:00:00")) \
  #     .withColumn("report_date",date_format(col("timestamp"),"yyyy-MM-dd"))
  
  # df.printSchema()

# if __name__ == '__main__':
#   main()