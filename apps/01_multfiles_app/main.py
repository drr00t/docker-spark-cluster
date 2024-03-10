from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,date_format

from app02_hosting.infra.hosting.sparkapp import SparkApplication

class SimpleApp01(SparkApplication):

  def execute(self, session: SparkSession, context: SparkContext) -> None:
    file = "/opt/spark-data/B63-2011-04-03_2011-05-03.csv"

    df = self.session.read.load(file,format = "csv", inferSchema="true", sep=",", header="true")\
        .withColumn("report_hour",date_format(col("timestamp"),"yyyy-MM-dd HH:00:00"))\
        .withColumn("report_date",date_format(col("timestamp"),"yyyy-MM-dd"))
    
    df.printSchema()


    # Filter invalid coordinates
    df.where("latitude <= 90 AND latitude >= -90 AND longitude <= 180 AND longitude >= -180") \
      .where("latitude != 0.000000 OR longitude !=  0.000000 ")


if __name__ == '__main__':
  SimpleApp01().start()