from typing import List
from datetime import datetime, date
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.testing.utils import assertDataFrameEqual


@pytest.fixture
def get_spark_session():
    spark = SparkSession.builder.appName("Testing App").getOrCreate()
    yield spark


@pytest.fixture
def get_data():
    yield [
        Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
        Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
    ]


def test_dataframe_created_ok(get_spark_session: SparkSession, get_data: List[Row]):
    df: DataFrame = get_spark_session.createDataFrame(get_data,
                                                      schema='a long, b double, c string, d date, e timestamp')
    assert df.count() == 3 