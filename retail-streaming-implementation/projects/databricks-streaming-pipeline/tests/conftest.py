import os
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    session = (SparkSession.builder.master("local[2]").appName("retail-tests")
        .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
