"""
This is the "front door" of your application which gets submitted
to Spark and serves as the starting point. If you needed to
implement a command-line inteface, for example, you'd invoke the
setup from here.
"""
import pyspark
import numpy
from application.util import metaphone_udf

import os
os.environ["HADOOP_CONF_DIR"] = "/opt/hadoop-3/etc/hadoop"

if __name__ == '__main__':
    spark = (
        pyspark.sql.SparkSession.builder.getOrCreate())

    # spark.sparkContext.setLogLevel('WARN')

    df = spark.read.option("delimiter", ",") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/data/adult_data.csv")

    df.crosstab('age', 'income').sort("age_income").show()

    test_np = np.array([1, 2, 3, 4, 5])
    print(test_np)

