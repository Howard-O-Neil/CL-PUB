import pyspark

JAVA_HOME = "/opt/corretto-8"

conf = new pyspark.SparkConf()
            .setJars([
                f"{JAVA_HOME}/rapids-4-spark_2.12-22.04.0.jar",
                f"{JAVA_HOME}/cudf-22.04.0-cuda11.jar",
            ])
            .set("spark.plugins", "com.nvidia.spark.SQLPlugin")
            .set("spark.rapids.sql.incompatibleOps.enabled", "true")
            .setMaster("local[*]")
            .setAppName("Recsys")

sc = new pyspark.SparkContext(conf)
