# Environment on cluster
export CLUSTER_SPARK_HOME="/opt/spark"
export CLUSTER_PYSPARK_PYTHON="/virtual/python/bin/python"
export HADOOP_CONF_DIR="/recsys/prototype/spark_submit/hdfs_cfg"

spark-submit \
    --name "Sample Spark Summit" \
    --master yarn \
    --deploy-mode client \
    --conf "spark.yarn.appMasterEnv.SPARK_HOME=$CLUSTER_SPARK_HOME" \
    --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=$CLUSTER_PYSPARK_PYTHON" \
    --conf "spark.yarn.jars=hdfs://128.0.5.3:9000/lib/java/spark/jars/*.jar" \
    --py-files "application.zip" \
    main.py
