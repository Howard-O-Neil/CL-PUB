export SPARK_HOME="/opt/spark"
export PYSPARK_PYTHON="/root/anaconda3/envs/pyenv/bin/python"
export HADOOP_CONF_DIR="/recsys/prototype/spark_submit/hdfs_cfg"

spark-submit \
    --name "Sample Spark Summit" \
    --master yarn \
    --deploy-mode client \
    --conf "spark.yarn.appMasterEnv.SPARK_HOME=$SPARK_HOME" \
    --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=$PYSPARK_PYTHON" \
    --archives "venv.zip#venv" \
    --py-files "application.zip" \
    main.py
