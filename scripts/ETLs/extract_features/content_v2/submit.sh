export SPARK_HOME="/usr/lib/spark"
export PYSPARK_PYTHON="env/bin/python"
export HADOOP_CONF_DIR="/etc/hadoop/conf"

PYSPARK_PYTHON="env/bin/python" spark-submit \
    --name "Paper vect extraction" \
    --archives "/home/howard/pyenv.tar.gz#env" \
    gs://clpub/source/ETLs/extract_features/content_v2/extract_paper_vect.py
