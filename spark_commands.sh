pyspark
spark-shell
spark-submit

# With graphframes
pyspark --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11
spark-submit --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11

# With aws
spark-shell --packages com.amazonaws:aws-java-sdk:1.12.232,org.apache.hadoop:hadoop-aws:3.3.1

# Performance tuning
spark-shell --num-executors 5 --executor-cores 6 --executor-memory 11G