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

# Heavy workload
pyspark --num-executors 10 --executor-cores 3 --executor-memory 20G --conf spark.dynamicAllocation.enabled=false
pyspark --num-executors 15 --executor-cores 2 --executor-memory 14G --conf spark.dynamicAllocation.enabled=false

# Clear HDFS scratch space
hdfs dfs -rm -rf /temp