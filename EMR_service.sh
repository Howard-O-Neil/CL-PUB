# Common web server

sudo systemctl restart nginx
sudo systemctl restart httpd

# Hadoop

sudo systemctl restart hadoop-hdfs-namenode
sudo systemctl restart hadoop-hdfs-datanode
sudo systemctl restart hadoop-kms
sudo systemctl restart hadoop-mapreduce-historyserver
sudo systemctl restart hadoop-yarn-nodemanager
sudo systemctl restart hadoop-yarn-proxyserver
sudo systemctl restart hadoop-yarn-resourcemanager
sudo systemctl restart hadoop-yarn-timelineserver

# Hue
sudo systemctl restart hue

# Oozie
sudo systemctl restart oozie

# Hive
sudo systemctl restart hive-hcatalog-server
sudo systemctl restart hive-server2

# Spark
sudo systemctl restart spark-history-server

# Zepplin
sudo systemctl restart zeppelin

# ZooKeeper
sudo systemctl restart zookeeper-server

# Hudi

# Tez
