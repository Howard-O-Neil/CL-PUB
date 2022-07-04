# Format namenode
sudo hdfs namenode -format

# Common web server

sudo systemctl stop nginx
sudo systemctl stop httpd

sudo systemctl start nginx
sudo systemctl start httpd

# Hadoop

sudo systemctl stop hadoop-hdfs-datanode
sudo systemctl stop hadoop-hdfs-namenode
sudo systemctl stop hadoop-kms
sudo systemctl stop hadoop-mapreduce-historyserver

sudo systemctl stop hadoop-yarn-nodemanager

sudo systemctl stop hadoop-yarn-proxyserver
sudo systemctl stop hadoop-yarn-resourcemanager
sudo systemctl stop hadoop-yarn-timelineserver

# sudo systemctl start hadoop-hdfs-datanode
# sudo systemctl start hadoop-hdfs-namenode
sudo hdfs --daemon start namenode
sudo hdfs --daemon start datanode

sudo systemctl start hadoop-kms
sudo systemctl start hadoop-mapreduce-historyserver

sudo systemctl start hadoop-yarn-nodemanager

sudo systemctl start hadoop-yarn-proxyserver
sudo systemctl start hadoop-yarn-resourcemanager
sudo systemctl start hadoop-yarn-timelineserver

# Hue
sudo systemctl stop hue

sudo systemctl start hue

# Oozie
sudo systemctl stop oozie

sudo systemctl start oozie

# Hive
sudo systemctl stop hive-hcatalog-server
sudo systemctl stop hive-server2

sudo systemctl start hive-hcatalog-server
sudo systemctl start hive-server2

# Spark
sudo systemctl stop spark-history-server

sudo systemctl start spark-history-server

# Zepplin
sudo systemctl stop zeppelin

sudo systemctl start zeppelin

# ZooKeeper
sudo systemctl stop zookeeper-server

sudo systemctl start zookeeper-server

# Hudi

# Tez
