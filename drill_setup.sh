sudo wget http://apache.mirrors.hoobly.com/drill/drill-1.19.0/apache-drill-1.19.0.tar.gz
sudo tar -xvf apache-drill-1.19.0.tar.gz
sudo rm -rf apache-drill-1.19.0.tar.gz
sudo mv apache-drill-1.19.0 apache-drill

export DRILL_HOME=/usr/lib/apache-drill
export DRILL_SITE=/etc/drill

sudo cp conf/drill-env.sh /etc/drill/
sudo cp conf/drill-on-yarn-example.conf /etc/drill/drill-on-yarn.conf
sudo cp conf/drill-override-example.conf /etc/drill/drill-override.conf

export HADOOP_CONF_DIR=/etc/hadoop/conf

./drill-on-yarn.sh --site /home/hadoop/apache-drill-1.19.0/conf
