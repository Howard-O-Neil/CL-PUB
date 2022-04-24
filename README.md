# Articles & Collaborators recommender
A recommender system for scholar articles and authors

## "mingkhoi/recsys-hdfs:latest" custom files
+ `/etc/bash.bashrc`
  + environment
  + pdsh fix
+ `~/.bashrc`
  + simple color SUSE PS1
+ `~/.ssh`
  + authorize key
+ `/opt`
  + amazon corretto 8
  + hadoop 3.3.2
+ `/opt/hadoop-3/libexec/hadoop-functions.sh`
  + pdsh fix

## "mingkhoi/recsys:latest" custom files 
+ `/opt`
  + amazon corretto 8
  + spark 3.2.1
+ `/opt/corretto-8/lib`
  + rapids 4 spark      .jar
  + cudf cuda11         .jar

## Some good commands
+ `jps` List java process, (hadoop daemon is java process)
+ `start-dfs.sh` Start all dfs, namenode + datanode. Remote access through ssh
+ `start-yarn.sh` Start all yarn, resource manager + node manager

All the config is mounted from host