reset-dns:
	@echo "[Reseting dns...]"
	@docker-compose -f *gpu.yaml restart docker-proxy-dns

update-tensor:
	@echo "[Fixing tensor image...]"
	@echo "[Please paste your clipboard...]"
	@docker-compose -f *gpu.yaml up -d build-tensor
	@docker exec -u root -it build-tensor bash
	@echo "[Committing fix...]"
	@docker commit build-tensor mingkhoi/recsys:fixbug-version1

update-hdfs:

	@echo "[Fixing hdfs image...]"
	@echo "[Please paste your clipboard...]"
	@docker-compose -f *gpu.yaml up -d build-hdfs
	@docker exec -u root -it build-hdfs bash
	@echo "[Committing fix...]"
	@docker commit build-hdfs mingkhoi/recsys-hdfs:fixbug-version1
	@docker-compose -f *gpu.yaml down

update-container:
	@echo "Fixing some startup errors..."
	update-tensor
	update-hdfs

up-gpu:
	@echo "Docker-compose up GPU cluster..."
	@docker-compose -f *gpu.yaml up

start_dfs := ssh hadoop@recsys-namenode1 $$(echo '"') \
	bash -c $$(echo '\"') $$(cat hadoop.env | tr '\n' ' ') /opt/hadoop-3/sbin/start-dfs.sh $$(echo '\"') \
	$$(echo '"')
up-hdfs:
	@echo "Deploy HDFS..."
	eval $(start_dfs)

start_yarn := ssh hadoop@recsys-resourcemanager $$(echo '"') \
	bash -c $$(echo '\"') $$(cat hadoop.env | tr '\n' ' ') /opt/hadoop-3/sbin/start-yarn.sh $$(echo '\"') \
	$$(echo '"')
up-yarn:
	@echo "Deploy YARN..."
	eval $(start_yarn)

start_sparkcluster := ssh hadoop@recsys-sparkmaster1 $$(echo '"') \
	bash -c $$(echo '\"') \
		$$(cat hadoop.env | tr '\n' ' ') /opt/spark/sbin/start-master.sh --properties-file /opt/spark/conf/spark.conf $$(echo '&&') \
	 	$$(cat hadoop.env | tr '\n' ' ') /opt/spark/sbin/start-workers.sh --properties-file /opt/spark/conf/spark.conf $$(echo '\"') \
	$$(echo '"')
up-spark:
	@echo "Deploy SPARK Cluster..." 
	eval $(start_sparkcluster)

up-cluster: up-hdfs up-yarn

format_namenode1 := ssh hadoop@recsys-namenode1 $$(echo '"') \
	bash -c $$(echo '\"') $$(cat hadoop.env | tr '\n' ' ') /opt/hadoop-3/bin/hdfs namenode -format $$(echo '\"') \
	$$(echo '"')
format_datanode1 := docker exec -it recsys-datanode1 \
	bash -c $$(echo '"') \
	rm -rf /home/hadoop/hdfs/* $$(echo '"')
format_datanode2 := docker exec -it recsys-datanode2 \
	bash -c $$(echo '"') \
	rm -rf /home/hadoop/hdfs/* $$(echo '"')
format_datanode3 := docker exec -it recsys-datanode3 \
	bash -c $$(echo '"') \
	rm -rf /home/hadoop/hdfs/* $$(echo '"')
format_datanode4 := docker exec -it recsys-datanode4 \
	bash -c $$(echo '"') \
	rm -rf /home/hadoop/hdfs/* $$(echo '"')
format_datanode5 := docker exec -it recsys-datanode5 \
	bash -c $$(echo '"') \
	rm -rf /home/hadoop/hdfs/* $$(echo '"')	
format-hdfs:
	@echo "Format HDFS..."
	eval $(format_namenode1)

	@echo "[Formatting datanode1...]"	
	eval $(format_datanode1)

	@echo "[Formatting datanode2...]"
	eval $(format_datanode2)

	@echo "[Formatting datanode3...]"	
	eval $(format_datanode3)

	@echo "[Formatting datanode4...]"	
	eval $(format_datanode4)

	@echo "[Formatting datanode5...]"	
	eval $(format_datanode5)		

check-jps:
	@echo "[JPS Namenode1...]"
	@docker exec -it recsys-namenode1 bash -c "/opt/corretto-8/bin/jps"
	@echo "[JPS Namenode2...]"
	@docker exec -it recsys-namenode2 bash -c "/opt/corretto-8/bin/jps"
	@echo "[JPS ResourceManager...]"
	@docker exec -it recsys-resourcemanager bash -c "/opt/corretto-8/bin/jps"
	@echo "[JPS Datanode1...]"
	@docker exec -it recsys-datanode1 bash -c "/opt/corretto-8/bin/jps"
	@echo "[JPS Datanode2...]"
	@docker exec -it recsys-datanode2 bash -c "/opt/corretto-8/bin/jps"
	@echo "[JPS Datanode3...]"
	@docker exec -it recsys-datanode3 bash -c "/opt/corretto-8/bin/jps"
	@echo "[JPS Datanode4...]"
	@docker exec -it recsys-datanode4 bash -c "/opt/corretto-8/bin/jps"
	@echo "[JPS Datanode5...]"
	@docker exec -it recsys-datanode5 bash -c "/opt/corretto-8/bin/jps"

	@echo "[JPS Spark master...]"
	@docker exec -it recsys-sparkmaster1 bash -c "/opt/corretto-8/bin/jps"
	@echo "[JPS Spark worker1...]"
	@docker exec -it recsys-sparkworker1 bash -c "/opt/corretto-8/bin/jps"
	@echo "[JPS Spark worker2...]"
	@docker exec -it recsys-sparkworker2 bash -c "/opt/corretto-8/bin/jps"
	@echo "[JPS Spark worker3...]"
	@docker exec -it recsys-sparkworker3 bash -c "/opt/corretto-8/bin/jps"

save_namespace := ssh hadoop@recsys-namenode1 $$(echo '"') \
	bash -c $$(echo '\"') \
		$$(cat hadoop.env | tr '\n' ' ') /opt/hadoop-3/bin/hdfs dfsadmin -safemode enter $$(echo '&&') \
		$$(cat hadoop.env | tr '\n' ' ') /opt/hadoop-3/bin/hdfs dfsadmin -safemode get $$(echo '&&') \
		$$(cat hadoop.env | tr '\n' ' ') /opt/hadoop-3/bin/hdfs dfsadmin -saveNamespace $$(echo '\"') \
	$$(echo '"')
stop_snn := ssh hadoop@recsys-namenode2 $$(echo '"') \
	bash -c $$(echo '\"') $$(cat hadoop.env | tr '\n' ' ') \
		/opt/hadoop-3/bin/hdfs --daemon stop secondarynamenode $$(echo '\"') \
	$$(echo '"')
mv_cp_nn1 := ssh hadoop@recsys-namenode2 $$(echo '"') \
	bash -c $$(echo '\"') \
	mv /home/hadoop/hdfs/cp_nn1 /home/hadoop/hdfs/cp_nn1_$$(date +%s) $$(echo '\"') $$(echo '"')
mv_cp_nn2 := ssh hadoop@recsys-namenode2 $$(echo '"') \
	bash -c $$(echo '\"') \
	mv /home/hadoop/hdfs/cp_nn2 /home/hadoop/hdfs/cp_nn2_$$(date +%s) $$(echo '\"') $$(echo '"')
mv_cp_edits1 := ssh hadoop@recsys-namenode2 $$(echo '"') \
	bash -c $$(echo '\"') \
	mv /home/hadoop/hdfs/cp_edits1 /home/hadoop/hdfs/cp_edits1_$$(date +%s) $$(echo '\"') $$(echo '"')
mv_cp_edits2 := ssh hadoop@recsys-namenode2 $$(echo '"') \
	bash -c $$(echo '\"') \
	mv /home/hadoop/hdfs/cp_edits2 /home/hadoop/hdfs/cp_edits2_$$(date +%s) $$(echo '\"') $$(echo '"')
stop_dfs := ssh hadoop@recsys-namenode1 $$(echo '"') \
	bash -c $$(echo '\"') \
		$$(cat hadoop.env | tr '\n' ' ') /opt/hadoop-3/bin/hdfs dfsadmin -safemode leave $$(echo '&&') \
		$$(cat hadoop.env | tr '\n' ' ') /opt/hadoop-3/sbin/stop-dfs.sh $$(echo '\"') \
	$$(echo '"')
down-hdfs:
	@echo "Down HDFS..."
	@echo "[Saving namespace...]"
	eval $(save_namespace)

	@echo "[Stopping secondary namenode...]"
	eval $(stop_snn)

	@echo "[Backup images checkpoint1...]"
	eval $(mv_cp_nn1)
	@echo "[Backup edits checkpoint1...]"
	eval $(mv_cp_edits1)

	@echo "[Stopping hdfs...]"
	eval $(stop_dfs)

stop_yarn := ssh hadoop@recsys-resourcemanager $$(echo '"') \
	bash -c $$(echo '\"') \
		$$(cat hadoop.env | tr '\n' ' ') /opt/hadoop-3/sbin/stop-yarn.sh $$(echo '\"') \
	$$(echo '"')
down-yarn:
	@echo "Down YARN..."
	@echo "[Stopping yarn...]"
	eval $(stop_yarn)

stop_sparkcluster := ssh hadoop@recsys-sparkmaster1 $$(echo '"') \
	bash -c $$(echo '\"') \
		$$(cat hadoop.env | tr '\n' ' ') /opt/spark/sbin/stop-all.sh $$(echo '\"') \
	$$(echo '"')
down-spark:
	@echo "Down SPARK Cluster..." 

	@echo "[Stopping spark cluster...]"
	eval $(stop_sparkcluster)

down-cluster: down-hdfs down-yarn

down-gpu:
	@echo "Docker-compose down GPU cluster..."
	@docker-compose -f *gpu.yaml down --remove-orphans

off-gpu:
	@echo "Turning off GPU cluster..."
	@docker stop recsys-namenode1 recsys-sparkmaster1 build-hdfs build-tensor recsys-sparkworker3 recsys-server1 recsys-datanode1 recsys-resourcemanager recsys-datanode5 recsys-datanode4 recsys-datanode2 recsys-sparkworker1 recsys-sparkworker2 recsys-datanode3 recsys-namenode2

rm-gpu:
	@echo "Removing GPU cluster..."
	@docker rm -f recsys-namenode1 recsys-sparkmaster1 build-hdfs build-tensor recsys-sparkworker3 recsys-server1 recsys-datanode1 recsys-resourcemanager recsys-datanode5 recsys-datanode4 recsys-datanode2 recsys-sparkworker1 recsys-sparkworker2 recsys-datanode3 recsys-namenode2

up-dns:
	@echo "Up DNS router..."
	@docker-compose -f *dns.yaml up -d

down-dns:
	@echo "Up DNS router..."
	@docker-compose -f *dns.yaml down
