up-gpu:
	@echo "Docker-compose up GPU cluster..."
	docker-compose -f *gpu.yaml up -d

start_dfs := docker exec -it recsys-namenode1 bash
up-hdfs:
	@echo "Deploy HDFS..."
	@echo "start-dfs.sh && exit" | xclip -selection c

	eval $(start_dfs)

start_yarn := docker exec -it recsys-resourcemanager bash
up-yarn:
	@echo "Deploy YARN..."
	@echo "start-yarn.sh && exit" | xclip -selection c
	eval $(start_yarn)

up-cluster: up-hdfs up-yarn

format_namenode1 := docker exec -it recsys-namenode1 bash
format_datanode1 := docker exec -it recsys-datanode1 \
	bash -c $$(echo '"') \
	rm -rf /home/hadoop/hdfs/* $$(echo '"')
format_datanode2 := docker exec -it recsys-datanode2 \
	bash -c $$(echo '"') \
	rm -rf /home/hadoop/hdfs/* $$(echo '"')
format_datanode3 := docker exec -it recsys-datanode3 \
	bash -c $$(echo '"') \
	rm -rf /home/hadoop/hdfs/* $$(echo '"')

format-hdfs:
	@echo "Format HDFS..."
	@echo "hdfs namenode -format && exit" | xclip -selection c
	eval $(format_namenode1)
	eval $(format_datanode1)
	eval $(format_datanode2)
	eval $(format_datanode3)

check-jps:
	@echo "===== JPS Namenode1"
	docker exec -it recsys-namenode1 bash -c "/opt/corretto-8/bin/jps"
	@echo "===== JPS Namenode2"
	docker exec -it recsys-namenode2 bash -c "/opt/corretto-8/bin/jps"
	@echo "===== JPS ResourceManager"
	docker exec -it recsys-resourcemanager bash -c "/opt/corretto-8/bin/jps"
	@echo "===== JPS Datanode1"
	docker exec -it recsys-datanode1 bash -c "/opt/corretto-8/bin/jps"
	@echo "===== JPS Datanode2"
	docker exec -it recsys-datanode2 bash -c "/opt/corretto-8/bin/jps"
	@echo "===== JPS Datanode3"
	docker exec -it recsys-datanode3 bash -c "/opt/corretto-8/bin/jps"

enter_safemode := docker exec -it recsys-namenode1 bash
get_safemode := docker exec -it recsys-namenode1 bash
save_namespace := docker exec -it recsys-namenode1 bash
stop_snn := docker exec -it recsys-namenode2 bash
mv_cp_nn1 := docker exec -it recsys-namenode2 \
	bash -c $$(echo '"') \
	mv /home/hadoop/hdfs/cp_nn1 /home/hadoop/hdfs/cp_nn1_$$(date +%s) $$(echo '"')
mv_cp_nn2 := docker exec -it recsys-namenode2 \
	bash -c $$(echo '"') \
	mv /home/hadoop/hdfs/cp_nn2 /home/hadoop/hdfs/cp_nn2_$$(date +%s) $$(echo '"')
mv_cp_edits1 := docker exec -it recsys-namenode2 \
	bash -c $$(echo '"') \
	mv /home/hadoop/hdfs/cp_edits1 /home/hadoop/hdfs/cp_edits1_$$(date +%s) $$(echo '"')
mv_cp_edits2 := docker exec -it recsys-namenode2 \
	bash -c $$(echo '"') \
	mv /home/hadoop/hdfs/cp_edits2 /home/hadoop/hdfs/cp_edits2_$$(date +%s) $$(echo '"')
exit_safemode := docker exec -it recsys-namenode1 bash
stop_dfs := docker exec -it recsys-namenode1 bash
down-hdfs:
	@echo "Down HDFS..."
	@echo "hdfs dfsadmin -safemode enter && exit" | xclip -selection c
	eval $(enter_safemode)

	@echo "hdfs dfsadmin -safemode get && exit" | xclip -selection c
	eval $(get_safemode)

	@echo "hdfs dfsadmin -saveNamespace && exit" | xclip -selection c
	eval $(save_namespace)

	@echo "hdfs --daemon stop secondarynamenode && exit" | xclip -selection c
	eval $(stop_snn)

	eval $(mv_cp_nn1)
	eval $(mv_cp_nn2)
	eval $(mv_cp_edits1)
	eval $(mv_cp_edits2)

	@echo "hdfs dfsadmin -safemode leave && exit" | xclip -selection c
	eval $(exit_safemode)

	@echo "stop-dfs.sh && exit" | xclip -selection c
	eval $(stop_dfs)

stop_yarn := docker exec -it recsys-resourcemanager bash
down-yarn:
	@echo "Down YARN..."
	@echo "stop-yarn.sh && exit" | xclip -selection c
	eval $(stop_yarn)

down-cluster: down-hdfs down-yarn

down-gpu:
	@echo "Docker-compose down GPU cluster..."
	docker-compose -f *gpu.yaml down --remove-orphans

up-dns:
	@echo "Up DNS router..."
	docker-compose -f *dns.yaml up -d

down-dns:
	@echo "Up DNS router..."
	docker-compose -f *dns.yaml up -d
