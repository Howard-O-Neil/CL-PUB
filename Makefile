up-gpu:
	docker-compose -f *gpu.yaml up -d
up-hdfs:
	docker exec -u hadoop -it recsys-namenode1 bash
up-yarn:
	docker exec -u hadoop -it recsys-resourcemanager bash
check-jps:
	docker exec -u hadoop recsys-namenode1 bash -c "/opt/corretto-8/bin/jps"
	docker exec -u hadoop recsys-namenode2 bash -c "/opt/corretto-8/bin/jps"
	docker exec -u hadoop recsys-resourcemanager bash -c "/opt/corretto-8/bin/jps"
	docker exec -u hadoop recsys-datanode1 bash -c "/opt/corretto-8/bin/jps"
	docker exec -u hadoop recsys-datanode2 bash -c "/opt/corretto-8/bin/jps"
	docker exec -u hadoop recsys-datanode3 bash -c "/opt/corretto-8/bin/jps"

down-gpu:
	docker-compose -f *gpu.yaml down --remove-orphans