# Custom service account roles
#       Compute Admin
#       Compute Storage Admin
#       Dataproc Editor
#       Dataproc Worker
#       Storage Admin
#       Storage Object Admin
#       Storage Object Creator
#       Storage Object Viewer

# Create dataproc cluster
# With specified service account
gcloud dataproc clusters create clpub-5478 \
    --region=asia-east1 \
    --service-account=clpub-453@clpub-2.iam.gserviceaccount.com \
    --single-node --master-machine-type n2-highmem-8 \
    --master-boot-disk-size 1000 --image-version 2.0-debian10 \
    --bucket clpub-cluster \
    --project clpub-2 \
    --optional-components HIVE_WEBHCAT,JUPYTER,ZEPPELIN,PRESTO,ZOOKEEPER \
    --scopes='https://www.googleapis.com/auth/cloud-platform'

# Create dataproc cluster default setting
gcloud dataproc clusters create recsys-c04a --bucket clpub \
    --region asia-east1 --zone asia-east1-a --single-node --master-machine-type n2-highmem-8 \
    --master-boot-disk-size 1000 --image-version 2.0-debian10 \
    --scopes 'https://www.googleapis.com/auth/cloud-platform' \
    --project clpub-2

# Create dataproc cluster for heavy jobs, 256gb ram
gcloud dataproc clusters create recsys-a3aa --bucket clpub-cluster \
    --service-account=clpub-453@clpub-2.iam.gserviceaccount.com \
    --region asia-east1 --zone asia-east1-a --single-node \
    --master-machine-type custom-8-262144-ext --master-boot-disk-size 1000 \
    --image-version 2.0-debian10 \
    --optional-components HIVE_WEBHCAT,ZEPPELIN,PRESTO,ZOOKEEPER \
    --scopes 'https://www.googleapis.com/auth/cloud-platform' \
    --master-min-cpu-platform "Intel Skylake" --project clpub-2

# Submit job
# Default run job by user root, caution of custom envs
gcloud dataproc jobs submit pyspark gs://clpub/source/ETLs/extract_features/push_published_history.py \
    --cluster=recsys-c04a \
    --region=asia-east1 \
    --properties="spark.pyspark.python=/opt/conda/default/bin/python,spark.pyspark.driver.python=/opt/conda/default/bin/python"

# Submit job directly
spark-submit -c spark.ui.showConsoleProgress=true gs://clpub/source/ETLs/extract_features/merge_coauthor.py

# Submit job with virtualenv
# Local is better on single node deploy
PYSPARK_PYTHON=/home/howard/miniconda3/envs/pyenv/bin/python spark-submit \
    -c spark.ui.showConsoleProgress=true \
    --master local[4] \
    gs://clpub/source/ETLs/extract_features/merge_coauthor.py \

# ===== Install + Deploy trino =====
# Default presto suck
# I love google cloud, but hate dataproc default image

# Trino require Java 17
wget https://cdn.azul.com/zulu/bin/zulu17.34.19-ca-jdk17.0.3-linux_x64.tar.gz
sudo tar -xvf zulu17.34.19-ca-jdk17.0.3-linux_x64.tar.gz -C /opt
sudo mv /opt/zulu17.34.19-ca-jdk17.0.3-linux_x64 /opt/java-17

wget https://repo1.maven.org/maven2/io/trino/trino-server/390/trino-server-390.tar.gz
sudo tar -xvf trino-server-390.tar.gz -C /opt
sudo mv /opt/trino-server-390 /opt/trino

sudo mkdir -p /opt/trino/etc
sudo mkdir -p /var/trino/data
sudo mkdir -p /opt/trino/etc/catalog
sudo touch /opt/trino/etc/node.properties
sudo touch /opt/trino/etc/jvm.config
sudo touch /opt/trino/etc/config.properties
sudo touch /opt/trino/etc/log.properties
sudo touch /opt/trino/etc/catalog/hive.properties

echo -e "\
node.environment=production \n\
node.id=a9d4b327-49f9-4fb2-846c-c3fe37464395 \n\
node.data-dir=/var/trino/data \n\
" | sudo tee /opt/trino/etc/node.properties

echo -e "\
-server \n\
-Xmx16G \n\
-XX:InitialRAMPercentage=80.0 \n\
-XX:MaxRAMPercentage=80.0 \n\
-XX:G1HeapRegionSize=32M \n\
-XX:+ExplicitGCInvokesConcurrent \n\
-XX:+ExitOnOutOfMemoryError \n\
-XX:+HeapDumpOnOutOfMemoryError \n\
-XX:-OmitStackTraceInFastThrow \n\
-XX:ReservedCodeCacheSize=512M \n\
-XX:PerMethodRecompilationCutoff=10000 \n\
-XX:PerBytecodeRecompilationCutoff=10000 \n\
-Djdk.attach.allowAttachSelf=true \n\
-Djdk.nio.maxCachedBufferSize=2000000 \n\
-XX:+UnlockDiagnosticVMOptions \n\
-XX:+UseAESIntrinsics \n\
" | sudo tee /opt/trino/etc/jvm.config

# Some how default 8080 port is being used
# Lesson learned, never touch default port pids
# Use a different port instead
echo -e "\
coordinator=true \n\
node-scheduler.include-coordinator=true \n\
http-server.http.port=8098 \n\
query.max-memory=40GB \n\
query.max-memory-per-node=1GB \n\
discovery.uri=http://localhost:8098 \n\
" | sudo tee /opt/trino/etc/config.properties

echo -e "io.trino=INFO\n" | sudo tee /opt/trino/etc/log.properties

# Start trino daemon
# Instead of 'launcher start', we can use 'launcher run' for debugging purpose
sudo bash -c "JAVA_HOME=/opt/java-17 PATH=/opt/java-17/bin/:$PATH /opt/trino/bin/launcher start"

# Setup Hive connector
# Default hive metastore port = 9083
echo -e "\
connector.name=hive
hive.metastore.uri=thrift://localhost:9083
" | sudo tee /opt/trino/etc/catalog/hive.properties

# Download Trino CLI, a simple Trino client for testing
wget https://repo1.maven.org/maven2/io/trino/trino-cli/390/trino-cli-390-executable.jar
mv trino-cli-390-executable.jar trino
sudo chmod +rwx trino

./trino --server=localhost:8098

# ===== =====

