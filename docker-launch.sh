netwrok_args="--driver=bridge --subnet=128.0.1.0/24 collaborative-recommender-network"

application_args="--name collab -d "
application_args+="-v ${PWD}/src:/collaborative-recommender/src:rw "
application_args+="-v ${PWD}/requirement.txt:/collaborative-recommender/requirement.txt:rw "
application_args+="-v ${PWD}/container_share.log:/container_share.log:rw "
application_args+="--network=collaborative-recommender-network "
application_args+="--ip=128.0.1.2 "
application_args+="--gpus all "
application_args+="--device /dev/nvidia0 "
application_args+="--device /dev/nvidiactl "
application_args+="--device /dev/nvidia-caps "
application_args+="--device /dev/nvidia-modeset "
application_args+="--device /dev/nvidia-uvm "
application_args+="--device /dev/nvidia-uvm-tools "

docker network rm collaborative-recommender-network | true && \
docker network create $netwrok_args && \
docker container rm -f collab | true && \
docker run $application_args mingkhoi/tensorrt-ubuntu18.04-cuda11.4:version1.0 tail -f /container_share.log