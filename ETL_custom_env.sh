# Because default EMR python version is 3.7
conda create -n pyenv python=3.7

alias python=/home/hadoop/miniconda3/envs/pyenv/bin/python3

python -m pip install pyarrow seaborn findspark tensorflow
python -m pip install pyspark
python -m pip install torch torchvision torchaudio --extra-index-url https://download.pytorch.org/whl/cpu
python -m pip install ipython

# use within python os environ
export HADOOP_CONF_DIR=/etc/hadoop/conf