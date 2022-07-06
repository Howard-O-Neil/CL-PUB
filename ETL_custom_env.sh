alias python=/home/hadoop/miniconda3/envs/pyenv/bin/python3

python -m pip install pyarrow seaborn findspark tensorflow
python -m pip install pyspark
python -m pip install torch torchvision torchaudio --extra-index-url https://download.pytorch.org/whl/cpu

# use within python os environ
export HADOOP_CONF_DIR=/etc/hadoop/conf