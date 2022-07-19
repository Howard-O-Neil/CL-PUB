# [EMR ONLY]
wget  https://repo.anaconda.com/miniconda/Miniconda3-py37_4.12.0-Linux-x86_64.sh
chmod +rwx ./Miniconda3-py37_4.12.0-Linux-x86_64.sh
./Miniconda3-py37_4.12.0-Linux-x86_64.sh

# install in base env, available in all sub env
conda install -c conda-forge conda-pack

# [EMR ONLY]
# Because default EMR python version is 3.7
conda create -n pyenv python=3.7
alias python=/home/hadoop/miniconda3/envs/pyenv/bin/python3
python -m pip install boto3

# [DATAPROC ONLY]
wget  https://repo.anaconda.com/miniconda/Miniconda3-py37_4.12.0-Linux-x86_64.sh
chmod +rwx ./Miniconda3-py37_4.12.0-Linux-x86_64.sh
./Miniconda3-py37_4.12.0-Linux-x86_64.sh

conda create -n pyenv python=3.7
conda activate pyenv
python -m pip install --upgrade urllib3
python -m pip install --upgrade numpy scipy
python -m pip install --upgrade pandas seaborn
python -m pip install --upgrade tensorflow
python -m pip install --upgrade torch torchvision torchaudio --extra-index-url https://download.pytorch.org/whl/cpu
python -m pip install --upgrade ipython
python -m pip install --upgrade Faker
python -m pip install --upgrade orjson
python -m pip install --upgrade findspark pyspark
python -m pip install --upgrade pyarrow
python -m pip install --upgrade "fastapi[all]"
python -m pip install --upgrade "uvicorn[standard]"
conda install -c pytorch faiss-cpu
sudo apt install libsasl2-dev
python -m pip install --upgrade sasl
python -m pip install --upgrade thrift
python -m pip install --upgrade thrift-sasl
python -m pip install --upgrade 'pyhive[presto]'
python -m pip install --upgrade 'pyhive[hive]'

# use within python os environ
export HADOOP_CONF_DIR=/etc/hadoop/conf

# create zip for ETLs
zip -r pyenv.zip /home/howard/miniconda3/envs/pyenv
