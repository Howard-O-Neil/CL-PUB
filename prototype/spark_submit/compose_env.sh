python -m venv .venv # create .venv IF NOT EXISTED
source ./.venv/bin/activate

./.venv/bin/python -m pip install -U pip
./.venv/bin/python -m pip install -r requirements.txt

pushd .venv/ # its like cd into venv
sudo zip -rq ../venv.zip *
popd # revert pushd

sudo zip -rq application.zip application/
sudo chown hadoop:1001 *.zip