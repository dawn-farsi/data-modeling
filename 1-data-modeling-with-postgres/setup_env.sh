#!/bin/sh
sudo apt-get install python3.8-dev
virtualenv -p python3.8 ./venv3.8
source ./venv3.8/bin/activate
./venv3.8/bin/pip3 install -r requirements
./venv3.8/bin/pip3 install jupyterlab
#etc.