# activate environment
source activate develop

cd /vagrant

# install the package for development
python setup.py develop

# install development dependencies
pip install -r ./requirements-dev.txt
