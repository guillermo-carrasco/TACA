# activate environment
source activate develop

# install development dependencies
cd /vagrant
pip install -r ./requirements-dev.txt

# install the package for development
python setup.py develop
