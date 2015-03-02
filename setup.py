from setuptools import setup, find_packages
import sys, os

try:
    with open("requirements.txt", "r") as f:
        install_requires = [x.strip() for x in f.readlines()]
except IOError:
    install_requires = []

version = '0.1.1'

setup(name='taca',
      version=version,
      description="Tool for the Automation of Cleanup and Analyses",
      long_description='This package contains a set of functionalities that are '
                       'useful in the day-to-day tasks of bioinformatitians in '
                       'National Genomics Infrastructure in Stockholm, Sweden.',
      keywords='bioinformatics',
      author='Guillermo Carrasco',
      author_email='guille.ch.88@gmail.com',
      url='http://taca.readthedocs.org/en/latest/',
      license='MIT',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      scripts=['scripts/taca', 'scripts/run_tracker.py'],
      install_requires=install_requires
      )
