from setuptools import setup, find_packages
import sys, os

from taca import __version__

try:
    with open("requirements.txt", "r") as f:
        install_requires = [x.strip() for x in f.readlines()]
except IOError:
    install_requires = []

setup(name='taca',
    version=__version__,
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

    entry_points={
        'console_scripts': ['taca = taca.cli:cli'],
        'taca.subcommands': [
            'storage = taca.storage.cli:storage'
        ]
    },
    install_requires=install_requires
)
