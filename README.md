<p align="center">
  <a href="https://github.com/SciLifeLab/TACA">
    <img width="512" height="175" src="artwork/logo.png"/>
  </a>
</p>

## Tool for the Automation of Cleanup and Analyses

[![PyPI version](https://badge.fury.io/py/taca.svg)](http://badge.fury.io/py/taca)
[![Build Status](https://travis-ci.org/SciLifeLab/TACA.svg?branch=master)](https://travis-ci.org/SciLifeLab/TACA)
[![Documentation Status](https://readthedocs.org/projects/taca/badge/?version=latest)](https://readthedocs.org/projects/taca/?badge=latest)

This package contains several tools for projects and data management in the [National Genomics Infrastructure](https://portal.scilifelab.se/genomics/) in Stockholm, Sweden.

### Install for development
You can setup a demo/development environment using [Vagrant][vagrant] provisioned by [Ansible][ansible].

```bash
$ ansible-galaxy install robinandeer.miniconda
$ vagrant up && vagrant ssh

# [inside virtual machince]
$ bash /vagrant/provisioning/run-me.sh
```

For a more detailed documentation please go to [the documentation page](http://taca.readthedocs.org/en/latest/).
