[![Documentation Status](https://readthedocs.org/projects/unicloud/badge/?version=latest)](https://unicloud.readthedocs.io/en/latest/?badge=latest)
[![Python Versions](https://img.shields.io/pypi/pyversions/unicloud.png)](https://img.shields.io/pypi/pyversions/unicloud)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/Serapieum-of-alex/unicloud.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/Serapieum-of-alex/unicloud/context:python)

![GitHub last commit](https://img.shields.io/github/last-commit/Serapieum-of-alex/unicloud)
![GitHub forks](https://img.shields.io/github/forks/Serapieum-of-alex/unicloud?style=social)
![GitHub Repo stars](https://img.shields.io/github/stars/Serapieum-of-alex/unicloud?style=social)
[![codecov](https://codecov.io/gh/Serapieum-of-alex/unicloud/branch/main/graph/badge.svg?token=g0DV4dCa8N)](https://codecov.io/gh/Serapieum-of-alex/unicloud)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/5e3aa4d0acc843d1a91caf33545ecf03)](https://www.codacy.com/gh/Serapieum-of-alex/unicloud/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=Serapieum-of-alex/unicloud&amp;utm_campaign=Badge_Grade)

![GitHub commits since latest release (by SemVer including pre-releases)](https://img.shields.io/github/commits-since/Serapieum-of-alex/unicloud/0.5.0?include_prereleases&style=plastic)
![GitHub last commit](https://img.shields.io/github/last-commit/Serapieum-of-alex/unicloud)

Current release info
====================

| Name                                                                                                                 | Downloads                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | Version                                                                                                                                                                                                                     | Platforms                                                                                                                                                                                                                                                                                                                                 |
|----------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [![Conda Recipe](https://img.shields.io/badge/recipe-unicloud-green.svg)](https://anaconda.org/conda-forge/unicloud) | [![Conda Downloads](https://img.shields.io/conda/dn/conda-forge/unicloud.svg)](https://anaconda.org/conda-forge/unicloud) [![Downloads](https://pepy.tech/badge/unicloud)](https://pepy.tech/project/unicloud) [![Downloads](https://pepy.tech/badge/unicloud/month)](https://pepy.tech/project/unicloud)  [![Downloads](https://pepy.tech/badge/unicloud/week)](https://pepy.tech/project/unicloud)  ![PyPI - Downloads](https://img.shields.io/pypi/dd/unicloud?color=blue&style=flat-square) | [![Conda Version](https://img.shields.io/conda/vn/conda-forge/unicloud.svg)](https://anaconda.org/conda-forge/unicloud) [![PyPI version](https://badge.fury.io/py/unicloud.svg)](https://badge.fury.io/py/unicloud) | [![Conda Platforms](https://img.shields.io/conda/pn/conda-forge/unicloud.svg)](https://anaconda.org/conda-forge/unicloud) [![Join the chat at https://gitter.im/unicloud/unicloud](https://badges.gitter.im/unicloud/unicloud.svg)](https://gitter.im/unicloud/unicloud?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) |

unicloud - GIS utility package
=====================================================================
**unicloud** is a GIS utility package using gdal, ....

unicloud

![1](/docs/images/package-work-flow/overall.png)

Main Features
-------------

- GIS modules to enable the modeler to fully prepare the meteorological inputs and do all the preprocessing
  needed to build the model (align rasters with the DEM), in addition to various methods to manipulate and
  convert different forms of distributed data (rasters, NetCDF, shapefiles)

Future work
-------------

- Developing a DEM processing module for generating the river network at different DEM spatial resolutions.

Installing unicloud
===============

Installing `unicloud` from the `conda-forge` channel can be achieved by:

```
conda install -c conda-forge unicloud=0.1.0
```

It is possible to list all the versions of `unicloud` available on your platform with:

```
conda search unicloud --channel conda-forge
```

## Install from GitHub

to install the last development to time, you can install the library from GitHub

```
pip install git+https://github.com/Serapieum-of-alex/unicloud
```

## pip

to install the last release, you can easily use pip

```
pip install unicloud==0.6.0
```

Quick start
===========

```
  >>> import unicloud
```

[other code samples](https://unicloud.readthedocs.io/en/latest/?badge=latest)

## Coverage

[![codecov](https://codecov.io/gh/Serapieum-of-alex/unicloud/branch/main/graphs/sunburst.svg?token=g0DV4dCa8N)](https://codecov.io/gh/Serapieum-of-alex/unicloud)
