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

unicloud - Cloud utility package
=====================================================================
- **Unicloud** provides robust and intuitive tools designed to simplify interactions with AWS S3 and Google
Cloud Storage (GCS), offering developers a streamlined API for managing cloud storage operations.
- Whether you're uploading data backups, retrieving files for analysis, or managing cloud storage resources
programmatically, our package ensures a seamless and efficient experience. Built with flexibility and ease of use
in mind, it supports a wide range of operations, including file uploads, downloads, and storage management tasks,
all while maintaining high security and reliability standards.
- Ideal for developers working in cloud-native
environments, data scientists requiring reliable data storage solutions, or businesses looking to automate their
cloud storage workflows, this package aims to enhance productivity and facilitate the seamless integration of
cloud storage capabilities into Python applications.

Documentation
=============
- Full documentation is available at [Read the Docs](https://unicloud.readthedocs.io/en/latest/?badge=latest)

Installing unicloud
===================

Installing `unicloud` from the `conda-forge` channel can be achieved by:

```
conda install -c conda-forge unicloud=0.4.0
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
pip install unicloud==0.4.0
```

to install only the Google Cloud storage part, you can use the following command :

```
pip install unicloud[gcs]==0.4.0
```

to install only the AWS S3 part, you can use the following command :

```
pip install unicloud[s3]==0.4.0
```

to indtall all the dependencies, you can use the following command :

```
pip install unicloud[all]==0.4.0
```

Quick start
===========

```
  >>> import unicloud
```

[other code samples](https://unicloud.readthedocs.io/en/latest/?badge=latest)
