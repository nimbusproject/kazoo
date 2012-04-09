#!/usr/bin/env python

import os
import codecs

VERSION = "0.1.0"

if os.path.exists("README.rst"):
    long_description = codecs.open('README.rst', "r", "utf-8").read()
else:
    long_description = "See http://github.com/nimbusproject/kazoo"


setupdict = {
    'name' : 'kazoo',
    'version' : VERSION,
    'description' : 'ZooKeeper client that supports async frameworks',
    'long_description' : long_description,
    'license' : 'Apache 2.0',
    'author' : 'Nimbus team',
    'author_email' : 'nimbus@mcs.anl.gov',
    'keywords': ['zookeeper','gevent', 'nimbus'],
    'classifiers' : [
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    "Topic :: Communications",
    "Topic :: System :: Distributed Computing",
    "Topic :: System :: Networking",
    "Topic :: Software Development :: Libraries :: Python Modules",],
    "url" : "https://github.com/nimbusproject/kazoo",
    "download_url" : "http://www.nimbusproject.org/downloads/kazoo-%s.tar.gz" % VERSION,
}

from setuptools import setup, find_packages
setupdict['packages'] = find_packages()
setupdict['install_requires'] = []
setupdict['tests_require'] = ['nose']
setupdict['test_suite'] = 'nose.collector'

setup(**setupdict)
