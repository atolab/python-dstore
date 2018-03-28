#!/usr/bin/env python3

from setuptools import setup

setup(
    name='dstore',
    version='0.1.0',
    packages=['dstore', ],
    author='kydos',
    url='https://github.com/atolab/dstore-python',
    install_requires=['python-cdds'],
    scripts=['bin/drest-server', 'bin/dwebsocket-server']
)
