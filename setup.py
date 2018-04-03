#!/usr/bin/env python3

from setuptools import setup

setup(
    name='dstore',
    version='0.1.0',
    packages=['dstore', ],
    author='kydos',
    url='https://github.com/atolab/dstore-python',
    install_requires=['python-cdds','flask','websockets'],
    scripts=['bin/dstore-rest-server', 'bin/dstore-websocket-server', 'bin/dstore-client']
)
