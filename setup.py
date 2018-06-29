#!/usr/bin/env python3

from setuptools import setup

setup(
    name='dstore',
    version='0.1.1',
    packages=['dstore', ],
    author='kydos',
    url='https://github.com/atolab/dstore-python',
    install_requires=['python-cdds','websockets'],
    scripts=['bin/dstore-websocket-server', 'bin/dstore-client']
)
