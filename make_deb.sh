#!/usr/bin/env bash


python3 setup.py sdist

cd dist
py2dsc --with-python3 True --with-python2 False dstore-0.0.9.tar.gz
cd deb_dist/dstore-0.0.9
dpkg-buildpackage -rfakeroot -uc -us

