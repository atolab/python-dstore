#!/usr/bin/env bash

$WD=$(PWD)

python3 setup.py sdist

cd dist
py2dsc --with-python3 True --with-python2 False dstore-0.1.0.tar.gz
cd deb_dist/dstore-0.1.0
dpkg-buildpackage -rfakeroot -uc -us

cp ../python-dstore_0.1.0-1_all.deb $WD
