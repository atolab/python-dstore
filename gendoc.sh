#!/bin/sh
rm -Rf docs
mkdir docs
sphinx-apidoc -f -F -o docs dstore

cat doc.part >> docs/conf.py

cd docs
make html
cd _build/html
# python -mSimpleHTTPServer 9999 &
