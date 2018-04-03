#!/bin/sh
rm -Rf docs
mkdir docs
sphinx-apidoc -f -F -o docs dstore
cd docs
make html
cd _build/html
# python -mSimpleHTTPServer 9999 &
