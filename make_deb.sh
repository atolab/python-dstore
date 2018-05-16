

$WD=$(PWD)

python3 setup.py sdist

cd dist
py2dsc dstore-0.1.0.tar.gz
cd deb_dist/dstore-0.1.0
dpkg-buildpackage -rfakeroot -uc -us

cp ../python-dstore_0.1.0-1_all.deb $WD
