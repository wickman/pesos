#!/usr/bin/env bash

THIRD_PARTY=$(dirname $0)

echo "THIRD_PARTY: $THIRD_PARTY"

git clone https://github.com/tornadoweb/tornado $THIRD_PARTY/tornado
pushd $THIRD_PARTY/tornado
  python setup.py sdist
popd

mv $THIRD_PARTY/tornado/dist/*.tar.gz $THIRD_PARTY
rm -rf $THIRD_PARTY/tornado
