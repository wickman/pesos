#!/bin/bash

TMPDIR=$(mktemp -d)

# TODO(wickman) This is a bug in pex, should be fixed.
rm -rf $HOME/.pex

pushd $TMPDIR
  git clone /vagrant pesos-copy
  pex -s pesos-copy -v -v -o pesos.pex
  cp -f pesos.pex $HOME/pesos.pex
popd

rm -rf $TMPDIR
