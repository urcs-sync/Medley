#!/bin/sh

bash bootstrap.sh
mkdir -p trans-compile
cd trans-compile
../configure
make -j8 