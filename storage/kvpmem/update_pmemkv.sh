#!bin/bash

# go to kvdk
cd ./extra/pmemkv

git submodule update .

mkdir -p build && cd build && rm -rf *

cmake .. -DBUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=Debug -DENGINE_CSMAP=1 -DCXX_STANDARD=14 -DCMAKE_INSTALL_PREFIX=/usr && make -j

