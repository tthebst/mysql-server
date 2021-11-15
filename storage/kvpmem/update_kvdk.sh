#!bin/bash

# go to kvdk
cd ./extra/kvdk

git submodule update --remote --merge . 

mkdir -p build && cd build && rm -rf *

cmake .. -DCMAKE_BUILD_TYPE=Release && make -j
cp libengine.so ../../../lib/libengine.so 
mkdir -p ../../../include/kvdk/
cp ../include/kvdk/* ../../../include/kvdk/

