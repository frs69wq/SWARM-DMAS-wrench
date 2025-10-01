#!/bin/bash

sudo apt update 
sudo apt install -y cmake libboost-all-dev jed nlohmann-json3-dev

wget --no-check-certificate https://framagit.org/simgrid/simgrid/-/archive/v4.0/simgrid-v4.0.tar.gz && tar -xf simgrid-v4.0.tar.gz && cd simgrid-v4.0 && cmake . && make -j`nproc` && sudo make install && cd .. && rm -rf simgrid-v4.0*
wget --no-check-certificate https://github.com/simgrid/file-system-module/archive/refs/tags/v0.3.tar.gz && tar -xf v0.3.tar.gz && cd file-system-module-0.3  && cmake . && make -j`nproc` && sudo make install && cd .. && rm -rf file-system-module-0.3*
git clone --depth=1 https://github.com/wrench-project/wrench.git && cd wrench && cmake . && make -j`nproc` && sudo make install && cd .. && rm -rf DTLMod*

rm -rf build && mkdir build && cd build && cmake .. && make -j`nproc` && cd ..
