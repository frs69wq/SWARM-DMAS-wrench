FROM wrenchproject/wrench-build:ubuntu-noble-gcc13

LABEL org.opencontainers.image.authors="silvarf@ornl.gov,henric@hawaii.edu"


#################################################
# INSTALL WRENCH
#################################################

USER root
WORKDIR /tmp

RUN echo "wrench ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# install libasio-dev
RUN apt-get update
RUN apt-get install -y python3 python3-pip python3-venv libasio-dev

# Upgrade pip to the latest version
RUN python3 -m venv /opt/venv
RUN /opt/venv/bin/python3 -m pip install --upgrade pip

# Install jsonref and python dependencies for the SWARM simulator
COPY requirements.txt .
RUN /opt/venv/bin/pip install --break-system-packages jsonref -r requirements.txt

RUN git config --global http.postBuffer 524288000
# reinstall SimGrid
RUN git clone https://github.com/simgrid/simgrid.git && cd simgrid && mkdir build && cd build && cmake .. && make -j12 && make install && cd .. && /bin/rm -rvf simgrid

# reinstall FSMod
RUN git clone https://github.com/simgrid/file-system-module.git && cd file-system-module && mkdir build && cd build && cmake .. && make -j12 && make install && cd .. && /bin/rm -rvf file-system-module

# install WRENCH
RUN git clone https://github.com/wrench-project/wrench.git && cd wrench && mkdir build && cd build && cmake .. && make -j12 wrench-daemon examples && make install && cp -r ./examples /home/wrench/ && chown -R wrench /home/wrench && cd ../.. && /bin/rm -rf wrench

# fix the run_all_examples.sh script
RUN sed -i "s/INSTALL_DIR=.*/INSTALL_DIR=\/home\/wrench\/examples/" /home/wrench/examples/run_all_examples.sh


#################################################
# WRENCH's user
#################################################
USER wrench
WORKDIR /home/wrench

RUN cd
RUN git clone https://github.com/frs69wq/SWARM-DMAS-wrench.git && cd SWARM-DMAS-wrench && mkdir build && cd build && cmake .. && make -j && cd ..


# set user's environment variable
ENV PATH="/opt/venv/bin:$PATH"
ENV CXX="g++-13" CC="gcc-13"
ENV LD_LIBRARY_PATH=/usr/local/lib

