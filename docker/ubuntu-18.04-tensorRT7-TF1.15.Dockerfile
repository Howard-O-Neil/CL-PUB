ARG BASE_IMG=ubuntu-18.04-cuda10-cudnn7
FROM mingkhoi/$BASE_IMG:version1.0
LABEL maintainer="Howard O'Neil"

ARG OS_VERSION=18.04
ARG OS_ARCH=amd64
ARG TRT_VERSION=7.0.0.11

# Copy all deb + tar
# Download to host OS, put it in the same directory as this dockefile
# Nvidia download require credentials so no wget

# tensorRT
COPY download/TensorRT-7.0.0.11.Ubuntu-18.04.x86_64-gnu.cuda-10.0.cudnn7.6.tar.gz /

# copy NGC
COPY download/ngccli_linux.zip /usr/local/bin

WORKDIR /

# Install TensorRT
RUN tar -xzvf TensorRT-$TRT_VERSION.Ubuntu-18.04.x86_64-gnu.cuda-10.0.cudnn7.6.tar.gz -C /usr/local/
RUN cd /usr/local/TensorRT-$TRT_VERSION/python && python3 -m pip install tensorrt-7.0.0.11-cp36-none-linux_x86_64.whl

# Install convert-to-uff
RUN cd /usr/local/TensorRT-$TRT_VERSION/uff && python3 -m pip install *.whl

# Install graphsurgeon
RUN cd /usr/local/TensorRT-$TRT_VERSION/graphsurgeon && python3 -m pip install *.whl

# Remove tar
RUN rm -rf TensorRT-$TRT_VERSION.Ubuntu-18.04.x86_64-gnu.cuda-10.0.cudnn7.6.tar.gz

# Install PyPI packages
RUN pip3 install --upgrade pip
RUN pip3 install setuptools>=41.0.0
RUN pip3 install tensorflow-gpu==1.15
RUN pip3 install jupyter jupyterlab

# Install Cmake
RUN cd /tmp && \
    wget https://github.com/Kitware/CMake/releases/download/v3.14.4/cmake-3.14.4-Linux-x86_64.sh && \
    chmod +x cmake-3.14.4-Linux-x86_64.sh && \
    ./cmake-3.14.4-Linux-x86_64.sh --prefix=/usr/local --exclude-subdir --skip-license && \
    rm ./cmake-3.14.4-Linux-x86_64.sh

# Configure NGC
RUN cd /usr/local/bin && unzip ngccli_linux.zip && chmod u+x ngc && rm ngccli_linux.zip ngc.md5 && echo "no-apikey\nascii\n" | ngc config set

# Set environment + workdir
ENV PATH=/usr/local/cuda/bin:$PATH
ENV PATH=/usr/local/TensorRT-$TRT_VERSION/bin:$PATH

# CUDA 64 bit libs
# ALso the first LD_LIBRARY_PATH value
ENV LD_LIBRARY_PATH=/usr/local/cuda/lib64

# CUPTI 64 bit libs
ENV LD_LIBRARY_PATH=/usr/local/cuda/extras/CUPTI/lib64:$LD_LIBRARY_PATH

# Set TensorRT environment
ENV TRT_LIBPATH=/usr/local/TensorRT-$TRT_VERSION/lib
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$TRT_LIBPATH

# Please specify your username
ARG username=howard 

ARG uid=1000
ARG gid=1000
RUN groupadd -r -f -g ${gid} $username && useradd -o -r -u ${uid} -g ${gid} -ms /bin/bash $username
RUN usermod -aG sudo $username
RUN echo "$username:123" | chpasswd
RUN rm /home/$username/.bashrc /home/$username/.profile
RUN cp ~/.bashrc /home/$username/
RUN cp ~/.profile /home/$username/

USER $username
