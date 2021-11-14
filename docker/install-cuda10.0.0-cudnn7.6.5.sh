# Install CUDA via RUN file
sh cuda_10.0.130_410.48_linux.run;
rm -rf cuda_10.0.130_410.48_linux.run;

# Install cuDNN
dpkg -i libcudnn7_7.6.5.32-1+cuda10.0_amd64.deb;
dpkg -i libcudnn7-dev_7.6.5.32-1+cuda10.0_amd64.deb;
dpkg -i libcudnn7-doc_7.6.5.32-1+cuda10.0_amd64.deb;
rm -rf libcudnn7_7.6.5.32-1+cuda10.0_amd64.deb;
rm -rf libcudnn7-dev_7.6.5.32-1+cuda10.0_amd64.deb;
rm -rf libcudnn7-doc_7.6.5.32-1+cuda10.0_amd64.deb;