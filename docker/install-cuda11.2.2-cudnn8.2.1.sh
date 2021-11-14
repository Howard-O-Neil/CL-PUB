# Install CUDA via RUN file
sh cuda_11.2.2_460.32.03_linux.run;
rm -rf cuda_11.2.2_460.32.03_linux.run;

# Install cuDNN
dpkg -i libcudnn8_8.2.1.32-1+cuda11.3_amd64.deb;
dpkg -i libcudnn8-dev_8.2.1.32-1+cuda11.3_amd64.deb;
dpkg -i libcudnn8-samples_8.2.1.32-1+cuda11.3_amd64.deb;
rm -rf libcudnn8_8.2.1.32-1+cuda11.3_amd64.deb;
rm -rf libcudnn8-dev_8.2.1.32-1+cuda11.3_amd64.deb;
rm -rf libcudnn8-samples_8.2.1.32-1+cuda11.3_amd64.deb;