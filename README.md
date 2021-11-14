# Collaborative Recommender
A collaborative recommender built with tensorflow 1x

## TensorRT
TensorRT gpu accelerated docker container
+ Run tensorflow code
+ Optimize & Deploy Tensorflow model

Container packed with
+ Ubuntu18.04
+ CUDA Toolkit
+ cuDNN
+ TensorRT
+ Tensorflow

## Supported image
+ `ubuntu-18.04-tensorrt7-tf1.15`
  + TensorRT 7.0.0.11
  + CUDA 10.0.130
  + cuDNN 7.6.5
  + tensorflow-gpu 1.15
+ `ubuntu-18.04-tensorrt8-tf2.7`
  + TensorRT 8.2.0.6
  + CUDA 11.2.2
  + cuDNN 8.2.1
  + tensorflow 2.6 (packed with GPU support)

## How to use?
Read more [here](./docker/README.md) # It works!

## Credits
TensorRT base image: https://github.com/NVIDIA/TensorRT