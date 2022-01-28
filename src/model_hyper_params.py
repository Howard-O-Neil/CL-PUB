learning_rate = 0.00065
decay_rate = 0.00025 # approximately around 45 epochs
lambda_val = 0.000065
training_epochs = 5000 # for early stopping
batch_size = 30

num_dense_layers = 2
num_dense_neurons = [4096, 4096]
drop_rates = [0.15, 0.25, 0.30, 0.35, 0.35, 0.4, 0.4] # 7  layer of drop rate

num_conv_layers = 13
conv_stride_steps = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
filter_wnd_w = [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3]
filter_wnd_h = [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3]
num_filters = [64, 64, 128, 128, 256, 256, 256, 512, 512, 512, 512, 512, 512]
conv_padding_strategies = ['SAME', 'SAME', 'SAME', 'SAME', 'SAME', 'SAME', 'SAME', 'SAME', 'SAME', 'SAME', 'SAME', 'SAME', 'SAME']

maxpool_wnd_w = [2, 2, 2, 2, 2]
maxpool_wnd_h = [2, 2, 2, 2, 2]
maxpool_stride_steps = [2, 2, 2, 2, 2]
maxpool_padding_strategies = ['SAME', 'SAME', 'SAME', 'SAME', 'SAME']