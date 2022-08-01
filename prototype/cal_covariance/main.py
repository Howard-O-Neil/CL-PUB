import torch
import torch.nn as nn
import torch.nn.functional as F
from torchviz import make_dot

x = torch.rand(7, 3)
print(torch.cov(x).numpy().shape)