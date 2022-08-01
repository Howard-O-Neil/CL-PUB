import torch
import torch.nn as nn
import torch.functional as F
from torchviz import make_dot

class Net(nn.Module):
    def __init__(self, input_features, output_features) -> None:
        super(Net, self).__init__()

        self.input_features = input_features
        self.output_features = output_features
        
        self.weight1 = torch.nn.Parameter(torch.rand(size=(input_features, output_features))) 
        self.bias1 = torch.nn.Parameter(torch.rand(output_features))

        self.weight2 = torch.nn.Parameter(torch.rand(size=(output_features, output_features))) 
        self.bias2 = torch.nn.Parameter(torch.rand(output_features))

        
    def forward(self, x):
        x = torch.matmul(x, self.weight1)
        x = torch.add(x, self.bias1)
        x = torch.matmul(x, self.weight2)
        x = torch.add(x, self.bias2)

        return x

input = torch.rand(size=(1, 2))
model = Net(2, 3)

y = model(input)

make_dot(y.mean(), params=dict(model.named_parameters())).render("attached", format="png")