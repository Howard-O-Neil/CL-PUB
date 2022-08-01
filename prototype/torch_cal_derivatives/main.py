import torch
import torch.nn as nn
import torch.nn.functional as F
from torchviz import make_dot

def _1_val_derivative():
    # f = 6 * x ** 2 + 2 * x + 4
    x = torch.tensor(3.0, requires_grad=True)
    y = torch.sum(
        torch.stack([
            torch.multiply(6.0, torch.square(x)), 
            torch.multiply(2.0, x), 
            torch.tensor(4.0, requires_grad=False)]),
        dim=0,
    )

    y.backward()

    print(f"x grad = {x.grad}")

def vals_derivative():
    u = torch.tensor(3., requires_grad=True)
    v = torch.tensor(4., requires_grad=True)

    # f = u**3 + v**2 + 4*u*v
    f = torch.sum(
        torch.stack([
            torch.pow(u, 3.), torch.pow(v, 2.),
            torch.multiply(torch.multiply(u, v), 4.)
        ])
    )

    f.backward()

    print(f"u grad = {u.grad}")
    print(f"v grad = {v.grad}")
    
_1_val_derivative()
print("=====")
vals_derivative()