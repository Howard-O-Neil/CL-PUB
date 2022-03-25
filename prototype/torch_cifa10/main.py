DATA_ROOT = "/home/howard/project/collaborative-recommender/basic-ML-Tensorflow/data"
IMAGE_ROOT = "/home/howard/project/collaborative-recommender/basic-ML-Tensorflow/image"

import os
import torch
import torchvision
import torchvision.transforms as transforms

transform = transforms.Compose(
    [transforms.ToTensor(),
    transforms.Normalize(mean=(0.5, 0.5, 0.5), std=(0.5, 0.5, 0.5))])

batch_size = 4

trainset = torchvision.datasets.CIFAR10(root=DATA_ROOT, train=True,
                                        download=True, transform=transform)
trainloader = torch.utils.data.DataLoader(trainset, batch_size=batch_size,
                                          shuffle=True, num_workers=2)

testset = torchvision.datasets.CIFAR10(root=DATA_ROOT, train=False,
                                       download=True, transform=transform)
testloader = torch.utils.data.DataLoader(testset, batch_size=batch_size,
                                         shuffle=False, num_workers=2)

classes = ('plane', 'car', 'bird', 'cat',
           'deer', 'dog', 'frog', 'horse', 'ship', 'truck')


import matplotlib.pyplot as plt
import numpy as np

def imshow(img):
    img = img / 2 + 0.5     # unnormalize
    npimg = img.numpy()
    plt.imshow(np.transpose(npimg, (1, 2, 0)))
    plt.savefig(os.path.join(IMAGE_ROOT, "torch_cifar10_rand_img.png"))

# get some random training images
dataiter = iter(trainloader)
images, labels = dataiter.next()

print(images.numpy().shape)
# show images
imshow(torchvision.utils.make_grid(images))
# print labels
print([labels[j] for j in range(batch_size)])
print(' '.join(f'{classes[labels[j]]:5s}' for j in range(batch_size)))

from modules.model import Net
import torch.nn as nn
import torch.functional as F
import torch.optim as optim

net = Net()

criterion = nn.CrossEntropyLoss()
optimizer = optim.SGD(net.parameters(), lr=0.001, momentum=0.9)

for epoch in range(2):
    
    running_loss = 0.
    for i, data in enumerate(trainloader, 0):
        inputs, labels = data

        optimizer.zero_grad()

        outputs = net(inputs)
        loss = criterion(outputs, labels)
        loss.backward()
        optimizer.step()

        running_loss += loss.item()
        if i % 2000 == 1999:
            print(f'[{epoch + 1}, {i + 1:5d}] loss: {running_loss / 2000:.3f}')

    #     break
    # break

print("Finished Training")