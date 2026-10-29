"""Train a small convolutional network on Fashion-MNIST with PyTorch.

Runs on a GPU when the instance type provides one, on CPU otherwise. When started by
`torchrun` (WORLD_SIZE > 1), training is distributed with DistributedDataParallel.
"""

import argparse
import os

import torch
import torch.distributed as dist
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader
from torch.utils.data.distributed import DistributedSampler
from torchvision import datasets, transforms


class Net(nn.Module):
    def __init__(self) -> None:
        super().__init__()
        self.conv1 = nn.Conv2d(1, 20, 5, 1)
        self.conv2 = nn.Conv2d(20, 50, 5, 1)
        self.fc1 = nn.Linear(4 * 4 * 50, 500)
        self.fc2 = nn.Linear(500, 10)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = F.max_pool2d(F.relu(self.conv1(x)), 2, 2)
        x = F.max_pool2d(F.relu(self.conv2(x)), 2, 2)
        x = torch.flatten(x, 1)
        x = F.relu(self.fc1(x))
        return F.log_softmax(self.fc2(x), dim=1)


def train(model, device, loader, optimizer, epoch, log_interval) -> None:
    model.train()
    for batch_idx, (data, target) in enumerate(loader):
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        loss = F.nll_loss(model(data), target)
        loss.backward()
        optimizer.step()
        if batch_idx % log_interval == 0:
            print(f"epoch {epoch} [{batch_idx * len(data)}/{len(loader.dataset)}] loss={loss.item():.4f}")


def evaluate(model, device, loader) -> float:
    model.eval()
    correct = 0
    with torch.no_grad():
        for data, target in loader:
            data, target = data.to(device), target.to(device)
            correct += (model(data).argmax(dim=1) == target).sum().item()
    return correct / len(loader.dataset)


def main() -> None:
    parser = argparse.ArgumentParser(description="PyTorch Fashion-MNIST example")
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--test-batch-size", type=int, default=1000)
    parser.add_argument("--epochs", type=int, default=1)
    parser.add_argument("--lr", type=float, default=0.01)
    parser.add_argument("--momentum", type=float, default=0.5)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--log-interval", type=int, default=100)
    parser.add_argument("--data-dir", default=os.getenv("DATA_DIR", "/tmp/data"))
    parser.add_argument("--save-model", metavar="PATH", help="save the trained weights to PATH")
    args = parser.parse_args()

    torch.manual_seed(args.seed)
    use_cuda = torch.cuda.is_available()
    distributed = int(os.getenv("WORLD_SIZE", "1")) > 1

    if distributed:
        dist.init_process_group(backend="nccl" if use_cuda else "gloo")
        local_rank = int(os.getenv("LOCAL_RANK", "0"))
        if use_cuda:
            torch.cuda.set_device(local_rank)
        device = torch.device("cuda", local_rank) if use_cuda else torch.device("cpu")
    else:
        device = torch.device("cuda" if use_cuda else "cpu")
    print(f"Using {device}, distributed={distributed}")

    transform = transforms.Compose([transforms.ToTensor(), transforms.Normalize((0.2860,), (0.3530,))])
    train_set = datasets.FashionMNIST(args.data_dir, train=True, download=True, transform=transform)
    test_set = datasets.FashionMNIST(args.data_dir, train=False, download=True, transform=transform)

    sampler = DistributedSampler(train_set) if distributed else None
    loader_kwargs = {"num_workers": 1, "pin_memory": True} if use_cuda else {}
    train_loader = DataLoader(
        train_set, batch_size=args.batch_size, shuffle=sampler is None, sampler=sampler, **loader_kwargs
    )
    test_loader = DataLoader(test_set, batch_size=args.test_batch_size, **loader_kwargs)

    model = Net().to(device)
    if distributed:
        model = nn.parallel.DistributedDataParallel(model, device_ids=[device.index] if use_cuda else None)
    optimizer = torch.optim.SGD(model.parameters(), lr=args.lr, momentum=args.momentum)

    for epoch in range(1, args.epochs + 1):
        if sampler is not None:
            sampler.set_epoch(epoch)
        train(model, device, train_loader, optimizer, epoch, args.log_interval)
        print(f"epoch {epoch} accuracy={evaluate(model, device, test_loader):.4f}")

    if args.save_model and (not distributed or dist.get_rank() == 0):
        module = model.module if distributed else model
        torch.save(module.state_dict(), args.save_model)
        print(f"Saved weights to {args.save_model}")

    if distributed:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
