# pytorch_examples

Trains a small convolutional network on Fashion-MNIST with PyTorch and prints the test accuracy
after each epoch. Uses a GPU when the instance type provides one, the CPU otherwise.

- **Job type:** `python` (core type, `python` image, Python 3.11)
- **Entry point:** `pytorch_mnist.py`
- **Dependencies:** `requirements.txt` (`torch`, `torchvision`), installed at start-up
- **Network:** the dataset is downloaded from the internet into `DATA_DIR` (default `/tmp/data`)
  on first use. Without outbound access, put the dataset in that directory instead.

## Build

Nothing to build. To try it locally:
`pip install -r requirements.txt && python pytorch_mnist.py --epochs 1`.

`torch` is a large download: for jobs that run often, consider a runtime image that already
contains it (see [Runtime images](https://graal.systems/en/docs/runtime-images)).

## Run with graalctl

```sh
SCRIPT_KEY=$(graalctl libraries upload pytorch_mnist.py)
REQ_KEY=$(graalctl libraries upload requirements.txt)

sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" \
    -e "s/<LIBRARY_KEY>/$SCRIPT_KEY/" \
    -e "s/<REQUIREMENTS_KEY>/$REQ_KEY/" job.json > job.local.json

JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

To train on a GPU, set `options.instance_type` in [`job.json`](job.json) to a GPU instance type
from `graalctl instance-types list`.
