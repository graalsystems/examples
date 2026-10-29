# tensorflow_examples

Trains a small convolutional network on MNIST-shaped images with TensorFlow and Keras, distributed
over several pods with `MultiWorkerMirroredStrategy`.

- **Job type:** `tensorflow` (specialised type: available only if your installation configured its
  runtime image; the platform's image is `python-ml`, Python 3.12 with TensorFlow 2.21 on CPU)
- **Entry point:** the module `tensorflow_mnist` (`options.module`), run with `python3 -m` in each
  pod; the job's `parameters` are its arguments
- **Distribution:** graal starts `number_replicas` pods (default 2, each of
  `replica_instance_type`) and sets `TF_CONFIG` in each; worker 0 is the chief. No operator or
  custom resource is involved. Without `TF_CONFIG` (a local run), the script trains on the local
  devices.
- **GPU:** with `"gpu": true`, the pods need an instance type with at least one GPU and run the
  `tensorflow-gpu` image (TensorFlow 2.21, CUDA 12). Because of the NVIDIA licence, that image is
  distributed only through a private registry: GPU runs need an installation that has it, otherwise
  the API answers `501 feature_disabled`.
- **Data:** `--dataset synthetic` (default) generates images in code and needs no network.
  `--dataset mnist` downloads MNIST with `keras.datasets`; a run has no outbound access by default,
  so this needs a project data connection of type `http` for `https://storage.googleapis.com`,
  listed in `options.connections`.

## Build

Nothing to build: the job runs `tensorflow_mnist.py` as uploaded, with the TensorFlow of the image.
To try it locally (with a Python version that TensorFlow 2.21 supports):

```sh
pip install -r requirements.txt
python tensorflow_mnist.py --epochs 1 --steps-per-epoch 10
```

## Run with graalctl

```sh
SCRIPT_KEY=$(graalctl libraries upload tensorflow_mnist.py)

sed -e "s/<IDENTITY_ID>/$IDENTITY_ID/" \
    -e "s/<LIBRARY_KEY>/$SCRIPT_KEY/" job.json > job.local.json

JOB_ID=$(graalctl jobs create --project "$PROJECT_ID" -f job.local.json -o id)
graalctl jobs run "$JOB_ID" --follow
```

Each worker logs its index and the cluster it joined, then the training progress.

If the API answers `501 feature_disabled`, the `tensorflow` type (or its GPU variant) is not
enabled on your installation. The options a specialised type accepts depend on the runtime image
configured there: check the installation's API reference (`/api/v1/docs`) and adjust
[`job.json`](job.json).
