"""Train a small convolutional network on MNIST-shaped images with TensorFlow and Keras.

In a graal `tensorflow` job, the platform starts `number_replicas` pods and sets TF_CONFIG in each
of them: the script then trains with MultiWorkerMirroredStrategy (worker 0 is the chief). Without
TF_CONFIG (a local run), it trains on the local devices.

    python tensorflow_mnist.py [--dataset synthetic|mnist] [--epochs 3] [--steps-per-epoch 50]

`synthetic` (the default) generates random images in code and needs no network; `mnist` downloads
the MNIST dataset with keras.datasets, which needs outbound access to storage.googleapis.com.
"""

import argparse
import json
import os

import numpy as np
import tensorflow as tf

NUM_CLASSES = 10
IMAGE_SHAPE = (28, 28, 1)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dataset", choices=["synthetic", "mnist"],
                        default=os.environ.get("DATASET", "synthetic"))
    parser.add_argument("--epochs", type=int, default=int(os.environ.get("EPOCHS", "3")))
    parser.add_argument("--steps-per-epoch", type=int, default=int(os.environ.get("STEPS_PER_EPOCH", "50")))
    parser.add_argument("--batch-size-per-replica", type=int, default=64)
    return parser.parse_args()


def make_strategy():
    """MultiWorkerMirroredStrategy when TF_CONFIG describes a cluster, the default strategy otherwise."""
    tf_config = json.loads(os.environ.get("TF_CONFIG", "{}"))
    workers = tf_config.get("cluster", {}).get("worker", [])
    if len(workers) > 1:
        task = tf_config.get("task", {})
        print(f"Worker {task.get('index')} of {len(workers)}: {workers}", flush=True)
        return tf.distribute.MultiWorkerMirroredStrategy(), task.get("index", 0) == 0
    return tf.distribute.get_strategy(), True


def load_data(name):
    if name == "mnist":
        (x_train, y_train), _ = tf.keras.datasets.mnist.load_data()
        x_train = x_train.astype("float32") / 255.0
    else:
        rng = np.random.default_rng(42)
        y_train = rng.integers(0, NUM_CLASSES, size=6000)
        # Images whose mean intensity depends on the label, so the network has something to learn.
        x_train = rng.random((6000, 28, 28), dtype=np.float32) * 0.5 + (y_train[:, None, None] / 20.0)
        x_train = x_train.astype("float32")
    return x_train[..., np.newaxis], y_train.astype("int64")


def build_model():
    model = tf.keras.Sequential([
        tf.keras.Input(shape=IMAGE_SHAPE),
        tf.keras.layers.Conv2D(32, 3, activation="relu"),
        tf.keras.layers.MaxPooling2D(),
        tf.keras.layers.Conv2D(64, 3, activation="relu"),
        tf.keras.layers.MaxPooling2D(),
        tf.keras.layers.Flatten(),
        tf.keras.layers.Dense(64, activation="relu"),
        tf.keras.layers.Dense(NUM_CLASSES, activation="softmax"),
    ])
    model.compile(optimizer="adam", loss="sparse_categorical_crossentropy", metrics=["accuracy"])
    return model


def main():
    args = parse_args()
    print(f"TensorFlow {tf.__version__}, GPUs: {len(tf.config.list_physical_devices('GPU'))}", flush=True)

    strategy, is_chief = make_strategy()
    batch_size = args.batch_size_per_replica * strategy.num_replicas_in_sync

    x_train, y_train = load_data(args.dataset)
    dataset = (tf.data.Dataset.from_tensor_slices((x_train, y_train))
               .shuffle(10_000, seed=42)
               .batch(batch_size)
               .repeat())
    options = tf.data.Options()
    options.experimental_distribute.auto_shard_policy = tf.data.experimental.AutoShardPolicy.DATA
    dataset = dataset.with_options(options)

    with strategy.scope():
        model = build_model()
    if is_chief:
        model.summary()

    history = model.fit(dataset, epochs=args.epochs, steps_per_epoch=args.steps_per_epoch, verbose=2)
    print(f"Final accuracy: {history.history['accuracy'][-1]:.4f}", flush=True)


if __name__ == "__main__":
    main()
