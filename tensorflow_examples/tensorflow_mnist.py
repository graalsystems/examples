"""Train a small convolutional network on MNIST-shaped images with TensorFlow.

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
    return tf.keras.Sequential([
        tf.keras.Input(shape=IMAGE_SHAPE),
        tf.keras.layers.Conv2D(32, 3, activation="relu"),
        tf.keras.layers.MaxPooling2D(),
        tf.keras.layers.Conv2D(64, 3, activation="relu"),
        tf.keras.layers.MaxPooling2D(),
        tf.keras.layers.Flatten(),
        tf.keras.layers.Dense(64, activation="relu"),
        tf.keras.layers.Dense(NUM_CLASSES),
    ])


def main():
    args = parse_args()
    print(f"TensorFlow {tf.__version__}, GPUs: {len(tf.config.list_physical_devices('GPU'))}", flush=True)

    strategy, is_chief = make_strategy()
    global_batch_size = args.batch_size_per_replica * strategy.num_replicas_in_sync

    x_train, y_train = load_data(args.dataset)
    dataset = (tf.data.Dataset.from_tensor_slices((x_train, y_train))
               .shuffle(10_000, seed=42)
               .repeat()
               .batch(global_batch_size, drop_remainder=True))
    options = tf.data.Options()
    options.experimental_distribute.auto_shard_policy = tf.data.experimental.AutoShardPolicy.DATA
    dataset = strategy.experimental_distribute_dataset(dataset.with_options(options))

    # A custom training loop: each replica computes its gradients, the strategy sums them across
    # replicas and workers (all-reduce) before every update, so all copies of the model stay equal.
    with strategy.scope():
        model = build_model()
        optimizer = tf.keras.optimizers.Adam()
        loss_fn = tf.keras.losses.SparseCategoricalCrossentropy(
            from_logits=True, reduction=tf.keras.losses.Reduction.NONE)
    if is_chief:
        model.summary()

    def step_fn(images, labels):
        with tf.GradientTape() as tape:
            logits = model(images, training=True)
            loss = tf.nn.compute_average_loss(loss_fn(labels, logits), global_batch_size=global_batch_size)
        gradients = tape.gradient(loss, model.trainable_variables)
        optimizer.apply_gradients(zip(gradients, model.trainable_variables))
        correct = tf.reduce_sum(tf.cast(tf.equal(tf.argmax(logits, axis=1), labels), tf.float32))
        return loss, correct

    @tf.function
    def train_step(iterator):
        images, labels = next(iterator)
        loss, correct = strategy.run(step_fn, args=(images, labels))
        return (strategy.reduce(tf.distribute.ReduceOp.SUM, loss, axis=None),
                strategy.reduce(tf.distribute.ReduceOp.SUM, correct, axis=None))

    iterator = iter(dataset)
    for epoch in range(1, args.epochs + 1):
        total_loss, total_correct = 0.0, 0.0
        for _ in range(args.steps_per_epoch):
            loss, correct = train_step(iterator)
            total_loss += float(loss)
            total_correct += float(correct)
        accuracy = total_correct / (args.steps_per_epoch * global_batch_size)
        print(f"Epoch {epoch}/{args.epochs}: loss {total_loss / args.steps_per_epoch:.4f}, "
              f"accuracy {accuracy:.4f}", flush=True)


if __name__ == "__main__":
    main()
