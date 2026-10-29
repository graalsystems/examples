"""Ray examples: parallel tasks, an actor and a Ray Data pipeline.

`ray.init()` connects to the cluster given by RAY_ADDRESS; without it, Ray starts a local
instance on the run's machine.
"""

import random

import numpy as np
import ray


@ray.remote
def count_inside(samples: int, seed: int) -> int:
    """Count random points of the unit square that fall inside the quarter circle."""
    rng = random.Random(seed)
    return sum(1 for _ in range(samples) if rng.random() ** 2 + rng.random() ** 2 <= 1.0)


@ray.remote
class Accumulator:
    """An actor keeps state between calls."""

    def __init__(self) -> None:
        self.total = 0

    def add(self, value: int) -> int:
        self.total += value
        return self.total


def tasks_example(tasks: int = 8, samples: int = 500_000) -> None:
    futures = [count_inside.remote(samples, seed) for seed in range(tasks)]
    inside = sum(ray.get(futures))
    print(f"pi ~ {4 * inside / (tasks * samples):.5f} ({tasks} tasks)")

    accumulator = Accumulator.remote()
    for value in range(1, 11):
        accumulator.add.remote(value)
    print(f"Actor total: {ray.get(accumulator.add.remote(0))}")


def add_square(batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    batch["square"] = batch["id"] ** 2
    return batch


def keep_multiples_of_three(batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    mask = batch["id"] % 3 == 0
    return {column: values[mask] for column, values in batch.items()}


def data_example(rows: int = 100_000) -> None:
    ds = (
        ray.data.range(rows)
        .map_batches(add_square, batch_format="numpy")
        .map_batches(keep_multiples_of_three, batch_format="numpy")
    )
    print(f"Rows kept: {ds.count()}, sum of squares: {ds.sum('square')}")
    print(ds.take(3))


def main() -> None:
    ray.init()
    print(f"Cluster resources: {ray.cluster_resources()}")
    tasks_example()
    data_example()
    ray.shutdown()


if __name__ == "__main__":
    main()
