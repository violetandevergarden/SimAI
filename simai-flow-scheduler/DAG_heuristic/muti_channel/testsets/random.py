"""Seeded random route-conflict DAGs."""

import random

from DAG_heuristic.common.interface import TestCase
from DAG_heuristic.muti_channel.legacy_generator import random_topology_instance


def cases(samples: int = 10, seed: int = 260819) -> list[TestCase]:
    rng = random.Random(seed)
    return [
        TestCase(
            f"random_topology_{index}",
            "random",
            random_topology_instance(rng, index),
        )
        for index in range(samples)
    ]

