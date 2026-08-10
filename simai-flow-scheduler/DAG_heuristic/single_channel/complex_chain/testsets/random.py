"""Seeded random fork/join DAGs."""

import random

from DAG_heuristic.common.interface import TestCase
from DAG_heuristic.single_channel.complex_chain.legacy_generator import random_join_dag


def cases(samples: int = 10, seed: int = 260817) -> list[TestCase]:
    rng = random.Random(seed)
    return [
        TestCase(f"random_join_{index}", "random", random_join_dag(rng, index))
        for index in range(samples)
    ]

