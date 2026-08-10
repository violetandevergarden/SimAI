"""Seeded random parallel-chain cases."""

from __future__ import annotations

import random

from DAG_heuristic.common.interface import TestCase
from DAG_heuristic.single_channel.parallel_chain.algorithms import ParallelChain
from DAG_heuristic.single_channel.parallel_chain.legacy_generator import random_instance


def cases(samples: int = 10, seed: int = 260813) -> list[TestCase]:
    rng = random.Random(seed)
    return [
        TestCase(
            f"random_chain_{index}",
            "random",
            tuple(ParallelChain.from_legacy(item) for item in random_instance(rng)),
        )
        for index in range(samples)
    ]

