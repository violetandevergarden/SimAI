"""DAGs constructed to attack ordering and join-aware heuristics."""

from DAG_heuristic.common.benchmark import adversarial_benchmarks
from DAG_heuristic.common.interface import TestCase
from DAG_heuristic.single_channel.complex_chain.legacy_generator import (
    last_blocker_overboost_counterexample,
)


def cases() -> list[TestCase]:
    dags = [*adversarial_benchmarks(), last_blocker_overboost_counterexample()]
    return [TestCase(dag.name, "adversarial", dag) for dag in dags]

