"""Small route-reservation and non-maximal-start counterexamples."""

from DAG_heuristic.common.interface import TestCase
from DAG_heuristic.muti_channel.algorithms import topology_motifs


def cases() -> list[TestCase]:
    return [
        TestCase(instance.dag.name, "adversarial", instance)
        for instance in topology_motifs()
    ]

