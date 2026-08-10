"""Small instances routed through real BFS and topology resource adapters."""

from DAG_heuristic.common.interface import TestCase
from DAG_heuristic.muti_channel.algorithms import manual_route_topology_instances


def cases() -> list[TestCase]:
    return [
        TestCase(instance.dag.name, "real_derived", instance)
        for instance in manual_route_topology_instances()
    ]

