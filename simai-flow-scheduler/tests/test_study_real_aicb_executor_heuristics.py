"""Tests for isolated real-executor heuristic allocation."""

from dataclasses import dataclass

from scripts.study_real_aicb_executor_heuristics import (
    ResidualPriorityAllocator,
)
from src.static_analysis.passes.topology_loader import Link, NetworkTopology


@dataclass
class _Flow:
    task_id: int
    path: list[int]


class _Context:
    def __init__(self, priorities):
        self.priorities = priorities

    def priority(self, task_id, policy, topology):
        return (self.priorities[task_id],)


def _topology() -> NetworkTopology:
    topology = NetworkTopology()
    for src, dst in ((0, 10), (10, 1), (10, 2), (3, 11), (11, 4)):
        topology.add_link(Link(src, dst, 100.0, 1.0, 0.0))
    return topology


def test_higher_priority_pauses_only_conflicting_flow() -> None:
    flows = [
        _Flow(1, [0, 10, 1]),
        _Flow(2, [0, 10, 2]),
        _Flow(3, [3, 11, 4]),
    ]
    allocator = ResidualPriorityAllocator(_Context({1: 3, 2: 2, 3: 1}), "test")

    result = allocator.allocate(flows, _topology())

    assert result[1] == 100.0
    assert result[2] == 0.0
    assert result[3] == 100.0
    assert allocator.telemetry.conflict_calls == 1


def test_equal_priority_uses_progressive_fair_share() -> None:
    flows = [_Flow(1, [0, 10, 1]), _Flow(2, [0, 10, 2])]
    allocator = ResidualPriorityAllocator(_Context({1: 1, 2: 1}), "test")

    result = allocator.allocate(flows, _topology())

    assert result == {1: 50.0, 2: 50.0}
