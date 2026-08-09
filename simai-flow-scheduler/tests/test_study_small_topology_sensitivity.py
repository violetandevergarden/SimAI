"""Tests for the hand-built topology sensitivity study."""

from src.static_analysis.passes.routing.bfs import bfs_shortest_path

from scripts.study_small_topology_sensitivity import (
    PLACEMENTS,
    four_rack_core_topology,
    single_switch_topology,
    two_rack_topology,
)


def test_single_switch_has_two_hop_gpu_routes() -> None:
    topology = single_switch_topology()

    assert bfs_shortest_path(topology, 0, 7) == [0, 8, 7]
    assert len(topology.links) == 16


def test_two_rack_routes_share_the_declared_uplink() -> None:
    topology = two_rack_topology()
    left = bfs_shortest_path(topology, 0, 4)
    right = bfs_shortest_path(topology, 1, 5)

    assert left == [0, 8, 9, 4]
    assert right == [1, 8, 9, 5]
    assert (8, 9) in set(zip(left, left[1:])) & set(zip(right, right[1:]))


def test_four_rack_routes_share_access_to_core_links() -> None:
    topology = four_rack_core_topology()
    first = bfs_shortest_path(topology, 0, 4)
    second = bfs_shortest_path(topology, 1, 6)

    assert first == [0, 8, 12, 10, 4]
    assert second == [1, 8, 12, 11, 6]
    assert (8, 12) in set(zip(first, first[1:])) & set(zip(second, second[1:]))


def test_all_manual_placements_are_permutations_of_eight_gpus() -> None:
    for placement in PLACEMENTS.values():
        assert sorted(placement) == list(range(8))
