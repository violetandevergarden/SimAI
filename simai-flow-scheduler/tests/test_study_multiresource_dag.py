"""Regression tests for the small topology-conflict scheduling model."""

import random

from scripts.benchmark_dag_oracle import _Builder
from scripts.study_multiresource_dag import (
    MultiResourceDAG,
    MultiResourceInstance,
    exact_multiresource_oracle,
    multi_resource_lower_bounds,
    random_topology_instance,
    rollout_multiresource,
    route_resource_sets,
    schedule_multiresource,
    topology_motifs,
)
from src.static_analysis.passes.routing import BfsStrategy
from src.static_analysis.passes.topology_loader import Link, NetworkTopology
from src.workload_format.schema import Meta, P2PWorkload, Task, TaskType


def test_disjoint_routes_progress_concurrently() -> None:
    instance = topology_motifs()[0]
    oracle = exact_multiresource_oracle(instance)

    assert oracle.makespan == 7
    assert oracle.decisions[:4] == [("left", "right")] * 4
    assert schedule_multiresource(instance).makespan == oracle.makespan


def test_shared_route_link_serializes_flows() -> None:
    instance = topology_motifs()[1]
    model = MultiResourceDAG(instance)
    left = model.order.index("left")
    right = model.order.index("right")

    assert not model.compatible((left, right))
    assert exact_multiresource_oracle(instance).makespan == 11


def test_partial_overlap_selects_a_maximal_compatible_set() -> None:
    instance = topology_motifs()[2]
    model = MultiResourceDAG(instance)
    ready = model.ready(model.initial)
    named = {
        tuple(model.order[index] for index in selected)
        for selected in model.maximal_compatible_sets(ready)
    }

    assert named == {("dp", "tp"), ("pp", "tp")}


def test_per_resource_load_is_a_valid_lower_bound() -> None:
    for instance in topology_motifs():
        model = MultiResourceDAG(instance)
        bounds = multi_resource_lower_bounds(model)
        optimum = exact_multiresource_oracle(instance).makespan

        assert bounds["combined"] <= optimum
        assert bounds["combined"] == max(
            bounds["critical_path"], bounds["max_resource_load"],
            bounds["resource_window"],
        )


def test_resource_window_bound_captures_late_shared_link_demand() -> None:
    builder = _Builder("window", "test", "late shared-resource traffic")
    release = builder.add("release", "compute", 4)
    first = builder.add("first", "comm", 3, (release,))
    second = builder.add("second", "comm", 3, (release,))
    builder.add("tail_first", "compute", 4, (first,))
    builder.add("tail_second", "compute", 4, (second,))
    instance = MultiResourceInstance(
        builder.finish(),
        {"first": frozenset({"link"}), "second": frozenset({"link"})},
    )
    bounds = multi_resource_lower_bounds(MultiResourceDAG(instance))
    optimum = exact_multiresource_oracle(instance).makespan

    assert bounds["resource_window"] == 14
    assert bounds["resource_window"] > bounds["max_resource_load"]
    assert bounds["combined"] <= optimum


def test_route_adapter_preserves_directed_link_overlap() -> None:
    topology = NetworkTopology()
    for src, dst in ((0, 10), (10, 0), (1, 10), (10, 1), (2, 10), (10, 2)):
        topology.add_link(Link(src, dst, 100.0, 1.0, 0.0))
    workload = P2PWorkload(
        version="1.0",
        meta=Meta(num_jobs=1, num_nodes=3),
        tasks=[
            Task(1, 0, TaskType.FLOW, src=0, dst=1, size_bytes=100),
            Task(2, 0, TaskType.FLOW, src=0, dst=2, size_bytes=100),
        ],
    )
    routes = BfsStrategy().compute_routes(workload, topology)
    resources = route_resource_sets(workload, routes)

    assert resources["1"] == frozenset({(0, 10), (10, 1)})
    assert resources["2"] == frozenset({(0, 10), (10, 2)})
    assert resources["1"] & resources["2"] == {(0, 10)}


def test_route_adapter_can_add_shared_nic_injection_resources() -> None:
    topology = NetworkTopology()
    for src, dst in ((0, 10), (10, 0), (10, 1), (1, 10), (0, 11), (11, 0), (11, 2), (2, 11)):
        topology.add_link(Link(src, dst, 100.0, 1.0, 0.0))
    workload = P2PWorkload(
        version="1.0",
        meta=Meta(num_jobs=1, num_nodes=3),
        tasks=[
            Task(1, 0, TaskType.FLOW, src=0, dst=1, size_bytes=100),
            Task(2, 0, TaskType.FLOW, src=0, dst=2, size_bytes=100),
        ],
    )
    routes = BfsStrategy().compute_routes(workload, topology)
    resources = route_resource_sets(
        workload,
        routes,
        include_source_nic=True,
        include_destination_nic=True,
    )

    assert ("nic_tx", 0) in resources["1"] & resources["2"]
    assert ("nic_rx", 1) in resources["1"]
    assert ("nic_rx", 2) in resources["2"]


def test_set_rollout_keeps_the_packed_dynamic_tail_incumbent() -> None:
    rng = random.Random(260819)
    for index in range(10):
        instance = random_topology_instance(rng, index)
        baseline = schedule_multiresource(instance, "dynamic_tail")
        rollout = rollout_multiresource(instance, top_k=2)

        assert rollout.makespan <= baseline.makespan


def test_heuristics_never_beat_exact_multiresource_oracle() -> None:
    rng = random.Random(19)
    for index in range(5):
        instance = random_topology_instance(rng, index)
        optimum = exact_multiresource_oracle(instance, max_states=300_000).makespan
        for result in (
            schedule_multiresource(instance, "dynamic_tail"),
            schedule_multiresource(instance, "resource_tail"),
            schedule_multiresource(instance, "bottleneck_first"),
            rollout_multiresource(instance, top_k=2),
        ):
            assert result.makespan >= optimum
