from __future__ import annotations

import random

from DAG_heuristic.muti_channel.legacy_generator import random_topology_instance
from DAG_heuristic.muti_channel.algorithms import (
    NonPreemptiveMultiResourceDAG,
    ResourceAction,
    exact_oracle,
    manual_route_topology_instances,
    schedule_greedy,
    schedule_rollout,
    topology_motifs,
)


def test_disjoint_routes_run_concurrently_without_preemption() -> None:
    instance = topology_motifs()[0]
    result = exact_oracle(instance)

    assert result.makespan == 7
    assert result.actions[0].starts == ("left", "right")
    comms = [interval for interval in result.intervals if interval.kind == "comm"]
    assert {(item.task_id, item.start, item.end) for item in comms} == {
        ("left", 0, 4),
        ("right", 0, 4),
    }


def test_shared_route_serializes_whole_flows() -> None:
    result = exact_oracle(topology_motifs()[1])
    comms = sorted(
        (interval for interval in result.intervals if interval.kind == "comm"),
        key=lambda item: item.start,
    )

    assert result.makespan == 11
    assert comms[0].end <= comms[1].start


def test_nonmaximal_start_and_wait_can_beat_every_maximal_start() -> None:
    instance = topology_motifs()[2]
    optional = exact_oracle(instance, mode="optional_idle")
    work_conserving = exact_oracle(instance, mode="work_conserving")

    assert optional.makespan == 8
    assert work_conserving.makespan == 12
    assert optional.actions[0].starts == ("a",)
    assert ("a",) not in NonPreemptiveMultiResourceDAG(
        instance
    ).start_subsets(
        NonPreemptiveMultiResourceDAG(instance).initial_state(),
        maximal_only=True,
    )


def test_active_flow_keeps_route_reserved_at_intermediate_event() -> None:
    instance = topology_motifs()[3]
    model = NonPreemptiveMultiResourceDAG(instance)
    state = model.initial_state()
    state = model.step(state, ResourceAction.start(("a", "b"))).after

    assert state.time == 1
    assert model.active_flows(state) == ("a",)
    assert model.ready_flows(state) == ("c",)
    assert not model.compatible(state, ("c",))
    assert model.legal_actions(state, "optional_idle") == (ResourceAction.wait(),)


def test_optional_set_rollout_repairs_nonmaximal_motif() -> None:
    instance = topology_motifs()[2]

    assert schedule_greedy(instance).makespan == 12
    assert schedule_rollout(
        instance, top_k=2, optional_actions=False
    ).makespan == 12
    assert schedule_rollout(
        instance, top_k=2, optional_actions=True
    ).makespan == 8


def test_manual_topologies_use_real_bfs_route_conflicts() -> None:
    single, two_rack, four_rack = manual_route_topology_instances()

    assert not (single.resources["f1"] & single.resources["f2"])
    assert (8, 9) in two_rack.resources["f1"] & two_rack.resources["f2"]
    assert (8, 12) in four_rack.resources["f1"] & four_rack.resources["f2"]
    assert not (four_rack.resources["f1"] & four_rack.resources["f3"])


def test_enhanced_schedules_keep_dynamic_incumbent_and_exact_lower_bound() -> None:
    rng = random.Random(260819)
    for index in range(5):
        instance = random_topology_instance(rng, index)
        optimum = exact_oracle(instance, max_states=300_000).makespan
        dynamic = schedule_greedy(instance).makespan
        for result in (
            schedule_greedy(instance, "resource_tail"),
            schedule_greedy(instance, "bottleneck_first"),
            schedule_rollout(instance, top_k=2, optional_actions=False),
            schedule_rollout(instance, top_k=2, optional_actions=True),
        ):
            assert result.makespan >= optimum
        assert schedule_rollout(
            instance, top_k=2, optional_actions=True
        ).makespan <= dynamic
