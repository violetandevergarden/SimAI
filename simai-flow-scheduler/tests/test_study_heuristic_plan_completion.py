"""Regression tests for the stage-4/5 completion study."""

from scripts.benchmark_dag_oracle import _Builder
from scripts.study_heuristic_plan_completion import (
    compute_only_bound,
    decouple_zero_bubble,
    pq_stratum,
    scale_instance,
    strict_series_partition,
)
from scripts.study_llm_route_windows import build_route_aware_probe
from scripts.study_multiresource_dag import MultiResourceDAG, MultiResourceInstance
from scripts.study_small_topology_sensitivity import (
    PLACEMENTS,
    four_rack_core_topology,
)


def test_strict_series_partition_uses_only_global_barriers() -> None:
    builder = _Builder("fork_join", "test", "strict series cut")
    root = builder.add("root", "compute", 1)
    left = builder.add("left", "comm", 1, (root,))
    right = builder.add("right", "comm", 1, (root,))
    join = builder.add("join", "compute", 1, (left, right))
    builder.add("tail", "comm", 1, (join,))

    result = strict_series_partition(builder.finish())

    assert result["barriers"] == ["root", "join", "tail"]
    assert result["regions"] == [["root"], ["left", "right", "join"], ["tail"]]


def test_zero_bubble_transform_is_isolated_and_removes_same_pair_edge() -> None:
    instance, info, _ = build_route_aware_probe(
        mode="zero_bubble",
        topology_path=None,
        topology=four_rack_core_topology(),
        quantum_us=25.0,
        assigned_nodes=PLACEMENTS["tp_cross"],
        ga=1,
        layers=1,
    )
    transformed, stats = decouple_zero_bubble(instance, info["task_meta"])

    assert stats["changed_w_nodes"] > 0
    assert stats["removed_b_to_w_edges"] > 0
    assert stats["added_f_to_w_edges"] > 0
    assert transformed.dag.validate() == []
    assert transformed.dag != instance.dag


def test_scaling_and_pq_classification() -> None:
    builder = _Builder("scale", "test", "profile scale")
    flow = builder.add("flow", "comm", 2)
    builder.add("compute", "compute", 4, (flow,))
    instance = MultiResourceInstance(builder.finish(), {"flow": frozenset({"link"})})
    scaled = scale_instance(instance, communication_scale=2, compute_scale=0.5)
    model = MultiResourceDAG(scaled)

    assert [task.duration for task in scaled.dag.tasks] == [4, 2]
    assert compute_only_bound(model) == 2
    assert pq_stratum(4, 2) == "balanced_P_approx_Q"
    assert pq_stratum(5, 2) == "communication_dominated"
