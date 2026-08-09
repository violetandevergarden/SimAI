"""Tests for route-aware windows extracted from effective LLM DAGs."""

from scripts.benchmark_dag_oracle import _Builder
from scripts.study_llm_route_windows import (
    build_route_aware_aicb,
    _route_decision_window,
    build_route_aware_probe,
)
from scripts.study_multiresource_dag import MultiResourceDAG, MultiResourceInstance


def _write_star_topology(path) -> None:
    lines = ["9 8 0 1 8 H100", "8"]
    lines.extend(f"{gpu} 8 200Gbps 1us 0" for gpu in range(8))
    path.write_text("\n".join(lines), encoding="utf-8")


def test_effective_probe_flows_bind_to_bfs_links_and_nics(tmp_path) -> None:
    topology = tmp_path / "star.topo"
    _write_star_topology(topology)

    instance, info, _graph = build_route_aware_probe(
        mode="1f1b",
        topology_path=topology,
        quantum_us=25.0,
    )

    assert info["tasks"] == 992
    assert info["flows"] == 560
    assert info["flow_dimensions"] == {"DP": 16, "TP": 512, "PP": 32}
    assert info["resource_model"] == "directed_links+nic_tx+nic_rx"
    assert set(instance.resources) == {
        task.task_id for task in instance.dag.tasks if task.kind == "comm"
    }
    assert all(
        any(isinstance(resource, tuple) and resource[0] == "nic_tx" for resource in values)
        for values in instance.resources.values()
    )


def test_link_only_probe_does_not_add_nic_resources(tmp_path) -> None:
    topology = tmp_path / "star.topo"
    _write_star_topology(topology)

    instance, info, _graph = build_route_aware_probe(
        mode="1f1b",
        topology_path=topology,
        quantum_us=25.0,
        include_nic_resources=False,
    )

    assert info["resource_model"] == "directed_links"
    assert all(
        not any(isinstance(resource, tuple) and isinstance(resource[0], str) for resource in values)
        for values in instance.resources.values()
    )


def test_decision_window_preserves_flow_resources_and_boundary_tail() -> None:
    builder = _Builder("full", "test", "two competing flows")
    left = builder.add("left", "comm", 3, role="PP:forward:0")
    right = builder.add("right", "comm", 1, role="DP:backward:0")
    builder.add("left_compute", "compute", 8, (left,))
    builder.add("right_compute", "compute", 2, (right,))
    instance = MultiResourceInstance(
        builder.finish(),
        {
            "left": frozenset({"shared", "left"}),
            "right": frozenset({"shared", "right"}),
        },
    )
    model = MultiResourceDAG(instance)
    seeds = [model.order.index("left"), model.order.index("right")]

    window = _route_decision_window(
        model, model.initial, seeds, 0, depth=0, boundary_cap=20,
    )

    assert set(window.resources.values()) == {
        frozenset({"shared", "left"}),
        frozenset({"shared", "right"}),
    }
    assert any(task.role == "boundary_tail" for task in window.dag.tasks)
