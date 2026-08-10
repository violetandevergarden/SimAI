"""Regression checks for the stage-0 effective-DAG audit."""

from pathlib import Path

from DAG_heuristic.single_channel.complex_chain.pipeline_dag_audit import (
    analyze_effective_dag,
    build_hybrid_input,
    build_mode,
    effective_edges,
    validate_timeline,
)


ROOT = Path(__file__).resolve().parents[4]


def _probe(*, tp: int = 1, dp: int = 1):
    return build_hybrid_input(
        pp=2,
        tp=tp,
        dp=dp,
        ga=4,
        layers=2,
        pp_comm_size=1_048_576,
        tp_comm_size=524_288,
        dp_comm_size=2_097_152,
    )


def test_all_pipeline_modes_produce_acyclic_effective_dags() -> None:
    header, items, job = _probe()
    for mode in (
        "1f1b",
        "interleaved_1f1b",
        "zero_bubble",
        "bidirectional",
        "dualpipe",
    ):
        built = build_mode(
            mode,
            header,
            items,
            job,
            vpp=2,
            gradient_sync_bytes=2_097_152,
        )
        metrics, graph = analyze_effective_dag(
            built,
            bandwidth_bytes_per_us=25_000,
        )

        assert metrics["nodes"]["total"] == len(graph["nodes"])
        assert metrics["edges"]["effective"] == len(graph["edges"])
        assert metrics["edges"]["compute_resource_new"] > 0
        assert metrics["width"]["exact_antichain"] is not None


def test_effective_dag_contains_every_consecutive_compute_order_edge() -> None:
    header, items, job = _probe()
    built = build_mode(
        "zero_bubble",
        header,
        items,
        job,
        vpp=2,
        gradient_sync_bytes=2_097_152,
    )
    _data, resource, effective = effective_edges(built.workload, built.plan)

    for order in built.plan.compute_order.values():
        for source, target in zip(order, order[1:]):
            assert (source, target) in resource
            assert (source, target) in effective


def test_executor_timeline_respects_effective_dag() -> None:
    header, items, job = _probe()
    built = build_mode(
        "1f1b",
        header,
        items,
        job,
        vpp=2,
        gradient_sync_bytes=2_097_152,
    )
    _metrics, graph = analyze_effective_dag(
        built,
        bandwidth_bytes_per_us=25_000,
    )
    result = validate_timeline(
        built,
        graph,
        ROOT / "inputs/topologies/AlibabaHPN_16g_8gps_DualToR_DualPlane_200Gbps_A100",
    )

    assert result["validated"] is True
    assert result["effective_edge_violations"] == []
