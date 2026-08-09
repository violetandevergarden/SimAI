"""Tests for LLM-semantic compatible-set candidates."""

from scripts.benchmark_dag_oracle import _Builder
from scripts.study_llm_structured_candidates import (
    compressed_frontier_signature,
    periodic_cached_rollout,
    FlowSemantic,
    semantic_candidate_sets,
    semantic_sidecar,
    structured_rollout,
)
from scripts.study_multiresource_dag import MultiResourceDAG, MultiResourceInstance


def test_flow_semantic_classifies_backbone_and_deferred_work() -> None:
    backbone = FlowSemantic("PP", "backward_input", 1, 2, 0, 0, 0, 0, 2)
    deferred = FlowSemantic("DP", "backward_weight", 1, 2, 0, 1, 0, 1, 2)

    assert backbone.backbone is True
    assert backbone.deferred is False
    assert deferred.backbone is False
    assert deferred.deferred is True


def _replica_instance():
    builder = _Builder("replicas", "test", "two symmetric DP replica waves")
    x0 = builder.add("x0", "comm", 1, role="TP:forward:0")
    y0 = builder.add("y0", "comm", 1, role="TP:forward:0")
    x1 = builder.add("x1", "comm", 1, role="TP:forward:0")
    y1 = builder.add("y1", "comm", 1, role="TP:forward:0")
    builder.add("tx0", "compute", 1, (x0,))
    builder.add("ty0", "compute", 1, (y0,))
    builder.add("tx1", "compute", 2, (x1,))
    builder.add("ty1", "compute", 2, (y1,))
    instance = MultiResourceInstance(
        builder.finish(),
        {
            "x0": frozenset({"x"}), "x1": frozenset({"x"}),
            "y0": frozenset({"y"}), "y1": frozenset({"y"}),
        },
    )
    meta = {}
    for name, replica, tp in (("x0", 0, 0), ("y0", 0, 1), ("x1", 1, 0), ("y1", 1, 1)):
        meta[name] = {
            "dimension": "TP", "phase": "forward", "iteration": 0,
            "layer_id": 0, "stage": 0,
            "chunk_id": tp, "num_chunks": 2,
            "src_coord": {"pp": 0, "dp": replica, "tp": tp},
        }
    return instance, meta


def test_replica_wavefront_adds_a_candidate_missing_from_dynamic_tail() -> None:
    instance, meta = _replica_instance()
    model = MultiResourceDAG(instance)
    semantics = semantic_sidecar(model, meta)
    candidates = semantic_candidate_sets(model, model.initial, semantics)

    assert set(candidates["dynamic"]) == {
        model.order.index("x1"), model.order.index("y1"),
    }
    assert set(candidates["replica_wavefront"]) == {
        model.order.index("x0"), model.order.index("y0"),
    }


def test_semantic_rollout_keeps_dynamic_incumbent() -> None:
    instance, meta = _replica_instance()
    baseline = structured_rollout(
        instance, meta, allowed_labels={"dynamic"},
    )
    enhanced = structured_rollout(instance, meta)

    assert enhanced.schedule.makespan <= baseline.schedule.makespan
    assert len(enhanced.schedule.decisions) == enhanced.schedule.makespan


def test_compressed_signature_can_drop_load_buckets() -> None:
    instance, meta = _replica_instance()
    model = MultiResourceDAG(instance)
    semantics = semantic_sidecar(model, meta)

    detailed = compressed_frontier_signature(model, model.initial, semantics)
    periodic = compressed_frontier_signature(
        model, model.initial, semantics, include_load=False,
    )

    assert detailed != periodic
    assert len(detailed) == len(periodic)
