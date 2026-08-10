from __future__ import annotations

from DAG_heuristic.muti_channel.experimental_llm import (
    FlowSemantic,
    leave_one_feature_out,
    llm_wait_motif,
    semantic_candidate_actions,
    semantic_sidecar,
    structured_rollout,
    teacher_coverage,
)
from DAG_heuristic.muti_channel.algorithms import (
    NonPreemptiveMultiResourceDAG,
    exact_oracle,
)


def test_flow_semantics_classify_backbone_and_deferred() -> None:
    backbone = FlowSemantic("PP", "forward", 0, 0, 0, 0, 0, 0, 1)
    deferred = FlowSemantic("DP", "backward_weight", 0, 0, 0, 0, 0, 0, 1)

    assert backbone.backbone is True
    assert backbone.deferred is False
    assert deferred.backbone is False
    assert deferred.deferred is True


def test_gap_fill_candidate_is_legal_nonmaximal_start() -> None:
    instance, meta = llm_wait_motif("gap_fill")
    model = NonPreemptiveMultiResourceDAG(instance)
    state = model.initial_state()
    semantics = semantic_sidecar(model, meta)
    candidates = semantic_candidate_actions(model, state, semantics)

    assert candidates["deferred_gap_fill"].starts == ("safe_w",)
    assert model.compatible(state, candidates["deferred_gap_fill"].starts)
    assert candidates["deferred_gap_fill"].starts not in model.start_subsets(
        state, maximal_only=True
    )


def test_semantic_gap_fill_beats_general_and_reaches_exact_optimum() -> None:
    instance, meta = llm_wait_motif("gap_fill_opt")
    general = structured_rollout(instance, meta, semantic=False)
    semantic = structured_rollout(instance, meta, semantic=True)
    optimum = exact_oracle(instance).makespan

    assert general.schedule.makespan == 10
    assert semantic.schedule.makespan == optimum == 9
    assert semantic.candidate_usage["deferred_gap_fill"] == 1


def test_leave_one_out_identifies_gap_fill_as_necessary() -> None:
    instance, meta = llm_wait_motif("gap_fill_ablation")
    result = leave_one_feature_out(instance, meta)

    assert result["full"] == 9
    assert result["without_deferred_gap_fill"] == 10
    assert all(
        value == 9
        for name, value in result.items()
        if name not in {"full", "without_deferred_gap_fill"}
    )


def test_semantic_teacher_coverage_repairs_general_miss() -> None:
    instance, meta = llm_wait_motif("gap_fill_teacher")
    coverage = teacher_coverage(instance, meta)

    assert coverage["general_value_coverage"] == 0.5
    assert coverage["semantic_value_coverage"] == 1.0
    assert coverage["semantic_only_events"] == 1


def test_periodic_cache_regenerates_legal_actions_and_keeps_general() -> None:
    instance, meta = llm_wait_motif("gap_fill_cache")
    general = structured_rollout(instance, meta, semantic=False)
    cached = structured_rollout(
        instance,
        meta,
        semantic=True,
        use_periodic_cache=True,
        general_incumbent=general.schedule,
    )

    assert cached.schedule.makespan <= general.schedule.makespan
    assert cached.schedule.makespan == 9
