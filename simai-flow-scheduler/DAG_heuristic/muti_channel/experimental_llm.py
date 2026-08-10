"""R5: LLM-structured whole-flow candidates on the R4 route model.

Semantic information proposes complete START(set) actions.  Every action is
evaluated with the same Dynamic-pack counterfactual as the general portfolio;
no semantic feature is added as a raw priority bonus.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass, replace
import json
from pathlib import Path
from statistics import mean
import sys
from time import perf_counter
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from DAG_heuristic.common.benchmark import _Builder
from DAG_heuristic.muti_channel.real_dag_adapter import build_route_aware_probe
from DAG_heuristic.muti_channel.algorithms import (
    NonPreemptiveMultiResourceDAG,
    ResourceAction,
    ResourceSchedule,
    ResourceState,
    _complete,
    _replay,
    exact_oracle,
    greedy_action,
    residual_resource_loads,
    schedule_greedy,
)
from DAG_heuristic.muti_channel.legacy_generator import MultiResourceInstance
from DAG_heuristic.muti_channel.topology_fixtures import (
    PLACEMENTS,
    four_rack_core_topology,
    two_rack_topology,
)


@dataclass(frozen=True)
class FlowSemantic:
    dimension: str
    phase: str
    iteration: int
    layer_id: int
    stage: int | None
    src_dp: int
    src_tp: int
    chunk_id: int
    num_chunks: int

    @property
    def backbone(self) -> bool:
        return self.dimension in {"PP", "TP"} and self.phase in {
            "forward",
            "backward_input",
        }

    @property
    def deferred(self) -> bool:
        return self.dimension == "DP" or self.phase == "backward_weight"

    @property
    def template(self) -> tuple[str, str, int | None, int]:
        return self.dimension, self.phase, self.stage, self.layer_id


@dataclass(frozen=True)
class StructuredSchedule:
    schedule: ResourceSchedule
    candidate_usage: dict[str, int]
    candidate_events: int
    mean_candidates: float
    semantic_wins: int
    cache_hits: int = 0
    cache_misses: int = 0


def semantic_sidecar(
    model: NonPreemptiveMultiResourceDAG,
    flow_meta: dict[str, dict],
) -> dict[str, FlowSemantic]:
    result = {}
    for task_id in model.order:
        index = model.index[task_id]
        if model.tasks[index].kind != "comm":
            continue
        meta = flow_meta[task_id]
        result[task_id] = FlowSemantic(
            str(meta["dimension"]),
            str(meta["phase"]),
            int(meta.get("iteration", 0)),
            int(meta.get("layer_id", -1)),
            meta.get("stage"),
            int(meta.get("src_coord", {}).get("dp", 0)),
            int(meta.get("src_coord", {}).get("tp", 0)),
            int(meta.get("chunk_id") or 0),
            int(meta.get("num_chunks") or 1),
        )
    return result


def _pack(
    model: NonPreemptiveMultiResourceDAG,
    state: ResourceState,
    ranked: Iterable[str],
) -> ResourceAction | None:
    selected = []
    occupied = set(model.occupied_resources(state))
    for task_id in ranked:
        resources = model.resources[model.index[task_id]]
        if occupied & resources:
            continue
        selected.append(task_id)
        occupied.update(resources)
    return ResourceAction.start(selected) if selected else None


def _earliest_finish(
    model: NonPreemptiveMultiResourceDAG,
    state: ResourceState,
) -> tuple[int, ...]:
    values = [0] * len(model.tasks)
    for index in range(len(model.tasks)):
        remaining = model.remaining(state, index)
        if not remaining:
            continue
        values[index] = remaining + max(
            (
                values[parent]
                for parent in model.deps[index]
                if model.remaining(state, parent)
            ),
            default=0,
        )
    return tuple(values)


def _future_backbone(
    model: NonPreemptiveMultiResourceDAG,
    state: ResourceState,
    semantics: dict[str, FlowSemantic],
) -> tuple[int, str] | None:
    earliest = _earliest_finish(model, state)
    candidates = []
    for task_id, semantic in semantics.items():
        index = model.index[task_id]
        if not semantic.backbone or state.tasks[index].status != "pending":
            continue
        release = max(
            (
                earliest[parent]
                for parent in model.deps[index]
                if model.remaining(state, parent)
            ),
            default=0,
        )
        if release > 0:
            candidates.append((release, task_id))
    return min(candidates) if candidates else None


def general_candidate_actions(
    model: NonPreemptiveMultiResourceDAG,
    state: ResourceState,
) -> dict[str, ResourceAction]:
    candidates = {}
    for policy in (
        "dynamic_tail",
        "resource_tail",
        "bottleneck_first",
        "spt",
        "lpt",
    ):
        action = greedy_action(model, state, policy)
        candidates.setdefault(policy, action)
    if any(runtime.status == "running" for runtime in state.tasks):
        candidates["wait"] = ResourceAction.wait()
    unique = {}
    for label, action in candidates.items():
        unique.setdefault(action, label)
    return {label: action for action, label in unique.items()}


def semantic_candidate_actions(
    model: NonPreemptiveMultiResourceDAG,
    state: ResourceState,
    semantics: dict[str, FlowSemantic],
) -> dict[str, ResourceAction]:
    ready = list(model.ready_flows(state))
    if not ready:
        return {}
    _path, tail = model.residual_features(state)
    loads = residual_resource_loads(model, state)

    def tail_value(task_id: str) -> int:
        return tail[model.index[task_id]]

    def bottleneck(task_id: str) -> int:
        index = model.index[task_id]
        return max((loads[item] for item in model.resources[index]), default=0)

    rankings: dict[str, list[str]] = {
        "backbone_first": sorted(
            ready,
            key=lambda task_id: (
                semantics[task_id].backbone,
                tail_value(task_id),
                bottleneck(task_id),
                task_id,
            ),
            reverse=True,
        ),
        "optimizer_deadline": sorted(
            ready,
            key=lambda task_id: (
                semantics[task_id].deferred,
                tail_value(task_id),
                bottleneck(task_id),
                task_id,
            ),
            reverse=True,
        ),
        "replica_wavefront": sorted(
            ready,
            key=lambda task_id: (
                -semantics[task_id].src_dp,
                -semantics[task_id].src_tp,
                tail_value(task_id),
                task_id,
            ),
            reverse=True,
        ),
        "chunk_wavefront": sorted(
            ready,
            key=lambda task_id: (
                semantics[task_id].chunk_id,
                tail_value(task_id),
                task_id,
            ),
            reverse=True,
        ),
    }
    for dimension in ("PP", "TP", "DP", "EP"):
        rankings[f"dimension_{dimension}"] = sorted(
            ready,
            key=lambda task_id: (
                semantics[task_id].dimension == dimension,
                tail_value(task_id),
                task_id,
            ),
            reverse=True,
        )

    candidates = {}
    for label, ranked in rankings.items():
        action = _pack(model, state, ranked)
        if action is not None:
            candidates[label] = action

    future = _future_backbone(model, state, semantics)
    if future is not None:
        release, backbone_id = future
        backbone_resources = model.resources[model.index[backbone_id]]
        safe = []
        for task_id in sorted(
            ready,
            key=lambda item: (
                semantics[item].backbone,
                tail_value(item),
                item,
            ),
            reverse=True,
        ):
            resources = model.resources[model.index[task_id]]
            duration = model.remaining(state, model.index[task_id])
            if semantics[task_id].backbone:
                safe.append(task_id)
            elif not (resources & backbone_resources) or duration <= release:
                safe.append(task_id)
        action = _pack(model, state, safe)
        if action is not None:
            candidates["deferred_gap_fill"] = action

    general_actions = set(general_candidate_actions(model, state).values())
    unique = {}
    for label, action in candidates.items():
        if action not in general_actions:
            unique.setdefault(action, label)
    return {label: action for action, label in unique.items()}


def _candidate_portfolio(
    model: NonPreemptiveMultiResourceDAG,
    state: ResourceState,
    semantics: dict[str, FlowSemantic],
    *,
    semantic: bool,
    disabled_labels: frozenset[str] = frozenset(),
) -> dict[str, ResourceAction]:
    result = general_candidate_actions(model, state)
    if semantic:
        for label, action in semantic_candidate_actions(
            model, state, semantics
        ).items():
            if label not in disabled_labels:
                result[label] = action
    unique = {}
    for label, action in result.items():
        unique.setdefault(action, label)
    return {label: action for action, label in unique.items()}


def _frontier_signature(
    model: NonPreemptiveMultiResourceDAG,
    state: ResourceState,
    semantics: dict[str, FlowSemantic],
) -> tuple:
    ready = []
    for task_id in model.ready_flows(state):
        semantic = semantics[task_id]
        ready.append(
            (
                semantic.template,
                semantic.src_dp,
                semantic.src_tp,
                semantic.chunk_id,
                semantic.num_chunks,
                model.remaining(state, model.index[task_id]),
                len(model.resources[model.index[task_id]]),
            )
        )
    active = sorted(
        (
            semantics[task_id].template,
            model.remaining(state, model.index[task_id]),
        )
        for task_id in model.active_flows(state)
    )
    return tuple(sorted(ready)), tuple(active)


def structured_rollout(
    instance: MultiResourceInstance,
    flow_meta: dict[str, dict],
    *,
    semantic: bool,
    disabled_labels: frozenset[str] = frozenset(),
    use_periodic_cache: bool = False,
    time_limit_s: float = 30.0,
    general_incumbent: ResourceSchedule | None = None,
) -> StructuredSchedule:
    started = perf_counter()
    model = NonPreemptiveMultiResourceDAG(instance)
    semantics = semantic_sidecar(model, flow_meta)
    baseline = schedule_greedy(instance)
    if semantic and general_incumbent is None:
        general_incumbent = structured_rollout(
            instance,
            flow_meta,
            semantic=False,
            time_limit_s=time_limit_s,
        ).schedule
    state = model.initial_state()
    actions = []
    usage: Counter[str] = Counter()
    candidate_counts = []
    semantic_wins = 0
    continuation_cache: dict[tuple, int] = {}
    label_cache: dict[tuple, str] = {}
    cache_hits = cache_misses = 0
    fallback = False

    def continuation(successor: ResourceState) -> int:
        key = tuple(
            (runtime.status, runtime.remaining) for runtime in successor.tasks
        )
        if key not in continuation_cache:
            continuation_cache[key] = _complete(
                model, successor, "dynamic_tail"
            )[0]
        return continuation_cache[key]

    while not model.is_finished(state):
        base = greedy_action(model, state, "dynamic_tail")
        if perf_counter() - started > time_limit_s:
            action = base
            label = "timeout_dynamic"
            fallback = True
        else:
            candidates = _candidate_portfolio(
                model,
                state,
                semantics,
                semantic=semantic,
                disabled_labels=disabled_labels,
            )
            candidate_counts.append(len(candidates))
            signature = _frontier_signature(model, state, semantics)
            cached = label_cache.get(signature) if use_periodic_cache else None
            if cached in candidates:
                label = cached
                action = candidates[label]
                cache_hits += 1
            else:
                cache_misses += use_periodic_cache
                scored = []
                general_labels = set(
                    general_candidate_actions(model, state)
                )
                for label, action in candidates.items():
                    transition = model.step(state, action)
                    delta = transition.after.time - transition.before.time
                    value = delta + continuation(transition.after)
                    scored.append(
                        (
                            value,
                            label not in general_labels,
                            action.kind == "wait",
                            label,
                            action,
                        )
                    )
                _value, _semantic_tie, _wait, label, action = min(scored)
                if use_periodic_cache:
                    label_cache[signature] = label
        semantic_wins += label not in general_candidate_actions(model, state)
        usage[label] += 1
        actions.append(action)
        state = model.step(state, action).after

    result = _replay(
        model,
        actions,
        runtime_ms=(perf_counter() - started) * 1000,
        candidate_actions=sum(candidate_counts),
        fallback=fallback,
    )
    incumbent = general_incumbent if semantic else baseline
    assert incumbent is not None
    if incumbent.makespan < result.makespan:
        result = replace(
            incumbent,
            runtime_ms=(perf_counter() - started) * 1000,
            fallback=fallback,
        )
        usage["general_incumbent"] += 1
    return StructuredSchedule(
        result,
        dict(usage),
        len(candidate_counts),
        mean(candidate_counts) if candidate_counts else 0.0,
        semantic_wins,
        cache_hits,
        cache_misses,
    )


def teacher_coverage(
    instance: MultiResourceInstance,
    flow_meta: dict[str, dict],
    *,
    max_events: int = 64,
    max_ready: int = 8,
) -> dict:
    model = NonPreemptiveMultiResourceDAG(instance)
    semantics = semantic_sidecar(model, flow_meta)
    state = model.initial_state()
    events = covered_general = covered_semantic = semantic_only = 0
    skipped = 0
    hits: Counter[str] = Counter()
    while not model.is_finished(state) and events < max_events:
        ready = model.ready_flows(state)
        if len(ready) > max_ready:
            skipped += 1
            action = greedy_action(model, state, "dynamic_tail")
            state = model.step(state, action).after
            continue
        exhaustive = model.legal_actions(state, "optional_idle")
        if len(exhaustive) > 1:
            events += 1
            values = {}
            for action in exhaustive:
                transition = model.step(state, action)
                delta = transition.after.time - transition.before.time
                values[action] = delta + _complete(
                    model, transition.after, "dynamic_tail"
                )[0]
            teacher = min(values.values())
            general = general_candidate_actions(model, state)
            semantic_actions = semantic_candidate_actions(
                model, state, semantics
            )
            general_best = min(values[action] for action in general.values())
            combined = {**general, **semantic_actions}
            semantic_best = min(values[action] for action in combined.values())
            covered_general += general_best == teacher
            covered_semantic += semantic_best == teacher
            semantic_only += semantic_best == teacher and general_best > teacher
            for label, action in semantic_actions.items():
                if values[action] == teacher:
                    hits[label] += 1
        action = greedy_action(model, state, "dynamic_tail")
        state = model.step(state, action).after
    return {
        "events": events,
        "skipped_wide_events": skipped,
        "general_value_coverage": covered_general / max(events, 1),
        "semantic_value_coverage": covered_semantic / max(events, 1),
        "semantic_only_events": semantic_only,
        "semantic_teacher_hits": dict(hits),
    }


SEMANTIC_LABELS = (
    "backbone_first",
    "optimizer_deadline",
    "replica_wavefront",
    "chunk_wavefront",
    "dimension_PP",
    "dimension_TP",
    "dimension_DP",
    "dimension_EP",
    "deferred_gap_fill",
)


def leave_one_feature_out(
    instance: MultiResourceInstance,
    flow_meta: dict[str, dict],
) -> dict[str, int]:
    general = structured_rollout(instance, flow_meta, semantic=False)
    full = structured_rollout(
        instance,
        flow_meta,
        semantic=True,
        general_incumbent=general.schedule,
    )
    result = {"full": full.schedule.makespan}
    for label in SEMANTIC_LABELS:
        result[f"without_{label}"] = structured_rollout(
            instance,
            flow_meta,
            semantic=True,
            disabled_labels=frozenset({label}),
            general_incumbent=general.schedule,
        ).schedule.makespan
    return result


def llm_wait_motif(name: str, *, swap_resources: bool = False):
    """A minimal PP-release gap where semantic partial-idle beats WAIT/pack."""

    builder = _Builder(name, "r5_llm_motif", "predictable PP release gap")
    release = builder.add("release", "compute", 1)
    long_dp = builder.add("long_dp", "comm", 5)
    safe_w = builder.add("safe_w", "comm", 3)
    pp = builder.add("pp", "comm", 1, (release,))
    builder.add("safe_tail", "compute", 6, (safe_w,))
    builder.add("pp_tail", "compute", 6, (pp,))
    blocked = "r1" if not swap_resources else "r0"
    safe = "r0" if not swap_resources else "r1"
    instance = MultiResourceInstance(
        builder.finish(),
        {
            "long_dp": frozenset({blocked}),
            "safe_w": frozenset({safe}),
            "pp": frozenset({blocked}),
        },
    )
    meta = {
        "long_dp": _meta("DP", "backward_weight", 0, 0),
        "safe_w": _meta("DP", "backward_weight", 1, 0),
        "pp": _meta("PP", "forward", 0, 0),
    }
    return instance, meta


def _meta(dimension: str, phase: str, dp: int, tp: int) -> dict:
    return {
        "dimension": dimension,
        "phase": phase,
        "iteration": 0,
        "layer_id": 0,
        "stage": 0,
        "src_coord": {"dp": dp, "tp": tp},
        "chunk_id": tp,
        "num_chunks": 2,
    }


def build_probe_scenarios(quantum_us: float) -> list[tuple[str, MultiResourceInstance, dict]]:
    configurations = (
        (
            "1f1b_four_rack_tp_cross",
            "1f1b",
            four_rack_core_topology(),
            PLACEMENTS["tp_cross"],
        ),
        (
            "bidirectional_four_rack_tp_cross",
            "bidirectional",
            four_rack_core_topology(),
            PLACEMENTS["tp_cross"],
        ),
        (
            "1f1b_two_rack_pp_cross",
            "1f1b",
            two_rack_topology(),
            PLACEMENTS["pp_cross"],
        ),
        (
            "bidirectional_two_rack_pp_cross",
            "bidirectional",
            two_rack_topology(),
            PLACEMENTS["pp_cross"],
        ),
    )
    result = []
    for name, mode, topology, placement in configurations:
        instance, info, _graph = build_route_aware_probe(
            mode=mode,
            topology_path=Path("unused"),
            topology=topology,
            assigned_nodes=placement,
            quantum_us=quantum_us,
            ga=2,
            layers=2,
        )
        result.append((name, instance, info["flow_meta"]))
    return result


def evaluate_scenario(
    name: str,
    instance: MultiResourceInstance,
    flow_meta: dict[str, dict],
    *,
    with_teacher: bool,
    with_ablation: bool = False,
) -> dict:
    dynamic = schedule_greedy(instance)
    general = structured_rollout(instance, flow_meta, semantic=False)
    semantic = structured_rollout(
        instance,
        flow_meta,
        semantic=True,
        general_incumbent=general.schedule,
    )
    periodic = structured_rollout(
        instance,
        flow_meta,
        semantic=True,
        use_periodic_cache=True,
        general_incumbent=general.schedule,
    )
    used_semantic = sorted(
        set(semantic.candidate_usage) & set(SEMANTIC_LABELS)
    )
    ablation = {}
    if with_ablation:
        for label in used_semantic:
            ablation[f"without_{label}"] = structured_rollout(
                instance,
                flow_meta,
                semantic=True,
                disabled_labels=frozenset({label}),
                general_incumbent=general.schedule,
            ).schedule.makespan
    return {
        "name": name,
        "tasks": len(instance.dag.tasks),
        "flows": len(flow_meta),
        "methods": {
            "dynamic": {
                "makespan": dynamic.makespan,
                "runtime_ms": dynamic.runtime_ms,
            },
            "general_rollout": _structured_dict(general),
            "semantic_rollout": _structured_dict(semantic),
            "periodic_semantic": _structured_dict(periodic),
        },
        "semantic_improvement": (
            general.schedule.makespan - semantic.schedule.makespan
        ),
        "teacher_coverage": (
            teacher_coverage(instance, flow_meta) if with_teacher else None
        ),
        "used_feature_ablation": ablation,
    }


def _structured_dict(result: StructuredSchedule) -> dict:
    return {
        "makespan": result.schedule.makespan,
        "runtime_ms": result.schedule.runtime_ms,
        "candidate_usage": result.candidate_usage,
        "candidate_events": result.candidate_events,
        "mean_candidates": result.mean_candidates,
        "semantic_wins": result.semantic_wins,
        "cache_hits": result.cache_hits,
        "cache_misses": result.cache_misses,
        "fallback": result.schedule.fallback,
    }


def run_study(*, quantum_us: float) -> dict:
    scenarios = []
    for index, (name, instance, meta) in enumerate(build_probe_scenarios(quantum_us)):
        scenarios.append(
            evaluate_scenario(
                name,
                instance,
                meta,
                with_teacher=index < 2,
                with_ablation=index < 2,
            )
        )
    motifs = []
    for index, swap in enumerate((False, True)):
        instance, meta = llm_wait_motif(f"llm_wait_motif_{index}", swap_resources=swap)
        row = evaluate_scenario(
            f"llm_wait_motif_{index}",
            instance,
            meta,
            with_teacher=True,
        )
        row["optimum"] = exact_oracle(instance).makespan
        row["leave_one_out"] = leave_one_feature_out(instance, meta)
        motifs.append(row)
    return {
        "model": {
            "flow": "non-preemptive full-route START(set)",
            "semantic_evaluation": "whole-action Dynamic-pack counterfactual",
            "cache": "semantic label only; legal action regenerated",
        },
        "probe_scenarios": scenarios,
        "motifs": motifs,
        "exit_condition": {
            "probe_scenarios_improved": sum(
                row["semantic_improvement"] > 0 for row in scenarios
            ),
            "motifs_improved": sum(
                row["semantic_improvement"] > 0 for row in motifs
            ),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quantum-us", type=float, default=25.0)
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "outputs/nonpreemptive_llm_structured/r5_summary.json",
    )
    args = parser.parse_args()
    report = run_study(quantum_us=args.quantum_us)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
