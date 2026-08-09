"""Stage-4: LLM-semantic compatible-set candidates and teacher coverage.

This study does not add semantic bonuses directly to one priority score.
Instead it generates a small portfolio of legal maximal flow sets using LLM
roles (pipeline backbone, deferred DP/W work, optimizer slack and dimension
balance), then evaluates those sets with the same next-event end-to-end
counterfactual used by the topology studies.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
import json
from pathlib import Path
from statistics import mean
import sys
from time import perf_counter


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.benchmark_dag_oracle import _is_finished  # noqa: E402
from scripts.study_llm_route_windows import build_route_aware_probe  # noqa: E402
from scripts.study_multiresource_dag import (  # noqa: E402
    MultiResourceDAG,
    MultiResourceInstance,
    MultiResourceResult,
    multi_resource_lower_bounds,
    residual_resource_loads,
    schedule_multiresource,
)
from scripts.study_small_topology_sensitivity import (  # noqa: E402
    PLACEMENTS,
    four_rack_core_topology,
    single_switch_topology,
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
            "forward", "backward_input",
        }

    @property
    def deferred(self) -> bool:
        return self.dimension == "DP" or self.phase == "backward_weight"

    @property
    def template(self) -> tuple[str, str, int | None, int]:
        return self.dimension, self.phase, self.stage, self.layer_id


@dataclass
class StructuredRolloutResult:
    schedule: MultiResourceResult
    candidate_usage: dict[str, int]
    mean_unique_candidates: float
    decisions_with_choice: int


@dataclass
class PeriodicCacheResult:
    schedule: MultiResourceResult
    cache_hits: int
    cache_misses: int
    fallback: bool


def semantic_sidecar(
    model: MultiResourceDAG,
    flow_meta: dict[str, dict],
) -> dict[int, FlowSemantic]:
    """Bind analyzer-owned LLM metadata to indexed communication tasks."""

    result = {}
    for index, task_id in enumerate(model.order):
        if model.tasks[index].kind != "comm":
            continue
        meta = flow_meta[task_id]
        result[index] = FlowSemantic(
            dimension=meta["dimension"],
            phase=meta["phase"],
            iteration=int(meta["iteration"]),
            layer_id=int(meta["layer_id"]),
            stage=meta["stage"],
            src_dp=int(meta["src_coord"]["dp"]),
            src_tp=int(meta["src_coord"]["tp"]),
            chunk_id=int(meta["chunk_id"] or 0),
            num_chunks=int(meta["num_chunks"] or 1),
        )
    return result


def _pack(model: MultiResourceDAG, ranked: list[int]) -> tuple[int, ...]:
    selected: list[int] = []
    occupied = set()
    for index in ranked:
        if not (occupied & model.resources[index]):
            selected.append(index)
            occupied.update(model.resources[index])
    return tuple(sorted(selected))


def _round_robin_dimensions(
    ready: list[int],
    semantics: dict[int, FlowSemantic],
    tail: tuple[int, ...],
) -> list[int]:
    groups: dict[str, list[int]] = defaultdict(list)
    for index in ready:
        groups[semantics[index].dimension].append(index)
    for values in groups.values():
        values.sort(key=lambda index: (tail[index], -index), reverse=True)
    order = [name for name in ("PP", "TP", "DP", "EP", "OTHER") if groups[name]]
    ranked: list[int] = []
    while any(groups[name] for name in order):
        for name in order:
            if groups[name]:
                ranked.append(groups[name].pop(0))
    return ranked


def semantic_candidate_sets(
    model: MultiResourceDAG,
    state: tuple[int, ...],
    semantics: dict[int, FlowSemantic],
) -> dict[str, tuple[int, ...]]:
    """Return a deduplicated portfolio of maximal compatible flow sets."""

    ready = model.ready(state)
    if not ready:
        return {"idle": ()}
    analysis = model.residual.analyze(state)
    loads = residual_resource_loads(model, state)

    def bottleneck(index: int) -> int:
        return max((loads[resource] for resource in model.resources[index]), default=0)

    horizon = analysis.lower_bound

    def slack(index: int) -> int:
        return horizon - (
            model.remaining(state, index) + analysis.tail[index]
        )

    rankings = {
        "dynamic": sorted(
            ready,
            key=lambda index: (
                analysis.tail[index], -model.remaining(state, index), -index,
            ),
            reverse=True,
        ),
        "bottleneck": sorted(
            ready,
            key=lambda index: (
                bottleneck(index), analysis.tail[index], -index,
            ),
            reverse=True,
        ),
        "backbone": sorted(
            ready,
            key=lambda index: (
                semantics[index].backbone,
                analysis.tail[index], bottleneck(index), -index,
            ),
            reverse=True,
        ),
        "optimizer_deadline": sorted(
            ready,
            key=lambda index: (
                -slack(index),
                semantics[index].deferred,
                analysis.tail[index], -index,
            ),
            reverse=True,
        ),
        "deferred_gap_fill": sorted(
            ready,
            key=lambda index: (
                semantics[index].backbone,
                semantics[index].deferred,
                -model.remaining(state, index) if semantics[index].deferred else analysis.tail[index],
                -index,
            ),
            reverse=True,
        ),
        # Fewer occupied resources first tends to leave room for more parallel
        # flows; critical tail breaks ties so width is not pursued blindly.
        "resource_complement": sorted(
            ready,
            key=lambda index: (
                len(model.resources[index]),
                -analysis.tail[index],
                model.remaining(state, index),
                index,
            ),
        ),
        "dimension_round_robin": _round_robin_dimensions(
            ready, semantics, analysis.tail,
        ),
        # Serializers and collective expansion repeatedly expose symmetric
        # replica waves.  Preserve the logical DP order as a candidate rather
        # than relying on a one-tick tail difference between replicas.
        "replica_wavefront": sorted(
            ready,
            key=lambda index: (
                semantics[index].src_dp,
                semantics[index].src_tp,
                -analysis.tail[index],
                index,
            ),
        ),
        # Equal endpoint/phase flows may be different chunks of the same Ring
        # step.  Expose the opposite end of the chunk wave as a candidate;
        # downstream readiness, not task id, decides which one is useful.
        "chunk_wavefront": sorted(
            ready,
            key=lambda index: (
                -semantics[index].chunk_id,
                semantics[index].src_dp,
                semantics[index].src_tp,
                -analysis.tail[index],
                index,
            ),
        ),
    }
    unique: dict[tuple[int, ...], str] = {}
    for label, ranked in rankings.items():
        selected = _pack(model, ranked)
        unique.setdefault(selected, label)
    return {label: selected for selected, label in unique.items()}


def compressed_frontier_signature(
    model: MultiResourceDAG,
    state: tuple[int, ...],
    semantics: dict[int, FlowSemantic],
    *,
    load_bucket: int = 8,
    include_load: bool = True,
) -> tuple:
    """Periodic state key retaining replica, chunk and coarse congestion.

    It intentionally omits absolute task ids and microbatch numbers so steady
    pipeline waves can match, while retaining the fields that explained the
    misses of the earlier coarse template key.
    """

    loads = residual_resource_loads(model, state)
    descriptors = []
    for index in model.ready(state):
        semantic = semantics[index]
        bottleneck = max((loads[item] for item in model.resources[index]), default=0)
        descriptor = (
            semantic.template,
            semantic.src_dp,
            semantic.src_tp,
            semantic.chunk_id,
            semantic.num_chunks,
            len(model.resources[index]),
        )
        if include_load:
            descriptor += (
                max(1, model.remaining(state, index) // load_bucket),
                bottleneck // load_bucket,
            )
        descriptors.append(descriptor)
    return tuple(sorted(descriptors))


def _complete_dynamic(model: MultiResourceDAG, initial: tuple[int, ...]) -> int:
    state = model.close(initial)
    elapsed = 0
    while not _is_finished(state):
        selected = model.greedy_set(state, "dynamic_tail") if model.ready(state) else ()
        state = model.close(model.tick(state, selected))
        elapsed += 1
    return elapsed


def structured_rollout(
    instance: MultiResourceInstance,
    flow_meta: dict[str, dict],
    *,
    allowed_labels: set[str] | None = None,
    _continuation_cache: dict[tuple[int, ...], int] | None = None,
) -> StructuredRolloutResult:
    """Counterfactually select among semantic sets with a Dynamic incumbent."""

    model = MultiResourceDAG(instance)
    semantics = semantic_sidecar(model, flow_meta)
    baseline = schedule_multiresource(instance, "dynamic_tail")
    started = perf_counter()
    state = model.initial
    decisions: list[tuple[str, ...]] = []
    usage: Counter[str] = Counter()
    candidate_counts: list[int] = []
    decisions_with_choice = 0
    continuation_cache = (
        _continuation_cache if _continuation_cache is not None else {}
    )

    def continuation(successor: tuple[int, ...]) -> int:
        if successor not in continuation_cache:
            continuation_cache[successor] = _complete_dynamic(model, successor)
        return continuation_cache[successor]

    while not _is_finished(model.close(state)):
        state = model.close(state)
        if not model.ready(state):
            decisions.append(())
            state = model.tick(state, ())
            continue
        candidates = semantic_candidate_sets(model, state, semantics)
        if allowed_labels is not None:
            candidates = {
                label: selected for label, selected in candidates.items()
                if label in allowed_labels or label == "dynamic"
            }
        candidate_counts.append(len(candidates))
        decisions_with_choice += len(candidates) > 1
        evaluated = []
        for label, selected in candidates.items():
            successor, delta = model.advance_to_event(state, selected)
            evaluated.append((
                delta + continuation(successor),
                delta + model.residual.analyze(successor).lower_bound,
                label,
                selected,
                successor,
                delta,
            ))
        _value, _bound, label, selected, successor, delta = min(evaluated)
        usage[label] += 1
        names = tuple(model.order[index] for index in selected)
        decisions.extend(names for _ in range(delta))
        state = successor

    schedule = MultiResourceResult(
        len(decisions), decisions, sum(not item for item in decisions),
        (perf_counter() - started) * 1000,
    )
    if baseline.makespan < schedule.makespan:
        schedule = baseline
        usage["dynamic_incumbent_fallback"] += 1
    return StructuredRolloutResult(
        schedule,
        dict(usage),
        mean(candidate_counts) if candidate_counts else 0,
        decisions_with_choice,
    )


def periodic_cached_rollout(
    instance: MultiResourceInstance,
    flow_meta: dict[str, dict],
    *,
    include_load: bool = True,
) -> PeriodicCacheResult:
    """Reuse a learned semantic label at repeated compressed frontiers.

    A miss performs the regular next-event counterfactual and stores only the
    winning *label*.  A hit regenerates the legal set for the current state and
    applies that label.  The completed schedule is compared with Dynamic-tail,
    so cache aliasing can never worsen the returned incumbent.
    """

    model = MultiResourceDAG(instance)
    semantics = semantic_sidecar(model, flow_meta)
    baseline = schedule_multiresource(instance, "dynamic_tail")
    started = perf_counter()
    state = model.initial
    decisions: list[tuple[str, ...]] = []
    labels: dict[tuple, str] = {}
    continuation_cache: dict[tuple[int, ...], int] = {}
    hits = 0
    misses = 0

    def continuation(successor: tuple[int, ...]) -> int:
        if successor not in continuation_cache:
            continuation_cache[successor] = _complete_dynamic(model, successor)
        return continuation_cache[successor]

    while not _is_finished(model.close(state)):
        state = model.close(state)
        if not model.ready(state):
            decisions.append(())
            state = model.tick(state, ())
            continue
        candidates = semantic_candidate_sets(model, state, semantics)
        signature = compressed_frontier_signature(
            model, state, semantics, include_load=include_load,
        )
        cached = labels.get(signature)
        if cached in candidates:
            label = cached
            selected = candidates[label]
            successor, delta = model.advance_to_event(state, selected)
            hits += 1
        else:
            misses += 1
            evaluated = []
            for label, selected in candidates.items():
                successor, delta = model.advance_to_event(state, selected)
                evaluated.append((
                    delta + continuation(successor), label, selected, successor, delta,
                ))
            _score, label, selected, successor, delta = min(evaluated)
            labels[signature] = label
        names = tuple(model.order[index] for index in selected)
        decisions.extend(names for _ in range(delta))
        state = successor

    schedule = MultiResourceResult(
        len(decisions), decisions, sum(not item for item in decisions),
        (perf_counter() - started) * 1000,
    )
    fallback = baseline.makespan < schedule.makespan
    if fallback:
        schedule = baseline
    return PeriodicCacheResult(schedule, hits, misses, fallback)


def leave_one_feature_out(
    instance: MultiResourceInstance,
    flow_meta: dict[str, dict],
) -> dict[str, int]:
    """Full-schedule leave-one-candidate-family-out ablation."""

    labels = {
        "dynamic", "bottleneck", "backbone", "optimizer_deadline",
        "deferred_gap_fill", "resource_complement", "dimension_round_robin",
        "replica_wavefront", "chunk_wavefront",
    }
    shared_cache: dict[tuple[int, ...], int] = {}
    full = structured_rollout(
        instance, flow_meta, _continuation_cache=shared_cache,
    ).schedule.makespan
    result = {"full": full}
    for removed in (
        "backbone", "optimizer_deadline", "deferred_gap_fill",
        "dimension_round_robin", "replica_wavefront", "chunk_wavefront",
    ):
        result[f"without_{removed}"] = structured_rollout(
            instance, flow_meta, allowed_labels=labels - {removed},
            _continuation_cache=shared_cache,
        ).schedule.makespan
    return result


def teacher_coverage(
    instance: MultiResourceInstance,
    flow_meta: dict[str, dict],
    *,
    max_events: int = 64,
) -> dict:
    """Compare semantic candidates with exhaustive one-event set evaluation."""

    model = MultiResourceDAG(instance)
    semantics = semantic_sidecar(model, flow_meta)
    state = model.initial
    events = 0
    choice_events = 0
    covered = 0
    exact_action_covered = 0
    feature_hits: Counter[str] = Counter()
    missed_events: list[dict] = []
    candidate_counts: list[int] = []
    exhaustive_counts: list[int] = []
    signatures: dict[tuple, Counter[str]] = defaultdict(Counter)
    continuation_cache: dict[tuple[int, ...], int] = {}

    def continuation(successor: tuple[int, ...]) -> int:
        if successor not in continuation_cache:
            continuation_cache[successor] = _complete_dynamic(model, successor)
        return continuation_cache[successor]

    while not _is_finished(model.close(state)) and events < max_events:
        state = model.close(state)
        ready = model.ready(state)
        if not ready:
            state = model.tick(state, ())
            continue
        dynamic = model.greedy_set(state, "dynamic_tail")
        exhaustive = model.maximal_compatible_sets(ready)
        candidates = semantic_candidate_sets(model, state, semantics)
        if len(exhaustive) > 1:
            choice_events += 1
            candidate_counts.append(len(candidates))
            exhaustive_counts.append(len(exhaustive))
            values = {}
            for selected in exhaustive:
                successor, delta = model.advance_to_event(state, selected)
                values[selected] = delta + continuation(successor)
            teacher_value = min(values.values())
            teacher_sets = {item for item, value in values.items() if value == teacher_value}
            semantic_values = {
                label: values[selected]
                for label, selected in candidates.items()
            }
            best_semantic = min(semantic_values.values())
            covered += best_semantic == teacher_value
            exact_action_covered += any(
                selected in teacher_sets for selected in candidates.values()
            )
            for label, value in semantic_values.items():
                if value == teacher_value:
                    feature_hits[label] += 1
            if best_semantic != teacher_value and len(missed_events) < 5:
                missed_events.append({
                    "event": events,
                    "teacher_value": teacher_value,
                    "best_semantic_value": best_semantic,
                    "ready": [
                        {
                            "task_id": model.order[index],
                            "remaining": model.remaining(state, index),
                            "tail": model.residual.analyze(state).tail[index],
                            "semantic": asdict(semantics[index]),
                            "resources": sorted(map(str, model.resources[index])),
                        }
                        for index in ready
                    ],
                    "teacher_sets": [
                        [model.order[index] for index in selected]
                        for selected in sorted(teacher_sets)
                    ],
                    "semantic_sets": {
                        label: {
                            "tasks": [model.order[index] for index in candidates[label]],
                            "value": semantic_values[label],
                        }
                        for label in candidates
                    },
                })
            signature = tuple(sorted(Counter(
                semantics[index].template for index in ready
            ).items()))
            winning_label = min(
                label for label, value in semantic_values.items()
                if value == best_semantic
            )
            signatures[signature][winning_label] += 1
            events += 1
        state, _delta = model.advance_to_event(state, dynamic)

    repeated = [counts for counts in signatures.values() if sum(counts.values()) > 1]
    consistency = [max(counts.values()) / sum(counts.values()) for counts in repeated]
    return {
        "choice_events": choice_events,
        "value_coverage": covered / max(choice_events, 1),
        "exact_action_coverage": exact_action_covered / max(choice_events, 1),
        "feature_teacher_hits": dict(feature_hits),
        "missed_events": missed_events,
        "mean_semantic_candidates": mean(candidate_counts) if candidate_counts else 0,
        "mean_exhaustive_sets": mean(exhaustive_counts) if exhaustive_counts else 0,
        "unique_frontier_templates": len(signatures),
        "repeated_frontier_templates": len(repeated),
        "mean_repeated_template_consistency": mean(consistency) if consistency else None,
    }


def evaluate_scenario(
    *,
    mode: str,
    topology_name: str,
    quantum_us: float,
) -> dict:
    topology = (
        four_rack_core_topology() if topology_name == "four_rack_core"
        else single_switch_topology()
    )
    instance, info, _graph = build_route_aware_probe(
        mode=mode,
        topology_path=Path("unused-when-topology-is-supplied"),
        topology=topology,
        quantum_us=quantum_us,
        assigned_nodes=PLACEMENTS["tp_cross"],
        ga=2,
        layers=2,
    )
    flow_meta = info["flow_meta"]
    structured = structured_rollout(instance, flow_meta)
    periodic = periodic_cached_rollout(instance, flow_meta)
    topology_only = structured_rollout(
        instance,
        flow_meta,
        allowed_labels={"dynamic", "bottleneck", "resource_complement"},
    )
    fast = {
        policy: schedule_multiresource(instance, policy)
        for policy in ("dynamic_tail", "resource_tail", "bottleneck_first", "lpt")
    }
    return {
        "mode": mode,
        "topology": topology_name,
        "placement": "tp_cross",
        "tasks": len(instance.dag.tasks),
        "flows": len(flow_meta),
        "lower_bounds": multi_resource_lower_bounds(MultiResourceDAG(instance)),
        "teacher_coverage": teacher_coverage(instance, flow_meta),
        "methods": {
            **{
                name: {"makespan": result.makespan, "runtime_ms": result.runtime_ms}
                for name, result in fast.items()
            },
            "semantic_rollout": {
                "makespan": structured.schedule.makespan,
                "runtime_ms": structured.schedule.runtime_ms,
                "candidate_usage": structured.candidate_usage,
                "mean_unique_candidates": structured.mean_unique_candidates,
                "decisions_with_choice": structured.decisions_with_choice,
            },
            "periodic_cached_rollout": {
                "makespan": periodic.schedule.makespan,
                "runtime_ms": periodic.schedule.runtime_ms,
                "cache_hits": periodic.cache_hits,
                "cache_misses": periodic.cache_misses,
                "fallback": periodic.fallback,
            },
            "topology_only_rollout": {
                "makespan": topology_only.schedule.makespan,
                "runtime_ms": topology_only.schedule.runtime_ms,
                "candidate_usage": topology_only.candidate_usage,
                "mean_unique_candidates": topology_only.mean_unique_candidates,
                "decisions_with_choice": topology_only.decisions_with_choice,
            },
        },
    }


def run_study(*, modes: list[str], topologies: list[str], quantum_us: float) -> dict:
    scenarios = [
        evaluate_scenario(mode=mode, topology_name=topology, quantum_us=quantum_us)
        for topology in topologies
        for mode in modes
    ]
    return {
        "config": {
            "modes": modes, "topologies": topologies, "quantum_us": quantum_us,
        },
        "semantic_definitions": {
            "backbone": "PP/TP flows in forward or backward_input phase",
            "deferred": "DP flows or backward_weight phase flows",
            "optimizer_slack": "residual LB - (flow remaining + residual tail)",
            "replica_wavefront": "logical DP replica order within [PP][DP][TP] layout",
            "chunk_wavefront": "alternative Ring chunk order for equal endpoint/template flows",
        },
        "scenarios": scenarios,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--modes", nargs="+", default=["1f1b", "bidirectional"])
    parser.add_argument(
        "--topologies", nargs="+",
        choices=["single_switch", "four_rack_core"],
        default=["single_switch", "four_rack_core"],
    )
    parser.add_argument("--quantum-us", type=float, default=25.0)
    parser.add_argument(
        "--output", type=Path,
        default=ROOT / "outputs" / "llm_structured_candidates" / "report.json",
    )
    args = parser.parse_args()
    report = run_study(
        modes=args.modes, topologies=args.topologies, quantum_us=args.quantum_us,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
