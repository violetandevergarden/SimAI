"""Stage-4a study: topology conflicts for small communication DAGs.

This is an intentionally small bridge between the single-channel oracle and
the full analytical executor.  A communication occupies a set of unit-capacity
logical resources (normally directed route links); ready communications whose
sets are disjoint progress concurrently.  Durations are integral and flows are
preemptible at quantum boundaries.  Compute semantics and dependency handling
are identical to :mod:`scripts.benchmark_dag_oracle`.

The model deliberately does *not* approximate bandwidth sharing.  Its purpose
is to expose route-overlap decisions and to provide an exact oracle for small
windows before a heuristic is integrated into the production executor.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import asdict, dataclass
from functools import lru_cache
import json
from pathlib import Path
import random
from statistics import mean
import sys
from time import perf_counter
from typing import Hashable, Iterable


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from DAG_heuristic.common.benchmark import (  # noqa: E402
    BenchmarkDAG,
    _Builder,
    _compute_closure,
    _indexed,
    _is_finished,
)
from DAG_heuristic.single_channel.complex_chain.legacy_generator import (  # noqa: E402
    ResidualDAG,
    random_join_dag,
)


Resource = Hashable
State = tuple[int, ...]


@dataclass(frozen=True)
class MultiResourceInstance:
    dag: BenchmarkDAG
    resources: dict[str, frozenset[Resource]]

    def validate(self) -> list[str]:
        errors = self.dag.validate()
        task_map = self.dag.task_map()
        comm_ids = {task.task_id for task in self.dag.tasks if task.kind == "comm"}
        missing = comm_ids - self.resources.keys()
        extra = self.resources.keys() - comm_ids
        if missing:
            errors.append(f"communications without resources: {sorted(missing)}")
        if extra:
            errors.append(f"resources for non-communications: {sorted(extra)}")
        for task_id, values in self.resources.items():
            if task_id in task_map and not isinstance(values, frozenset):
                errors.append(f"{task_id}: resource set must be a frozenset")
            if task_id in task_map and task_map[task_id].duration <= 0:
                errors.append(f"{task_id}: communication duration must be positive")
        return errors


@dataclass
class MultiResourceResult:
    makespan: int
    decisions: list[tuple[str, ...]]
    network_idle_ticks: int
    runtime_ms: float
    explored_states: int = 0
    lower_bounds: dict[str, int] | None = None


class MultiResourceDAG:
    """Indexed residual DAG with route-resource conflict operations."""

    def __init__(self, instance: MultiResourceInstance):
        errors = instance.validate()
        if errors:
            raise ValueError(errors)
        self.instance = instance
        self.order, self.tasks, self.deps = _indexed(instance.dag)
        self.resources = tuple(
            instance.resources.get(task_id, frozenset()) for task_id in self.order
        )
        self.residual = ResidualDAG(instance.dag)
        self.initial = self.close(tuple(-1 for _ in self.tasks))

    def close(self, state: State) -> State:
        return _compute_closure(self.tasks, self.deps, state)

    def ready(self, state: State) -> list[int]:
        return [
            index
            for index, task in enumerate(self.tasks)
            if task.kind == "comm"
            and state[index] != 0
            and (state[index] > 0 or all(state[parent] == 0 for parent in self.deps[index]))
        ]

    def remaining(self, state: State, index: int) -> int:
        return state[index] if state[index] > 0 else self.tasks[index].duration

    def compatible(self, selected: Iterable[int]) -> bool:
        occupied: set[Resource] = set()
        for index in selected:
            if occupied & self.resources[index]:
                return False
            occupied.update(self.resources[index])
        return True

    def tick(self, raw_state: State, selected: Iterable[int]) -> State:
        state = self.close(raw_state)
        chosen = tuple(selected)
        ready = set(self.ready(state))
        if any(index not in ready for index in chosen):
            raise ValueError("selected communication is not ready")
        if not self.compatible(chosen):
            raise ValueError("selected communications have a resource conflict")
        values = list(state)
        for index, task in enumerate(self.tasks):
            if task.kind == "compute" and values[index] > 0:
                values[index] -= 1
        for index in chosen:
            remaining = values[index]
            if remaining == -1:
                remaining = self.tasks[index].duration
            values[index] = remaining - 1
        return tuple(values)

    def maximal_compatible_sets(self, ready: Iterable[int]) -> list[tuple[int, ...]]:
        """Enumerate inclusion-maximal compatible ready sets deterministically."""

        items = tuple(sorted(ready))
        compatible_sets: list[tuple[int, ...]] = []

        def visit(position: int, chosen: tuple[int, ...], occupied: frozenset[Resource]) -> None:
            if position == len(items):
                compatible_sets.append(chosen)
                return
            item = items[position]
            visit(position + 1, chosen, occupied)
            if not (occupied & self.resources[item]):
                visit(
                    position + 1,
                    (*chosen, item),
                    occupied | self.resources[item],
                )

        visit(0, (), frozenset())
        nonempty = [item for item in compatible_sets if item]
        maximal = [
            item for item in nonempty
            if not any(set(item) < set(other) for other in nonempty)
        ]
        return sorted(set(maximal))

    def greedy_set(self, state: State, policy: str) -> tuple[int, ...]:
        ready = self.ready(state)
        if not ready:
            return ()
        analysis = self.residual.analyze(state)
        loads = residual_resource_loads(self, state)

        def bottleneck(index: int) -> int:
            return max((loads[resource] for resource in self.resources[index]), default=0)

        if policy == "dynamic_tail":
            key = lambda index: (  # noqa: E731
                analysis.tail[index], -self.remaining(state, index), -index,
            )
        elif policy == "resource_tail":
            key = lambda index: (  # noqa: E731
                analysis.tail[index], bottleneck(index),
                len(self.resources[index]), -self.remaining(state, index), -index,
            )
        elif policy == "bottleneck_first":
            key = lambda index: (  # noqa: E731
                bottleneck(index), analysis.tail[index],
                len(self.resources[index]), -index,
            )
        elif policy == "spt":
            key = lambda index: (  # noqa: E731
                -self.remaining(state, index), analysis.tail[index], -index,
            )
        elif policy == "lpt":
            key = lambda index: (  # noqa: E731
                self.remaining(state, index), analysis.tail[index], -index,
            )
        elif policy in {"pp_first", "dp_first"}:
            preferred = policy.split("_", 1)[0].upper()
            key = lambda index: (  # noqa: E731
                self.tasks[index].role.split(":", 1)[0] == preferred,
                analysis.tail[index], -self.remaining(state, index), -index,
            )
        else:
            raise ValueError(f"unknown multi-resource policy: {policy}")
        ranked = sorted(ready, key=key, reverse=True)
        selected: list[int] = []
        occupied: set[Resource] = set()
        for index in ranked:
            if not (occupied & self.resources[index]):
                selected.append(index)
                occupied.update(self.resources[index])
        return tuple(sorted(selected))

    def advance_to_event(
        self, state: State, selected: tuple[int, ...],
    ) -> tuple[State, int]:
        state = self.close(state)
        initial_ready = set(self.ready(state))
        elapsed = 0
        while True:
            active_compute = {
                index for index, task in enumerate(self.tasks)
                if task.kind == "compute" and state[index] > 0
            }
            successor = self.close(self.tick(state, selected))
            elapsed += 1
            completed_flow = any(successor[index] == 0 for index in selected)
            completed_compute = any(successor[index] == 0 for index in active_compute)
            new_ready = set(self.ready(successor)) - initial_ready
            if completed_flow or completed_compute or new_ready:
                return successor, elapsed
            state = successor


def route_resource_sets(
    workload,
    route_table,
    *,
    directed: bool = True,
    task_id_prefix: str = "",
    include_source_nic: bool = False,
    include_destination_nic: bool = False,
) -> dict[str, frozenset[Resource]]:
    """Map workload flows to route-link resource sets.

    Directed links match the executor's full-duplex representation.  Passing
    ``directed=False`` merges the two directions and is useful for a
    half-duplex/shared-medium sensitivity experiment.
    """

    result: dict[str, frozenset[Resource]] = {}
    for task in workload.tasks:
        if not task.is_flow():
            continue
        path = route_table.get_path(task)
        links: list[Resource] = []
        for src, dst in zip(path, path[1:]):
            if directed:
                links.append((src, dst))
            else:
                links.append(tuple(sorted((src, dst))))
        if task.src is not None and include_source_nic:
            links.append(("nic_tx", task.src))
        if task.dst is not None and include_destination_nic:
            links.append(("nic_rx", task.dst))
        result[f"{task_id_prefix}{task.task_id}"] = frozenset(links)
    return result


def residual_resource_loads(model: MultiResourceDAG, state: State) -> dict[Resource, int]:
    loads: dict[Resource, int] = defaultdict(int)
    for index, task in enumerate(model.tasks):
        if task.kind != "comm" or state[index] == 0:
            continue
        amount = model.remaining(state, index)
        for resource in model.resources[index]:
            loads[resource] += amount
    return dict(loads)


def multi_resource_lower_bounds(model: MultiResourceDAG, state: State | None = None) -> dict[str, int]:
    state = model.initial if state is None else model.close(state)
    analysis = model.residual.analyze(state)
    loads = residual_resource_loads(model, state)
    window = _resource_window_bound(model, state, max(
        analysis.critical_path_bound,
        max(loads.values(), default=0),
    ))
    result = {
        "critical_path": analysis.critical_path_bound,
        "max_resource_load": max(loads.values(), default=0),
        "resource_window": window,
    }
    result["combined"] = max(result.values())
    return result


def _resource_window_bound(
    model: MultiResourceDAG,
    state: State,
    base: int,
) -> int:
    """Find the first horizon satisfying every per-resource demand test."""

    analysis = model.residual.analyze(state)
    comms = [
        index for index, task in enumerate(model.tasks)
        if task.kind == "comm" and state[index] != 0
    ]
    if not comms:
        return base
    releases = {
        index: max(
            (
                analysis.earliest_finish[parent]
                for parent in model.deps[index]
                if state[parent] != 0
            ),
            default=0,
        )
        for index in comms
    }

    def feasible(horizon: int) -> bool:
        resources = {item for index in comms for item in model.resources[index]}
        for resource in resources:
            windows = [
                (
                    releases[index],
                    horizon - analysis.tail[index],
                    model.remaining(state, index),
                )
                for index in comms
                if resource in model.resources[index]
            ]
            if any(release + duration > deadline for release, deadline, duration in windows):
                return False
            starts = {0, *(release for release, _deadline, _duration in windows)}
            ends = {horizon, *(deadline for _release, deadline, _duration in windows)}
            for start in starts:
                for end in ends:
                    if end < start:
                        continue
                    demand = sum(
                        duration
                        for release, deadline, duration in windows
                        if release >= start and deadline <= end
                    )
                    if demand > end - start:
                        return False
        return True

    # Executing every unfinished task serially is a finite safe cap.
    serial_horizon = sum(
        model.remaining(state, index)
        for index in range(len(model.tasks))
        if state[index] != 0
    )
    low = base
    high = max(base, serial_horizon)
    while low < high:
        middle = (low + high) // 2
        if feasible(middle):
            high = middle
        else:
            low = middle + 1
    return low


def exact_multiresource_oracle(
    instance: MultiResourceInstance,
    *,
    max_states: int = 2_000_000,
) -> MultiResourceResult:
    """Exact DP over residual states and maximal compatible ready sets."""

    started = perf_counter()
    model = MultiResourceDAG(instance)
    choices: dict[State, tuple[int, ...]] = {}
    explored = 0

    @lru_cache(maxsize=None)
    def solve(raw_state: State) -> int:
        nonlocal explored
        explored += 1
        if explored > max_states:
            raise RuntimeError(f"multi-resource oracle exceeded max_states={max_states}")
        state = model.close(raw_state)
        if _is_finished(state):
            return 0
        ready = model.ready(state)
        active_compute = any(
            task.kind == "compute" and state[index] > 0
            for index, task in enumerate(model.tasks)
        )
        candidates = model.maximal_compatible_sets(ready) if ready else [()]
        if not candidates and not active_compute:
            raise RuntimeError("unfinished DAG has no runnable task")
        best = sys.maxsize
        best_set: tuple[int, ...] = ()
        for selected in candidates:
            value = 1 + solve(model.tick(state, selected))
            if value < best:
                best = value
                best_set = selected
        choices[state] = best_set
        return best

    makespan = solve(model.initial)
    decisions: list[tuple[str, ...]] = []
    state = model.initial
    while not _is_finished(model.close(state)):
        state = model.close(state)
        selected = choices[state]
        decisions.append(tuple(model.order[index] for index in selected))
        state = model.tick(state, selected)
    bounds = multi_resource_lower_bounds(model)
    if bounds["combined"] > makespan:
        raise AssertionError(f"invalid lower bound {bounds} > optimum {makespan}")
    return MultiResourceResult(
        makespan, decisions, sum(not item for item in decisions),
        (perf_counter() - started) * 1000, explored, bounds,
    )


def schedule_multiresource(
    instance: MultiResourceInstance,
    policy: str = "dynamic_tail",
) -> MultiResourceResult:
    model = MultiResourceDAG(instance)
    started = perf_counter()
    state = model.initial
    decisions: list[tuple[str, ...]] = []
    while not _is_finished(model.close(state)):
        state = model.close(state)
        selected = model.greedy_set(state, policy) if model.ready(state) else ()
        decisions.append(tuple(model.order[index] for index in selected))
        state = model.tick(state, selected)
    return MultiResourceResult(
        len(decisions), decisions, sum(not item for item in decisions),
        (perf_counter() - started) * 1000,
    )


def rollout_multiresource(
    instance: MultiResourceInstance,
    *,
    top_k: int = 4,
    base_policy: str = "dynamic_tail",
) -> MultiResourceResult:
    """Next-event counterfactual rollout over compatible communication sets."""

    if top_k < 1:
        raise ValueError("top_k must be positive")
    model = MultiResourceDAG(instance)
    started = perf_counter()
    baseline = schedule_multiresource(instance, base_policy)

    def complete(initial: State) -> int:
        state = model.close(initial)
        elapsed = 0
        while not _is_finished(state):
            selected = model.greedy_set(state, base_policy) if model.ready(state) else ()
            state = model.close(model.tick(state, selected))
            elapsed += 1
        return elapsed

    state = model.initial
    decisions: list[tuple[str, ...]] = []
    while not _is_finished(model.close(state)):
        state = model.close(state)
        ready = model.ready(state)
        if not ready:
            decisions.append(())
            state = model.tick(state, ())
            continue
        analysis = model.residual.analyze(state)
        candidates = model.maximal_compatible_sets(ready)
        candidates.sort(
            key=lambda selected: (
                sum(analysis.tail[index] for index in selected),
                sum(model.remaining(state, index) for index in selected),
                tuple(-index for index in selected),
            ),
            reverse=True,
        )
        base_set = model.greedy_set(state, base_policy)
        shortlisted = candidates[:top_k]
        if base_set not in shortlisted:
            shortlisted.append(base_set)
        evaluated = []
        for selected in shortlisted:
            successor, delta = model.advance_to_event(state, selected)
            evaluated.append((delta + complete(successor), selected, successor, delta))
        _score, selected, successor, delta = min(
            evaluated, key=lambda item: (item[0], item[1]),
        )
        names = tuple(model.order[index] for index in selected)
        decisions.extend(names for _ in range(delta))
        state = successor

    result = MultiResourceResult(
        len(decisions), decisions, sum(not item for item in decisions),
        (perf_counter() - started) * 1000,
    )
    if baseline.makespan < result.makespan:
        baseline.runtime_ms = result.runtime_ms
        return baseline
    return result


def topology_motifs() -> list[MultiResourceInstance]:
    motifs: list[MultiResourceInstance] = []

    builder = _Builder(
        "disjoint_routes", "topology_motif",
        "Two independent PP-like flows use disjoint links and should overlap.",
    )
    left = builder.add("left", "comm", 4, role="pp")
    right = builder.add("right", "comm", 4, role="pp")
    builder.add("left_tail", "compute", 3, (left,))
    builder.add("right_tail", "compute", 3, (right,))
    motifs.append(MultiResourceInstance(
        builder.finish(),
        {"left": frozenset({"uplink-0"}), "right": frozenset({"uplink-1"})},
    ))

    builder = _Builder(
        "shared_uplink", "topology_motif",
        "Endpoint links differ, but both routes cross one shared uplink.",
    )
    left = builder.add("left", "comm", 4, role="dp")
    right = builder.add("right", "comm", 4, role="dp")
    builder.add("left_tail", "compute", 3, (left,))
    builder.add("right_tail", "compute", 3, (right,))
    motifs.append(MultiResourceInstance(
        builder.finish(),
        {
            "left": frozenset({"nic-0", "shared-uplink"}),
            "right": frozenset({"nic-1", "shared-uplink"}),
        },
    ))

    builder = _Builder(
        "pp_dp_partial_overlap", "topology_motif",
        "PP and DP flows partially overlap while a TP-like local flow is disjoint.",
    )
    pp = builder.add("pp", "comm", 3, role="pp")
    dp = builder.add("dp", "comm", 4, role="dp")
    tp = builder.add("tp", "comm", 5, role="tp")
    builder.add("pp_tail", "compute", 5, (pp,))
    builder.add("dp_tail", "compute", 2, (dp,))
    builder.add("tp_tail", "compute", 1, (tp,))
    motifs.append(MultiResourceInstance(
        builder.finish(),
        {
            "pp": frozenset({"nic-0", "fabric-pp"}),
            "dp": frozenset({"nic-0", "fabric-dp"}),
            "tp": frozenset({"nvlink-0"}),
        },
    ))
    return motifs


def random_topology_instance(rng: random.Random, index: int) -> MultiResourceInstance:
    dag = random_join_dag(rng, index)
    resources: dict[str, frozenset[Resource]] = {}
    fabrics = ("pp-fabric", "dp-fabric", "tp-fabric")
    for position, task in enumerate(task for task in dag.tasks if task.kind == "comm"):
        # Every route has an endpoint NIC and usually one role-related fabric.
        # A small shared-uplink probability creates the partial overlaps that
        # distinguish topology-aware set scheduling from one-channel ordering.
        role_index = {"pp": 0, "dp": 1, "tp": 2}.get(task.role, position % 3)
        values: set[Resource] = {f"nic-{rng.randrange(3)}", fabrics[role_index]}
        if rng.random() < 0.35:
            values.add("shared-uplink")
        resources[task.task_id] = frozenset(values)
    return MultiResourceInstance(dag, resources)


def evaluate(*, samples: int, seed: int, max_states: int) -> dict:
    rng = random.Random(seed)
    instances = [*topology_motifs()]
    instances.extend(random_topology_instance(rng, index) for index in range(samples))
    rows = []
    for instance in instances:
        oracle = exact_multiresource_oracle(instance, max_states=max_states)
        collapsed = MultiResourceInstance(
            instance.dag,
            {
                task.task_id: frozenset({"single-channel"})
                for task in instance.dag.tasks if task.kind == "comm"
            },
        )
        collapsed_oracle = exact_multiresource_oracle(
            collapsed, max_states=max_states,
        )
        results = {
            "dynamic_tail_pack": schedule_multiresource(instance, "dynamic_tail"),
            "resource_tail_pack": schedule_multiresource(instance, "resource_tail"),
            "bottleneck_first": schedule_multiresource(instance, "bottleneck_first"),
            "set_rollout_2": rollout_multiresource(instance, top_k=2),
            "set_rollout_4": rollout_multiresource(instance, top_k=4),
        }
        rows.append({
            "name": instance.dag.name,
            "category": instance.dag.category,
            "tasks": len(instance.dag.tasks),
            "resources": len(set().union(*instance.resources.values())),
            "optimum": oracle.makespan,
            "single_channel_optimum": collapsed_oracle.makespan,
            "single_channel_overestimate": collapsed_oracle.makespan / oracle.makespan,
            "oracle_states": oracle.explored_states,
            "lower_bounds": oracle.lower_bounds,
            "methods": {
                name: {
                    "makespan": result.makespan,
                    "ratio": result.makespan / oracle.makespan,
                    "runtime_ms": result.runtime_ms,
                }
                for name, result in results.items()
            },
        })
    methods = next(iter(rows))["methods"] if rows else {}
    summary = {}
    for method in methods:
        ratios = [row["methods"][method]["ratio"] for row in rows]
        summary[method] = {
            "mean_ratio": mean(ratios),
            "observed_max_ratio": max(ratios),
            "optimal_fraction": sum(value == 1 for value in ratios) / len(ratios),
            "mean_runtime_ms": mean(row["methods"][method]["runtime_ms"] for row in rows),
        }
    hard = [
        row for row in rows
        if row["methods"]["dynamic_tail_pack"]["makespan"] > row["optimum"]
    ]
    hard_summary = {}
    for method in methods:
        if not hard:
            break
        gaps_closed = []
        repairs = 0
        for row in hard:
            baseline = row["methods"]["dynamic_tail_pack"]["makespan"]
            value = row["methods"][method]["makespan"]
            optimum = row["optimum"]
            gaps_closed.append((baseline - value) / (baseline - optimum))
            repairs += value == optimum
        hard_summary[method] = {
            "repairs_to_exact": repairs,
            "mean_gap_closed": mean(gaps_closed),
        }
    return {
        "config": {"samples": samples, "seed": seed, "max_states": max_states},
        "model": {
            "communication": "preemptive integral flows requiring exclusive route-resource sets",
            "compute": "immediate parallel progress; fixed resource order already in DAG",
            "lower_bound": "max(residual critical path, max per-resource load)",
        },
        "summary": summary,
        "topology_concurrency": {
            "mean_single_channel_overestimate": mean(
                row["single_channel_overestimate"] for row in rows
            ),
            "observed_max_single_channel_overestimate": max(
                row["single_channel_overestimate"] for row in rows
            ),
        },
        "hard_subset": {
            "definition": "dynamic_tail_pack > exact multi-resource optimum",
            "count": len(hard),
            "methods": hard_summary,
        },
        "instances": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=30)
    parser.add_argument("--seed", type=int, default=260819)
    parser.add_argument("--max-states", type=int, default=500_000)
    parser.add_argument(
        "--output", type=Path,
        default=ROOT / "outputs" / "multiresource_dag" / "report.json",
    )
    args = parser.parse_args()
    report = evaluate(samples=args.samples, seed=args.seed, max_states=args.max_states)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report["summary"], indent=2))
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
