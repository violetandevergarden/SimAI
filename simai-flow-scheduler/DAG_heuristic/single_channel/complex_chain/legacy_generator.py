"""General-DAG fixtures and helper policies used by the migrated algorithms.

The public algorithms run with the non-preemptive state model.  Some historical
comparison helpers remain in this module for fixture compatibility and are not
registered as current algorithms.

Implemented policies
--------------------
* residual dynamic-tail;
* join-gate-adjusted dynamic-tail;
* top-k event rollout with a gate-aware base policy;
* bounded local event beam with an always-available gate-aware incumbent.

"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
import json
from pathlib import Path
import random
from statistics import mean
import sys
from time import perf_counter
from typing import Callable


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from DAG_heuristic.common.benchmark import (  # noqa: E402
    BenchmarkDAG,
    _Builder,
    _compute_closure,
    _indexed,
    _is_finished,
    _ready_comms,
    _tick,
    adversarial_benchmarks,
    exact_oracle,
    heuristic_schedule,
    llm_motif_benchmarks,
    reduce_effective_dag,
)


State = tuple[int, ...]


@dataclass(frozen=True)
class ResidualAnalysis:
    tail: tuple[int, ...]
    gate_tail: tuple[int, ...]
    gate_gain: tuple[int, ...]
    earliest_finish: tuple[int, ...]
    communication_bound: int
    critical_path_bound: int

    @property
    def lower_bound(self) -> int:
        return max(self.communication_bound, self.critical_path_bound)


@dataclass
class DAGScheduleResult:
    makespan: int
    decisions: list[str | None]
    network_idle: int
    preemptions: int
    runtime_ms: float


Selector = Callable[[State, list[int]], int]


class ResidualDAG:
    """Indexed DAG plus residual critical-path and join-gating calculations."""

    def __init__(self, dag: BenchmarkDAG):
        self.dag = dag
        self.order, self.tasks, self.deps = _indexed(dag)
        self.children: tuple[tuple[int, ...], ...] = self._children()
        self.initial = self.close(tuple(-1 for _ in self.tasks))

    def _children(self) -> tuple[tuple[int, ...], ...]:
        values: dict[int, list[int]] = defaultdict(list)
        for child, parents in enumerate(self.deps):
            for parent in parents:
                values[parent].append(child)
        return tuple(tuple(values[index]) for index in range(len(self.tasks)))

    def close(self, state: State) -> State:
        return _compute_closure(self.tasks, self.deps, state)

    def ready(self, state: State) -> list[int]:
        return _ready_comms(self.tasks, self.deps, state)

    def tick(self, state: State, selected: int | None) -> State:
        return self.close(_tick(self.tasks, self.deps, state, selected))

    def remaining(self, state: State, index: int) -> int:
        value = state[index]
        return value if value > 0 else self.tasks[index].duration

    def analyze(self, raw_state: State) -> ResidualAnalysis:
        state = self.close(raw_state)
        count = len(self.tasks)

        # Optimistic residual earliest finishes ignore channel contention but
        # preserve all unfinished precedence constraints.
        earliest = [0] * count
        for index, task in enumerate(self.tasks):
            if state[index] == 0:
                continue
            own = self.remaining(state, index)
            predecessor_finish = max(
                (earliest[parent] for parent in self.deps[index] if state[parent] != 0),
                default=0,
            )
            earliest[index] = own + predecessor_finish

        tail = [0] * count
        for index in reversed(range(count)):
            if state[index] == 0:
                continue
            tail[index] = max(
                (
                    self.remaining(state, child) + tail[child]
                    for child in self.children[index]
                    if state[child] != 0
                ),
                default=0,
            )

        # Join urgency follows the stage-3 plan's last-blocker definition:
        # g(v, x) = max(0, EF(v) - max_{u != v} EF(u)).  It is an additive
        # urgency signal; it never discounts or rewrites the residual critical
        # path, because optimistic EF can be inaccurate under contention.
        gate_tail = list(tail)
        gate_gain = [0] * count
        for index, task in enumerate(self.tasks):
            if task.kind != "comm" or state[index] == 0:
                continue
            own_arrival = self.remaining(state, index)
            for child in self.children[index]:
                if state[child] == 0:
                    continue
                unfinished_others = [
                    parent
                    for parent in self.deps[child]
                    if parent != index and state[parent] != 0
                ]
                if len(self.deps[child]) > 1 and unfinished_others:
                    other_arrival = max(earliest[parent] for parent in unfinished_others)
                elif len(self.deps[child]) > 1:
                    other_arrival = 0
                else:
                    continue
                gate_gain[index] = max(
                    gate_gain[index], max(0, own_arrival - other_arrival),
                )
            gate_tail[index] = tail[index] + gate_gain[index]

        communication = sum(
            self.remaining(state, index)
            for index, task in enumerate(self.tasks)
            if task.kind == "comm" and state[index] != 0
        )
        critical_path = max(earliest, default=0)
        return ResidualAnalysis(
            tuple(tail), tuple(gate_tail), tuple(gate_gain), tuple(earliest),
            communication, critical_path,
        )

    def select_dynamic_tail(self, state: State, ready: list[int]) -> int:
        analysis = self.analyze(state)
        return max(
            ready,
            key=lambda index: (
                analysis.tail[index],
                analysis.gate_gain[index],
                -self.remaining(state, index),
                -index,
            ),
        )

    def select_gate_tail(self, state: State, ready: list[int]) -> int:
        analysis = self.analyze(state)
        return max(
            ready,
            key=lambda index: (
                analysis.gate_tail[index],
                analysis.gate_gain[index],
                analysis.tail[index],
                -self.remaining(state, index),
                -index,
            ),
        )

    def advance_to_event(self, state: State, selected: int) -> tuple[State, int]:
        """Run one communication until completion or the next compute event."""

        state = self.close(state)
        initial_ready = set(self.ready(state))
        elapsed = 0
        while True:
            active_compute = {
                index
                for index, task in enumerate(self.tasks)
                if task.kind == "compute" and state[index] > 0
            }
            successor = self.tick(state, selected)
            elapsed += 1
            completed_compute = any(
                successor[index] == 0 for index in active_compute
            )
            new_ready = set(self.ready(successor)) - initial_ready
            if successor[selected] == 0 or completed_compute or new_ready:
                return successor, elapsed
            state = successor

    def advance_action(
        self,
        state: State,
        selected: int,
        horizon: str,
    ) -> tuple[State, int]:
        """Advance one counterfactual action under a declared commitment."""

        if horizon == "tick":
            return self.tick(state, selected), 1
        if horizon == "event":
            return self.advance_to_event(state, selected)
        if horizon == "flow_complete":
            state = self.close(state)
            elapsed = 0
            while state[selected] != 0:
                state = self.tick(state, selected)
                elapsed += 1
            return state, elapsed
        raise ValueError(f"unknown rollout horizon: {horizon}")


def _simulate_from_state(
    model: ResidualDAG,
    initial: State,
    selector: Selector,
    *,
    collect_decisions: bool,
) -> DAGScheduleResult:
    started = perf_counter()
    state = model.close(initial)
    decisions: list[str | None] = []
    idle = 0
    preemptions = 0
    previous: int | None = None
    elapsed = 0
    while not _is_finished(state):
        ready = model.ready(state)
        selected = selector(state, ready) if ready else None
        if selected is None:
            idle += 1
        elif (
            previous is not None
            and previous != selected
            and state[previous] > 0
            and model.tasks[previous].kind == "comm"
        ):
            preemptions += 1
        if collect_decisions:
            decisions.append(model.order[selected] if selected is not None else None)
        state = model.tick(state, selected)
        previous = selected
        elapsed += 1
    return DAGScheduleResult(
        elapsed, decisions, idle, preemptions, (perf_counter() - started) * 1000,
    )


def schedule_policy(dag: BenchmarkDAG, policy: str) -> DAGScheduleResult:
    model = ResidualDAG(dag)
    if policy == "dynamic_tail":
        selector = model.select_dynamic_tail
    elif policy == "gate_dynamic_tail":
        selector = model.select_gate_tail
    else:
        started = perf_counter()
        makespan, decisions = heuristic_schedule(dag, policy)
        return DAGScheduleResult(
            makespan, decisions, decisions.count(None), 0,
            (perf_counter() - started) * 1000,
        )
    return _simulate_from_state(model, model.initial, selector, collect_decisions=True)


def rollout_schedule(
    dag: BenchmarkDAG,
    *,
    top_k: int = 4,
    base_policy: str = "dynamic_tail",
    candidate_policy: str = "hybrid",
    horizon: str = "event",
) -> DAGScheduleResult:
    """Top-k event rollout; the candidate set always contains the base action.

    Candidate quality is the complete base-policy upper bound and the selected
    communication is committed until the event used in that evaluation.  A
    separately retained full baseline schedule is the deterministic-profile
    safeguard.
    """

    if top_k < 1:
        raise ValueError("top_k must be positive")
    if candidate_policy not in {"dynamic_tail", "join", "hybrid"}:
        raise ValueError(f"unknown candidate policy: {candidate_policy}")
    if horizon not in {"tick", "event", "flow_complete"}:
        raise ValueError(f"unknown rollout horizon: {horizon}")
    model = ResidualDAG(dag)
    if base_policy == "gate_dynamic_tail":
        base = model.select_gate_tail
    elif base_policy == "dynamic_tail":
        base = model.select_dynamic_tail
    else:
        raise ValueError(f"unsupported rollout base: {base_policy}")

    started = perf_counter()
    baseline = _simulate_from_state(
        model, model.initial, base, collect_decisions=True,
    )
    state = model.initial
    decisions: list[str | None] = []
    idle = 0
    preemptions = 0
    previous: int | None = None
    while not _is_finished(state):
        ready = model.ready(state)
        if not ready:
            decisions.append(None)
            state = model.tick(state, None)
            idle += 1
            previous = None
            continue

        analysis = model.analyze(state)
        dynamic_ranked = sorted(
            ready,
            key=lambda index: (
                analysis.tail[index], analysis.gate_gain[index],
                -model.remaining(state, index), -index,
            ),
            reverse=True,
        )
        join_ranked = sorted(
            ready,
            key=lambda index: (
                analysis.gate_gain[index], analysis.tail[index],
                -model.remaining(state, index), -index,
            ),
            reverse=True,
        )
        if candidate_policy == "dynamic_tail":
            candidates = dynamic_ranked[:top_k]
        elif candidate_policy == "join":
            candidates = join_ranked[:top_k]
        else:
            dynamic_quota = (top_k + 1) // 2
            candidates = dynamic_ranked[:dynamic_quota]
            candidates.extend(
                index for index in join_ranked
                if index not in candidates
            )
            candidates = candidates[:top_k]

        evaluated = []
        for candidate in candidates:
            successor, delta = model.advance_action(state, candidate, horizon)
            continuation = _simulate_from_state(
                model, successor, base, collect_decisions=False,
            ).makespan
            residual_lb = model.analyze(successor).lower_bound
            evaluated.append((
                delta + continuation,
                delta + residual_lb,
                -analysis.gate_tail[candidate],
                candidate,
            ))
        selected = min(evaluated)[-1]
        if (
            previous is not None and previous != selected
            and state[previous] > 0 and model.tasks[previous].kind == "comm"
        ):
            preemptions += 1
        state, delta = model.advance_action(state, selected, horizon)
        decisions.extend(model.order[selected] for _ in range(delta))
        previous = selected

    result = DAGScheduleResult(
        len(decisions), decisions, idle, preemptions,
        (perf_counter() - started) * 1000,
    )
    if baseline.makespan < result.makespan:
        baseline.runtime_ms = result.runtime_ms
        return baseline
    return result


def local_beam_schedule(
    dag: BenchmarkDAG,
    *,
    width: int = 8,
    event_depth: int = 3,
) -> DAGScheduleResult:
    """Receding-horizon event beam with a gate-aware incumbent safeguard."""

    if width < 1 or event_depth < 1:
        raise ValueError("width and event_depth must be positive")
    model = ResidualDAG(dag)
    base = model.select_dynamic_tail
    started = perf_counter()
    baseline = _simulate_from_state(
        model, model.initial, base, collect_decisions=True,
    )
    state = model.initial
    decisions: list[str | None] = []
    idle = 0
    while not _is_finished(state):
        ready = model.ready(state)
        if not ready:
            decisions.append(None)
            state = model.tick(state, None)
            idle += 1
            continue

        # item = (state, elapsed macro time, first action)
        frontier: dict[State, tuple[int, int]] = {}
        for candidate in ready:
            successor, delta = model.advance_to_event(state, candidate)
            frontier.setdefault(successor, (delta, candidate))
        for _depth in range(1, event_depth):
            expanded: dict[State, tuple[int, int]] = {}
            for item_state, (elapsed, first) in frontier.items():
                item_ready = model.ready(item_state)
                if not item_ready:
                    successor = model.tick(item_state, None)
                    expanded.setdefault(successor, (elapsed + 1, first))
                    continue
                for candidate in item_ready:
                    successor, delta = model.advance_to_event(item_state, candidate)
                    current = expanded.get(successor)
                    value = (elapsed + delta, first)
                    if current is None or value < current:
                        expanded[successor] = value
            ranked = sorted(
                expanded.items(),
                key=lambda item: (
                    item[1][0] + model.analyze(item[0]).lower_bound,
                    item[1][0] + _simulate_from_state(
                        model, item[0], base, collect_decisions=False,
                    ).makespan,
                    item[1][1],
                ),
            )[:width]
            frontier = dict(ranked)

        # Compare complete base-policy upper bounds and explicitly include its
        # first action, so beam pruning cannot make the returned schedule worse
        # than the gate-aware incumbent at this decision.
        base_action = base(state, ready)
        choices: list[tuple[int, int]] = []
        for item_state, (elapsed, first) in frontier.items():
            total = elapsed + _simulate_from_state(
                model, item_state, base, collect_decisions=False,
            ).makespan
            choices.append((total, first))
        base_successor = model.tick(state, base_action)
        base_total = 1 + _simulate_from_state(
            model, base_successor, base, collect_decisions=False,
        ).makespan
        choices.append((base_total, base_action))
        selected = min(choices)[1]
        decisions.append(model.order[selected])
        state = model.tick(state, selected)

    result = DAGScheduleResult(
        len(decisions), decisions, idle, 0, (perf_counter() - started) * 1000,
    )
    if baseline.makespan < result.makespan:
        baseline.runtime_ms = result.runtime_ms
        return baseline
    return result


def random_join_dag(rng: random.Random, index: int) -> BenchmarkDAG:
    """Generate a small fork/join DAG that remains exact-oracle friendly."""

    builder = _Builder(
        f"random_join_{index}", "random_general",
        "Random small DAG with branch releases, second flows and a final join.",
    )
    endpoints: list[str] = []
    branches = rng.randint(2, 5)
    for branch in range(branches):
        release = builder.add(
            f"r{branch}", "compute", rng.randint(0, 3), role="release",
        )
        first = builder.add(
            f"c{branch}_0", "comm", rng.randint(1, 4), (release,),
            role=rng.choice(("pp", "tp", "dp")),
        )
        compute = builder.add(
            f"x{branch}_0", "compute", rng.randint(1, 6), (first,),
            role="backbone" if branch == 0 else "side",
        )
        if rng.random() < 0.7:
            second = builder.add(
                f"c{branch}_1", "comm", rng.randint(1, 4), (compute,),
                role=rng.choice(("pp", "dp")),
            )
            endpoints.append(second)
        else:
            endpoints.append(compute)

    # Sometimes create a nested join so dynamic gating is exercised at more
    # than one level without making the oracle state space too large.
    if branches >= 3 and rng.random() < 0.6:
        nested = builder.add(
            "nested_join", "compute", rng.randint(1, 3), tuple(endpoints[:2]),
            role="join",
        )
        endpoints = [nested, *endpoints[2:]]
    join = builder.add(
        "optimizer_join", "compute", rng.randint(1, 3), tuple(endpoints),
        role="optimizer",
    )
    if rng.random() < 0.7:
        final = builder.add(
            "final_comm", "comm", rng.randint(1, 3), (join,), role="pp",
        )
        builder.add("sink", "compute", rng.randint(1, 4), (final,), role="sink")
    return builder.finish(branches=branches)


def last_blocker_overboost_counterexample() -> BenchmarkDAG:
    """A fixed case where raw last-blocker urgency hurts dynamic tail by one."""

    builder = _Builder(
        "last_blocker_overboost", "adversarial",
        "Directly adding join last-blocker urgency over-prioritizes a long flow.",
    )
    r0 = builder.add("r0", "compute", 3)
    c00 = builder.add("c0_0", "comm", 3, (r0,))
    x00 = builder.add("x0_0", "compute", 4, (c00,))
    c01 = builder.add("c0_1", "comm", 1, (x00,))
    r1 = builder.add("r1", "compute", 1)
    c10 = builder.add("c1_0", "comm", 3, (r1,))
    x10 = builder.add("x1_0", "compute", 1, (c10,))
    r2 = builder.add("r2", "compute", 2)
    c20 = builder.add("c2_0", "comm", 4, (r2,))
    x20 = builder.add("x2_0", "compute", 2, (c20,))
    c21 = builder.add("c2_1", "comm", 1, (x20,))
    nested = builder.add("nested_join", "compute", 2, (c01, x10), role="join")
    optimizer = builder.add(
        "optimizer_join", "compute", 1, (nested, c21), role="optimizer",
    )
    final = builder.add("final_comm", "comm", 1, (optimizer,))
    builder.add("sink", "compute", 4, (final,))
    return builder.finish()


def benchmark_suite(samples: int, seed: int) -> tuple[list[BenchmarkDAG], int]:
    dags = [
        *adversarial_benchmarks(), last_blocker_overboost_counterexample(),
        *llm_motif_benchmarks(),
    ]
    effective_path = ROOT / "outputs" / "dag_audit" / "1f1b" / "effective_dag.json"
    if effective_path.exists():
        graph = json.loads(effective_path.read_text(encoding="utf-8"))
        for bucket_rank in range(8):
            reduced = reduce_effective_dag(graph, bucket_rank=bucket_rank)
            try:
                exact_oracle(reduced, max_states=250_000)
            except RuntimeError:
                continue
            dags.append(reduced)
    rng = random.Random(seed)
    random_count = 0
    attempts = 0
    while random_count < samples and attempts < samples * 5:
        attempts += 1
        dag = random_join_dag(rng, random_count)
        try:
            exact_oracle(dag, max_states=250_000)
        except RuntimeError:
            continue
        dags.append(dag)
        random_count += 1
    return dags, random_count


def evaluate(samples: int, seed: int) -> dict:
    dags, random_count = benchmark_suite(samples, seed)
    methods = (
        "longest_tail", "gate_aware", "dynamic_tail", "gate_dynamic_tail",
        "rollout2", "rollout4", "rollout8", "beam8",
    )
    rows = []
    aggregate: dict[str, list[float]] = defaultdict(list)
    optimal_count: dict[str, int] = defaultdict(int)
    worst: dict[str, dict] = {}
    for dag in dags:
        oracle = exact_oracle(dag, max_states=500_000)
        results = {
            "longest_tail": schedule_policy(dag, "longest_tail"),
            "gate_aware": schedule_policy(dag, "gate_aware"),
            "dynamic_tail": schedule_policy(dag, "dynamic_tail"),
            "gate_dynamic_tail": schedule_policy(dag, "gate_dynamic_tail"),
            "rollout2": rollout_schedule(dag, top_k=2),
            "rollout4": rollout_schedule(dag, top_k=4),
            "rollout8": rollout_schedule(dag, top_k=8),
            "beam8": local_beam_schedule(dag, width=8, event_depth=3),
        }
        values = {}
        for method in methods:
            result = results[method]
            ratio = result.makespan / oracle.makespan
            aggregate[method].append(ratio)
            optimal_count[method] += result.makespan == oracle.makespan
            values[method] = {
                "makespan": result.makespan,
                "ratio": ratio,
                "runtime_ms": result.runtime_ms,
                "idle": result.network_idle,
            }
            if method not in worst or ratio > worst[method]["ratio"]:
                worst[method] = {
                    "ratio": ratio,
                    "dag": dag.name,
                    "category": dag.category,
                    "makespan": result.makespan,
                    "optimum": oracle.makespan,
                }
        rows.append({
            "name": dag.name,
            "category": dag.category,
            "tasks": len(dag.tasks),
            "optimum": oracle.makespan,
            "oracle_states": oracle.explored_states,
            "methods": values,
        })

    summary = {
        method: {
            "mean_ratio": mean(aggregate[method]),
            "max_ratio": max(aggregate[method]),
            "optimal_fraction": optimal_count[method] / len(dags),
            "mean_runtime_ms": mean(
                row["methods"][method]["runtime_ms"] for row in rows
            ),
            "worst": worst[method],
        }
        for method in methods
    }
    return {
        "model": "single preemptive communication bottleneck + parallel compute",
        "seed": seed,
        "fixed_benchmarks": len(dags) - random_count,
        "random_benchmarks": random_count,
        "instances": len(dags),
        "summary": summary,
        "rows": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--seed", type=int, default=260817)
    parser.add_argument(
        "--output", type=Path,
        default=ROOT / "outputs" / "general_dag_heuristics" / "report.json",
    )
    args = parser.parse_args()
    if args.samples < 1:
        parser.error("--samples must be positive")
    report = evaluate(args.samples, args.seed)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({
        "instances": report["instances"],
        "summary": report["summary"],
        "output": str(args.output),
    }, indent=2))


if __name__ == "__main__":
    main()
