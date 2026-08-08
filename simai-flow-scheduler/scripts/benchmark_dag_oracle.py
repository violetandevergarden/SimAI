"""Reproducible small-DAG benchmarks and an exact scheduling oracle.

Model
-----
* ``comm`` nodes share one unit-capacity, preemptive channel;
* ``compute`` nodes start immediately when ready and progress in parallel;
* all durations are positive integer quanta;
* precedence edges already include fixed compute-resource order.

This is deliberately the single-bottleneck research model used by the
heuristic plan.  It is small-instance infrastructure, not a replacement for
the topology-aware simulator.

Examples::

    python scripts/benchmark_dag_oracle.py
    python scripts/benchmark_dag_oracle.py --search-samples 500
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from functools import lru_cache
import json
from pathlib import Path
import random
from statistics import mean
import sys
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@dataclass(frozen=True)
class BenchTask:
    task_id: str
    kind: str
    duration: int
    deps: tuple[str, ...] = ()
    role: str = ""
    cut: str = ""

    def __post_init__(self) -> None:
        if self.kind not in {"compute", "comm"}:
            raise ValueError(f"unknown task kind: {self.kind}")
        if self.duration < 0:
            raise ValueError("task duration must be non-negative")


@dataclass(frozen=True)
class BenchmarkDAG:
    name: str
    category: str
    tasks: tuple[BenchTask, ...]
    description: str = ""
    parameters: tuple[tuple[str, str], ...] = ()

    def task_map(self) -> dict[str, BenchTask]:
        return {task.task_id: task for task in self.tasks}

    def validate(self) -> list[str]:
        errors: list[str] = []
        ids = [task.task_id for task in self.tasks]
        if len(ids) != len(set(ids)):
            errors.append("duplicate task id")
        known = set(ids)
        for task in self.tasks:
            missing = set(task.deps) - known
            if missing:
                errors.append(f"{task.task_id}: unknown deps {sorted(missing)}")
        if errors:
            return errors
        try:
            topological_order(self)
        except ValueError as error:
            errors.append(str(error))
        return errors


@dataclass
class OracleResult:
    makespan: int
    decisions: list[str | None]
    explored_states: int
    lower_bounds: dict[str, int]


@dataclass
class _Builder:
    name: str
    category: str
    description: str
    tasks: list[BenchTask] = field(default_factory=list)

    def add(
        self,
        task_id: str,
        kind: str,
        duration: int,
        deps: Iterable[str] = (),
        *,
        role: str = "",
        cut: str = "",
    ) -> str:
        self.tasks.append(BenchTask(task_id, kind, duration, tuple(deps), role, cut))
        return task_id

    def finish(self, **parameters: object) -> BenchmarkDAG:
        dag = BenchmarkDAG(
            self.name,
            self.category,
            tuple(self.tasks),
            self.description,
            tuple(sorted((key, str(value)) for key, value in parameters.items())),
        )
        errors = dag.validate()
        if errors:
            raise ValueError(f"invalid generated DAG {dag.name}: {errors}")
        return dag


def topological_order(dag: BenchmarkDAG) -> list[str]:
    tasks = dag.task_map()
    children: dict[str, list[str]] = {task_id: [] for task_id in tasks}
    degree = {task_id: len(task.deps) for task_id, task in tasks.items()}
    for task in tasks.values():
        for dependency in task.deps:
            children[dependency].append(task.task_id)
    ready = sorted(task_id for task_id, value in degree.items() if value == 0)
    order: list[str] = []
    while ready:
        task_id = ready.pop(0)
        order.append(task_id)
        for child in children[task_id]:
            degree[child] -= 1
            if degree[child] == 0:
                ready.append(child)
        ready.sort()
    if len(order) != len(tasks):
        raise ValueError("benchmark graph contains a cycle")
    return order


def _indexed(dag: BenchmarkDAG):
    order = topological_order(dag)
    tasks = dag.task_map()
    index = {task_id: position for position, task_id in enumerate(order)}
    deps = tuple(tuple(index[item] for item in tasks[task_id].deps) for task_id in order)
    return order, tuple(tasks[task_id] for task_id in order), deps


# State value: -1=pending, 0=complete, >0=remaining and active/started.
State = tuple[int, ...]


def _compute_closure(tasks: tuple[BenchTask, ...], deps, state: State) -> State:
    values = list(state)
    changed = True
    while changed:
        changed = False
        for index, task in enumerate(tasks):
            if task.kind != "compute" or values[index] != -1:
                continue
            if all(values[parent] == 0 for parent in deps[index]):
                values[index] = task.duration
                changed = True
    return tuple(values)


def _ready_comms(tasks: tuple[BenchTask, ...], deps, state: State) -> list[int]:
    return [
        index
        for index, task in enumerate(tasks)
        if task.kind == "comm"
        and state[index] != 0
        and (state[index] > 0 or all(state[parent] == 0 for parent in deps[index]))
    ]


def _tick(
    tasks: tuple[BenchTask, ...],
    deps,
    state: State,
    selected: int | None,
) -> State:
    values = list(state)
    for index, task in enumerate(tasks):
        if task.kind == "compute" and values[index] > 0:
            values[index] -= 1
    if selected is not None:
        remaining = values[selected]
        if remaining == -1:
            remaining = tasks[selected].duration
        values[selected] = remaining - 1
    return tuple(values)


def _is_finished(state: State) -> bool:
    return all(value == 0 for value in state)


def _tail_lengths(dag: BenchmarkDAG) -> dict[str, int]:
    order = topological_order(dag)
    tasks = dag.task_map()
    children: dict[str, list[str]] = {task_id: [] for task_id in order}
    for task in tasks.values():
        for dependency in task.deps:
            children[dependency].append(task.task_id)
    tail: dict[str, int] = {}
    for task_id in reversed(order):
        tail[task_id] = max(
            (tasks[child].duration + tail[child] for child in children[task_id]),
            default=0,
        )
    return tail


def lower_bounds(dag: BenchmarkDAG) -> dict[str, int]:
    """Return P, Q, L, window, cut and their maximum.

    ``window`` is the smallest horizon satisfying all necessary preemptive
    release/deadline demand inequalities derived from precedence-only earliest
    starts and downstream tails.  ``cut`` is the largest explicitly labelled
    communication-cut load; under one channel it is intentionally dominated
    by P, but remains useful when the same benchmark is lifted to multi-link
    models.
    """

    order = topological_order(dag)
    tasks = dag.task_map()
    children: dict[str, list[str]] = {task_id: [] for task_id in order}
    for task in tasks.values():
        for dependency in task.deps:
            children[dependency].append(task.task_id)

    earliest_finish: dict[str, int] = {}
    pure_compute: dict[str, int] = {}
    for task_id in order:
        task = tasks[task_id]
        pred_finish = max((earliest_finish[item] for item in task.deps), default=0)
        pred_compute = max((pure_compute[item] for item in task.deps), default=0)
        earliest_finish[task_id] = pred_finish + task.duration
        pure_compute[task_id] = pred_compute + (
            task.duration if task.kind == "compute" else 0
        )

    downstream: dict[str, int] = {}
    for task_id in reversed(order):
        downstream[task_id] = max(
            (tasks[child].duration + downstream[child] for child in children[task_id]),
            default=0,
        )

    comms = [task for task in tasks.values() if task.kind == "comm"]
    p_bound = sum(task.duration for task in comms)
    q_bound = max(pure_compute.values(), default=0)
    l_bound = max(earliest_finish.values(), default=0)
    cut_loads: Counter[str] = Counter()
    for task in comms:
        if task.cut:
            cut_loads[task.cut] += task.duration
    cut_bound = max(cut_loads.values(), default=0)

    base = max(p_bound, q_bound, l_bound, cut_bound)
    releases = {
        task.task_id: max((earliest_finish[item] for item in task.deps), default=0)
        for task in comms
    }

    def demand_feasible(horizon: int) -> bool:
        windows = [
            (releases[task.task_id], horizon - downstream[task.task_id], task.duration)
            for task in comms
        ]
        if any(release + duration > deadline for release, deadline, duration in windows):
            return False
        endpoints_a = {0, *(release for release, _deadline, _duration in windows)}
        endpoints_b = {horizon, *(deadline for _release, deadline, _duration in windows)}
        for start in endpoints_a:
            for end in endpoints_b:
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

    serial_horizon = sum(task.duration for task in tasks.values())
    window_bound = base
    while window_bound <= serial_horizon and not demand_feasible(window_bound):
        window_bound += 1
    if window_bound > serial_horizon:
        window_bound = serial_horizon
    result = {
        "P": p_bound,
        "Q": q_bound,
        "L": l_bound,
        "window": window_bound,
        "cut": cut_bound,
    }
    result["combined"] = max(result.values())
    return result


def exact_oracle(dag: BenchmarkDAG, *, max_states: int = 2_000_000) -> OracleResult:
    """Solve a small benchmark exactly by memoized ready-flow enumeration."""

    errors = dag.validate()
    if errors:
        raise ValueError(errors)
    order, tasks, deps = _indexed(dag)
    initial = _compute_closure(tasks, deps, tuple(-1 for _ in tasks))
    choices: dict[State, int | None] = {}
    explored = 0

    @lru_cache(maxsize=None)
    def solve(raw_state: State) -> int:
        nonlocal explored
        explored += 1
        if explored > max_states:
            raise RuntimeError(f"exact oracle exceeded max_states={max_states}")
        state = _compute_closure(tasks, deps, raw_state)
        if _is_finished(state):
            return 0
        ready = _ready_comms(tasks, deps, state)
        active_compute = any(
            task.kind == "compute" and state[index] > 0
            for index, task in enumerate(tasks)
        )
        candidates: list[int | None] = ready if ready else [None]
        if not ready and not active_compute:
            raise RuntimeError("unfinished DAG has neither communication nor active compute")
        best = sys.maxsize
        best_choice: int | None = None
        for selected in candidates:
            value = 1 + solve(_tick(tasks, deps, state, selected))
            if value < best:
                best = value
                best_choice = selected
        choices[state] = best_choice
        return best

    makespan = solve(initial)
    decisions: list[str | None] = []
    state = initial
    while not _is_finished(_compute_closure(tasks, deps, state)):
        state = _compute_closure(tasks, deps, state)
        selected = choices[state]
        decisions.append(order[selected] if selected is not None else None)
        state = _tick(tasks, deps, state, selected)
    bounds = lower_bounds(dag)
    if bounds["combined"] > makespan:
        raise AssertionError(f"invalid lower bound {bounds} > optimum {makespan}")
    return OracleResult(makespan, decisions, explored, bounds)


def branch_and_bound_oracle(
    dag: BenchmarkDAG,
    *,
    max_states: int = 2_000_000,
) -> OracleResult:
    """Independent exact DFS with an incumbent and residual-work pruning."""

    order, tasks, deps = _indexed(dag)
    initial = _compute_closure(tasks, deps, tuple(-1 for _ in tasks))
    incumbents = [(*heuristic_schedule(dag, policy), policy) for policy in POLICIES]
    best_time, best_path, _policy = min(incumbents, key=lambda item: item[0])
    seen_elapsed: dict[State, int] = {}
    explored = 0

    def residual_bound(state: State) -> int:
        communication = sum(
            (value if value > 0 else task.duration)
            for task, value in zip(tasks, state)
            if task.kind == "comm" and value != 0
        )
        active_compute = max(
            (
                value
                for task, value in zip(tasks, state)
                if task.kind == "compute" and value > 0
            ),
            default=0,
        )
        return max(communication, active_compute)

    def visit(raw_state: State, elapsed: int, path: list[str | None]) -> None:
        nonlocal best_time, best_path, explored
        state = _compute_closure(tasks, deps, raw_state)
        explored += 1
        if explored > max_states:
            raise RuntimeError(f"branch-and-bound exceeded max_states={max_states}")
        if _is_finished(state):
            if elapsed < best_time:
                best_time = elapsed
                best_path = list(path)
            return
        if elapsed + residual_bound(state) >= best_time:
            return
        if seen_elapsed.get(state, sys.maxsize) <= elapsed:
            return
        seen_elapsed[state] = elapsed
        ready = _ready_comms(tasks, deps, state)
        candidates: list[int | None] = ready if ready else [None]
        for selected in candidates:
            visit(
                _tick(tasks, deps, state, selected),
                elapsed + 1,
                [*path, order[selected] if selected is not None else None],
            )

    visit(initial, 0, [])
    bounds = lower_bounds(dag)
    if bounds["combined"] > best_time:
        raise AssertionError(f"invalid lower bound {bounds} > optimum {best_time}")
    return OracleResult(best_time, best_path, explored, bounds)


POLICIES = ("fifo", "spt", "lpt", "longest_tail", "lrpt", "gate_aware")


def heuristic_schedule(dag: BenchmarkDAG, policy: str) -> tuple[int, list[str | None]]:
    if policy not in POLICIES:
        raise ValueError(f"unknown policy: {policy}")
    order, tasks, deps = _indexed(dag)
    state = _compute_closure(tasks, deps, tuple(-1 for _ in tasks))
    tail = _tail_lengths(dag)
    children: dict[int, list[int]] = {index: [] for index in range(len(tasks))}
    for child, parents in enumerate(deps):
        for parent in parents:
            children[parent].append(child)
    decisions: list[str | None] = []
    elapsed = 0
    while True:
        state = _compute_closure(tasks, deps, state)
        if _is_finished(state):
            break
        ready = _ready_comms(tasks, deps, state)
        selected: int | None = None
        if ready:
            remaining = lambda index: (
                state[index] if state[index] > 0 else tasks[index].duration
            )
            unlock = lambda index: sum(
                tasks[child].duration
                for child in children[index]
                if tasks[child].kind == "compute"
            )
            if policy == "fifo":
                selected = min(ready)
            elif policy == "spt":
                selected = min(ready, key=lambda index: (remaining(index), index))
            elif policy == "lpt":
                selected = max(ready, key=lambda index: (remaining(index), -index))
            elif policy == "longest_tail":
                selected = max(ready, key=lambda index: (tail[order[index]], -index))
            elif policy == "lrpt":
                selected = max(
                    ready,
                    key=lambda index: (tail[order[index]] + remaining(index), -index),
                )
            else:
                selected = max(
                    ready,
                    key=lambda index: (
                        tail[order[index]] + unlock(index),
                        unlock(index),
                        -remaining(index),
                        -index,
                    ),
                )
        decisions.append(order[selected] if selected is not None else None)
        state = _tick(tasks, deps, state, selected)
        elapsed += 1
    return elapsed, decisions


def _chain_dag(
    name: str,
    category: str,
    chains: tuple[tuple[tuple[int, ...], tuple[int, ...]], ...],
    description: str,
) -> BenchmarkDAG:
    builder = _Builder(name, category, description)
    for chain_index, (comms, computes) in enumerate(chains):
        previous: str | None = None
        for operation, (comm, compute) in enumerate(zip(comms, computes)):
            flow = builder.add(
                f"c{chain_index}_flow{operation}", "comm", comm,
                () if previous is None else (previous,), role="chain_flow",
            )
            previous = builder.add(
                f"c{chain_index}_compute{operation}", "compute", compute,
                (flow,), role="chain_compute",
            )
    return builder.finish(chains=len(chains))


def adversarial_benchmarks() -> list[BenchmarkDAG]:
    result = [
        _chain_dag(
            "large_flow_vs_long_tail", "adversarial",
            (((1, 1), (8, 0)), ((8,), (0,))),
            "A short flow unlocks a long compute tail while a large flow competes.",
        ),
        _chain_dag(
            "longest_tail_counterexample", "adversarial",
            (((2, 1), (3, 1)), ((1, 2), (2, 1))),
            "Small counterexample where static longest-tail is not optimal.",
        ),
        _chain_dag(
            "lrpt_double_count", "adversarial",
            (((3, 1), (1, 5)), ((1, 2), (4, 0)), ((2,), (2,))),
            "Large current flow can be counted twice by LRPT-style scores.",
        ),
    ]

    builder = _Builder(
        "join_false_critical", "adversarial",
        "Two apparently critical branches meet a join; only the last arrival gates it.",
    )
    a = builder.add("a_flow", "comm", 2, role="join_input")
    a_tail = builder.add("a_compute", "compute", 5, (a,))
    b = builder.add("b_flow", "comm", 3, role="join_input")
    join = builder.add("join", "compute", 1, (a_tail, b), role="join")
    builder.add("sink", "compute", 2, (join,))
    result.append(builder.finish())

    builder = _Builder(
        "fork_multi_unlock", "adversarial",
        "One small flow releases several compute branches before a final join.",
    )
    root = builder.add("fork_flow", "comm", 1, role="fork")
    branches = [builder.add(f"branch{i}", "compute", 3 + i, (root,)) for i in range(3)]
    side = builder.add("large_side_flow", "comm", 6)
    builder.add("join", "compute", 1, (*branches, side), role="join")
    result.append(builder.finish())

    builder = _Builder(
        "deferred_w_competition", "adversarial",
        "Several W/DP side jobs compete with a PP flow that unlocks backbone compute.",
    )
    pp = builder.add("pp", "comm", 1, role="pp")
    backbone = builder.add("backbone", "compute", 7, (pp,), role="backbone")
    dps = []
    for index, duration in enumerate((2, 3, 2)):
        w = builder.add(f"w{index}", "compute", 1, role="w")
        dps.append(builder.add(f"dp{index}", "comm", duration, (w,), role="dp", cut="optimizer"))
    builder.add("optimizer", "compute", 1, (backbone, *dps), role="optimizer")
    result.append(builder.finish())

    builder = _Builder(
        "optimizer_dp_burst", "adversarial",
        "A concentrated set of DP flows forms the optimizer barrier.",
    )
    dps = []
    for index in range(4):
        w = builder.add(f"w{index}", "compute", index % 2 + 1, role="w")
        dps.append(builder.add(f"dp{index}", "comm", 2, (w,), role="dp", cut="optimizer"))
    builder.add("optimizer", "compute", 2, dps, role="optimizer")
    result.append(builder.finish())
    return result


def pp_wave(*, direction: str, stages: int = 3, microbatches: int = 2) -> BenchmarkDAG:
    builder = _Builder(
        f"pp_{direction}_wave", "llm_motif", f"Parameterized PP {direction} wave.",
    )
    stage_order = range(stages) if direction == "forward" else range(stages - 1, -1, -1)
    for microbatch in range(microbatches):
        previous = None
        for position, stage in enumerate(stage_order):
            compute = builder.add(
                f"mb{microbatch}_s{stage}_{direction}", "compute", 2,
                () if previous is None else (previous,), role=direction,
            )
            if position + 1 < stages:
                previous = builder.add(
                    f"mb{microbatch}_s{stage}_{direction}_pp", "comm", 1,
                    (compute,), role="pp", cut=f"pp_boundary_{min(stage, stage + (1 if direction == 'forward' else -1))}",
                )
            else:
                previous = compute
    return builder.finish(stages=stages, microbatches=microbatches)


def one_f_one_b_motif(*, stages: int = 2, microbatches: int = 3) -> BenchmarkDAG:
    builder = _Builder("one_f_one_b", "llm_motif", "Small 1F1B wave with per-stage compute order.")
    f: dict[tuple[int, int], str] = {}
    b: dict[tuple[int, int], str] = {}
    for mb in range(microbatches):
        for stage in range(stages):
            deps = ()
            if stage:
                deps = (builder.add(f"fpp_{mb}_{stage-1}", "comm", 1, (f[mb, stage - 1],), role="pp"),)
            f[mb, stage] = builder.add(f"f_{mb}_{stage}", "compute", 2, deps, role="f")
        for stage in reversed(range(stages)):
            deps = (f[mb, stage],)
            if stage + 1 < stages:
                deps = (builder.add(f"bpp_{mb}_{stage+1}", "comm", 1, (b[mb, stage + 1],), role="pp"),)
            b[mb, stage] = builder.add(f"b_{mb}_{stage}", "compute", 3, deps, role="b")
            builder.add(f"w_{mb}_{stage}", "compute", 1, (b[mb, stage],), role="w")
    # Materialize a simple valid per-stage 1F1B order as resource edges by
    # replacing each task with the same task plus the previous ordered task.
    by_id = {task.task_id: task for task in builder.tasks}
    stage_sequences = {
        stage: [f[mb, stage] for mb in range(min(stages - stage, microbatches))]
        + [item for mb in range(microbatches) for item in (f[mb, stage], b[mb, stage])]
        for stage in range(stages)
    }
    # Remove duplicates while preserving order, then add only acyclic forward
    # resource edges (topological order is the authority for this motif).
    topo = topological_order(builder.finish(stages=stages, microbatches=microbatches))
    rank = {task_id: index for index, task_id in enumerate(topo)}
    extra: dict[str, list[str]] = defaultdict(list)
    for sequence in stage_sequences.values():
        unique = list(dict.fromkeys(sequence))
        for left, right in zip(unique, unique[1:]):
            if rank[left] < rank[right]:
                extra[right].append(left)
    builder.tasks = [
        BenchTask(task.task_id, task.kind, task.duration,
                  tuple(dict.fromkeys((*task.deps, *extra[task.task_id]))), task.role, task.cut)
        for task in builder.tasks
    ]
    return builder.finish(stages=stages, microbatches=microbatches)


def zb_fork_motif(*, microbatches: int = 3) -> BenchmarkDAG:
    builder = _Builder("zb_bw_fork", "llm_motif", "Idealized ZB where B and W fork from grad-output readiness.")
    weights = []
    previous_b = None
    for mb in reversed(range(microbatches)):
        f = builder.add(f"f{mb}", "compute", 2, role="f")
        grad = builder.add(f"grad{mb}", "comm", 1, () if previous_b is None else (previous_b,), role="pp_grad")
        b = builder.add(f"b{mb}", "compute", 3, (f, grad), role="b")
        weights.append(builder.add(f"w{mb}", "compute", 2, (f, grad), role="w"))
        previous_b = b
    builder.add("optimizer", "compute", 1, weights, role="optimizer")
    return builder.finish(microbatches=microbatches)


def w_dp_optimizer_motif(*, buckets: int = 3) -> BenchmarkDAG:
    builder = _Builder("w_dp_optimizer_join", "llm_motif", "W buckets feed DP collectives and one optimizer join.")
    dps = []
    for bucket in range(buckets):
        w = builder.add(f"w{bucket}", "compute", bucket + 1, role="w")
        dps.append(builder.add(f"dp{bucket}", "comm", 2, (w,), role="dp", cut="optimizer"))
    builder.add("optimizer", "compute", 2, dps, role="optimizer")
    return builder.finish(buckets=buckets)


def tp_plus_pp_motif(*, tp_flows: int = 3) -> BenchmarkDAG:
    builder = _Builder("tp_collective_plus_pp", "llm_motif", "TP flow fan-out/fan-in followed by a PP send.")
    f = builder.add("f", "compute", 2, role="f")
    tp = [builder.add(f"tp{i}", "comm", 1, (f,), role="tp", cut="tp_collective") for i in range(tp_flows)]
    join = builder.add("tp_complete", "compute", 1, tp, role="collective_join")
    pp = builder.add("pp", "comm", 2, (join,), role="pp")
    builder.add("next_stage_f", "compute", 3, (pp,), role="f")
    return builder.finish(tp_flows=tp_flows)


def warmup_steady_cooldown_motif(*, microbatches: int = 4) -> BenchmarkDAG:
    builder = _Builder("warmup_steady_cooldown", "llm_motif", "One rank's F/B/W schedule phases with PP side flows.")
    sequence: list[str] = []
    for mb in range(2):
        sequence.append(builder.add(f"f{mb}", "compute", 2, role="warmup_f"))
    for mb in range(2, microbatches):
        sequence.append(builder.add(f"f{mb}", "compute", 2, role="steady_f"))
        sequence.append(builder.add(f"b{mb-2}", "compute", 3, role="steady_b"))
        sequence.append(builder.add(f"w{mb-2}", "compute", 1, role="steady_w"))
    for mb in range(max(0, microbatches - 2), microbatches):
        sequence.append(builder.add(f"b{mb}", "compute", 3, role="cooldown_b"))
        sequence.append(builder.add(f"w{mb}", "compute", 1, role="cooldown_w"))
    previous = None
    rewritten = []
    for task_id in sequence:
        task = next(task for task in builder.tasks if task.task_id == task_id)
        deps = task.deps if previous is None else (*task.deps, previous)
        rewritten.append(BenchTask(task.task_id, task.kind, task.duration, tuple(dict.fromkeys(deps)), task.role, task.cut))
        previous = task_id
    builder.tasks = rewritten
    # Ready side flows create actual communication choices around the fixed rank.
    for mb in range(microbatches):
        builder.add(f"pp_side{mb}", "comm", 1, role="pp")
    return builder.finish(microbatches=microbatches)


def llm_motif_benchmarks() -> list[BenchmarkDAG]:
    return [
        pp_wave(direction="forward"),
        pp_wave(direction="backward"),
        one_f_one_b_motif(),
        zb_fork_motif(),
        w_dp_optimizer_motif(),
        tp_plus_pp_motif(),
        warmup_steady_cooldown_motif(),
    ]


def reduce_effective_dag(
    graph: dict,
    *,
    max_flows: int = 4,
    quantum_us: float = 500.0,
    bucket_rank: int = 0,
) -> BenchmarkDAG:
    """Extract a small conflict window from a stage-0 effective DAG export.

    Seeds are the densest group of flows by quantized earliest-start time.  The
    reduction keeps direct predecessors/successors and every companion input of
    a retained join.  External predecessors become parallel boundary-release
    compute nodes; external downstream work becomes one conservative tail.
    """

    nodes = {node["id"]: node for node in graph["nodes"]}
    predecessors: dict[int, set[int]] = defaultdict(set)
    successors: dict[int, set[int]] = defaultdict(set)
    for edge in graph["edges"]:
        predecessors[edge["target"]].add(edge["source"])
        successors[edge["source"]].add(edge["target"])
    flows = [node for node in nodes.values() if node["type"] == "flow"]
    if not flows:
        raise ValueError("effective DAG has no flow nodes")
    buckets: dict[int, list[dict]] = defaultdict(list)
    for node in flows:
        buckets[round(node["earliest_start_us"] / quantum_us)].append(node)
    ranked_buckets = sorted(
        buckets.items(), key=lambda item: (-len(item[1]), item[0]),
    )
    if not 0 <= bucket_rank < len(ranked_buckets):
        raise ValueError(
            f"bucket_rank={bucket_rank} outside 0..{len(ranked_buckets) - 1}"
        )
    _bucket, candidates = ranked_buckets[bucket_rank]
    seeds = sorted(candidates, key=lambda node: (node["slack_us"], node["id"]))[:max_flows]
    selected = {node["id"] for node in seeds}
    for seed in list(selected):
        selected.update(predecessors[seed])
        selected.update(successors[seed])
    for node_id in list(selected):
        if len(predecessors[node_id]) > 1:
            selected.update(predecessors[node_id])

    builder = _Builder(
        (
            "reduced_real_1f1b_window"
            if bucket_rank == 0
            else f"reduced_real_1f1b_window_{bucket_rank}"
        ),
        "real_reduction",
        "Conflict window reduced from a stage-0 effective DAG export.",
    )

    def quantize(value: float, *, allow_zero: bool = False) -> int:
        rounded = round(value / quantum_us)
        return max(0 if allow_zero else 1, rounded)

    boundary_for: dict[int, str] = {}
    for node_id in sorted(selected):
        external = predecessors[node_id] - selected
        if external:
            release = max(nodes[item]["earliest_finish_us"] for item in external)
            boundary_for[node_id] = builder.add(
                f"release_{node_id}", "compute", quantize(release, allow_zero=True),
                role="boundary_release",
            )
    for node_id in sorted(selected):
        node = nodes[node_id]
        deps = [f"t{item}" for item in sorted(predecessors[node_id] & selected)]
        if node_id in boundary_for:
            deps.append(boundary_for[node_id])
        builder.add(
            f"t{node_id}",
            "comm" if node["type"] == "flow" else "compute",
            quantize(node["duration_us"], allow_zero=node["type"] == "compute"),
            deps,
            role=f"{node.get('phase', '')}:{node.get('stage', '')}",
        )
    graph_makespan = max(node["earliest_finish_us"] for node in nodes.values())
    retained_sinks = [node_id for node_id in selected if not (successors[node_id] & selected)]
    for node_id in retained_sinks:
        tail = max(0.0, graph_makespan - nodes[node_id]["earliest_finish_us"])
        if tail:
            builder.add(
                f"tail_{node_id}", "compute", quantize(tail), (f"t{node_id}",),
                role="boundary_tail",
            )
    reduced = builder.finish(
        source_nodes=len(nodes), selected_nodes=len(selected),
        seed_flows=len(seeds), quantum_us=quantum_us, bucket_rank=bucket_rank,
    )
    return contract_compute_chains(reduced)


def contract_compute_chains(dag: BenchmarkDAG) -> BenchmarkDAG:
    """Safely merge fixed compute-only series pairs.

    A pair is contracted only when the first compute has exactly one child and
    the second compute has exactly that one predecessor.  No fork, join or
    communication boundary is crossed.
    """

    tasks = dag.task_map()
    while True:
        children: dict[str, list[str]] = {task_id: [] for task_id in tasks}
        for task in tasks.values():
            for dependency in task.deps:
                children[dependency].append(task.task_id)
        pair = next(
            (
                (left.task_id, children[left.task_id][0])
                for left in tasks.values()
                if left.kind == "compute"
                and len(children[left.task_id]) == 1
                and tasks[children[left.task_id][0]].kind == "compute"
                and tasks[children[left.task_id][0]].deps == (left.task_id,)
            ),
            None,
        )
        if pair is None:
            break
        left_id, right_id = pair
        left, right = tasks[left_id], tasks[right_id]
        merged_id = f"{left_id}+{right_id}"
        replacement = BenchTask(
            merged_id,
            "compute",
            left.duration + right.duration,
            left.deps,
            f"{left.role}+{right.role}".strip("+"),
        )
        rewritten: dict[str, BenchTask] = {}
        for task_id, task in tasks.items():
            if task_id in {left_id, right_id}:
                continue
            rewritten[task_id] = BenchTask(
                task.task_id,
                task.kind,
                task.duration,
                tuple(merged_id if dependency == right_id else dependency for dependency in task.deps),
                task.role,
                task.cut,
            )
        rewritten[merged_id] = replacement
        tasks = rewritten
    result = BenchmarkDAG(
        dag.name,
        dag.category,
        tuple(tasks.values()),
        dag.description,
        (*dag.parameters, ("contracted_nodes", str(len(dag.tasks) - len(tasks)))),
    )
    errors = result.validate()
    if errors:
        raise AssertionError(f"compute-chain contraction produced invalid DAG: {errors}")
    return result


def random_chain_dag(rng: random.Random) -> BenchmarkDAG:
    chains = []
    for _ in range(rng.randint(2, 4)):
        length = rng.randint(1, 3)
        chains.append((
            tuple(rng.randint(1, 3) for _ in range(length)),
            tuple(rng.randint(0, 5) for _ in range(length)),
        ))
    return _chain_dag("random_chain", "search", tuple(chains), "Seeded random parallel chains.")


def random_join_fork_dag(rng: random.Random) -> BenchmarkDAG:
    """Generate a small layered DAG with both fork and join opportunities."""

    builder = _Builder(
        "random_join_fork", "search",
        "Seeded layered communication/compute DAG with randomized joins.",
    )
    first_compute = []
    for index in range(rng.randint(2, 4)):
        flow = builder.add(f"root_flow{index}", "comm", rng.randint(1, 3))
        first_compute.append(builder.add(
            f"root_compute{index}", "compute", rng.randint(0, 5), (flow,),
        ))
    tails = []
    for index in range(rng.randint(1, 3)):
        parent_count = rng.randint(1, min(2, len(first_compute)))
        parents = tuple(sorted(rng.sample(first_compute, parent_count)))
        flow = builder.add(
            f"join_flow{index}", "comm", rng.randint(1, 3), parents,
            cut="final_join",
        )
        tails.append(builder.add(
            f"tail_compute{index}", "compute", rng.randint(0, 5), (flow,),
        ))
    builder.add("sink", "compute", rng.randint(0, 3), tails, role="join")
    return builder.finish()


def evaluate(dag: BenchmarkDAG, *, max_states: int = 2_000_000) -> dict:
    oracle = exact_oracle(dag, max_states=max_states)
    branch_and_bound = branch_and_bound_oracle(dag, max_states=max_states)
    if branch_and_bound.makespan != oracle.makespan:
        raise AssertionError(
            f"oracle disagreement for {dag.name}: DP={oracle.makespan}, "
            f"B&B={branch_and_bound.makespan}"
        )
    policies = {}
    for policy in POLICIES:
        makespan, _decisions = heuristic_schedule(dag, policy)
        policies[policy] = {
            "makespan": makespan,
            "ratio": makespan / oracle.makespan,
        }
    return {
        "name": dag.name,
        "category": dag.category,
        "description": dag.description,
        "parameters": dict(dag.parameters),
        "nodes": len(dag.tasks),
        "comm_nodes": sum(task.kind == "comm" for task in dag.tasks),
        "optimum": oracle.makespan,
        "explored_states": oracle.explored_states,
        "branch_and_bound_states": branch_and_bound.explored_states,
        "lower_bounds": oracle.lower_bounds,
        "optimal_decisions": oracle.decisions,
        "policies": policies,
    }


def search_worst_instances(samples: int, seed: int) -> dict:
    rng = random.Random(seed)
    worst: dict[str, dict] = {}
    ratios: dict[str, list[float]] = defaultdict(list)
    families: Counter[str] = Counter()
    solved = 0
    skipped = 0
    for sample in range(samples):
        dag = random_chain_dag(rng) if sample % 2 == 0 else random_join_fork_dag(rng)
        families[dag.name] += 1
        try:
            result = evaluate(dag, max_states=300_000)
        except RuntimeError:
            skipped += 1
            continue
        solved += 1
        for policy, policy_result in result["policies"].items():
            ratio = policy_result["ratio"]
            ratios[policy].append(ratio)
            if policy not in worst or ratio > worst[policy]["ratio"]:
                worst[policy] = {
                    "family": dag.name,
                    "ratio": ratio,
                    "makespan": policy_result["makespan"],
                    "optimum": result["optimum"],
                    "tasks": [asdict(task) for task in dag.tasks],
                }
    return {
        "seed": seed,
        "requested": samples,
        "solved": solved,
        "skipped_state_limit": skipped,
        "families": dict(families),
        "policies": {
            policy: {
                "mean_ratio": mean(values),
                "max_ratio": worst[policy]["ratio"],
                "worst": worst[policy],
            }
            for policy, values in ratios.items()
        },
    }


def dag_to_json(dag: BenchmarkDAG) -> dict:
    return {
        "name": dag.name,
        "category": dag.category,
        "description": dag.description,
        "parameters": dict(dag.parameters),
        "tasks": [asdict(task) for task in dag.tasks],
    }


def dag_from_json(payload: dict) -> BenchmarkDAG:
    """Load the stable JSON representation emitted by :func:`dag_to_json`."""

    dag = BenchmarkDAG(
        payload["name"],
        payload["category"],
        tuple(
            BenchTask(
                task["task_id"],
                task["kind"],
                int(task["duration"]),
                tuple(task.get("deps", ())),
                task.get("role", ""),
                task.get("cut", ""),
            )
            for task in payload["tasks"]
        ),
        payload.get("description", ""),
        tuple(sorted((key, str(value)) for key, value in payload.get("parameters", {}).items())),
    )
    errors = dag.validate()
    if errors:
        raise ValueError(f"invalid benchmark JSON: {errors}")
    return dag


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="outputs/benchmark_oracle")
    parser.add_argument("--search-samples", type=int, default=200)
    parser.add_argument("--seed", type=int, default=260811)
    parser.add_argument(
        "--effective-dag",
        default="outputs/dag_audit/1f1b/effective_dag.json",
        help="Stage-0 export used for the real-DAG reduction benchmark.",
    )
    args = parser.parse_args()
    if args.search_samples < 0:
        parser.error("--search-samples must be non-negative")

    benchmarks = [*adversarial_benchmarks(), *llm_motif_benchmarks()]
    effective_path = ROOT / args.effective_dag
    reduction_error = None
    if effective_path.exists():
        graph = json.loads(effective_path.read_text(encoding="utf-8"))
        benchmarks.append(reduce_effective_dag(graph))
    else:
        reduction_error = f"missing stage-0 effective DAG: {effective_path}"

    output = ROOT / args.out
    output.mkdir(parents=True, exist_ok=True)
    results = []
    for dag in benchmarks:
        result = evaluate(dag)
        results.append(result)
        (output / f"{dag.name}.json").write_text(
            json.dumps({"benchmark": dag_to_json(dag), "evaluation": result}, indent=2),
            encoding="utf-8",
        )
        print(
            f"[{dag.category}] {dag.name}: nodes={len(dag.tasks)} "
            f"comm={result['comm_nodes']} opt={result['optimum']} "
            f"LB={result['lower_bounds']['combined']} states={result['explored_states']}"
        )

    search = search_worst_instances(args.search_samples, args.seed)
    report = {
        "model": {
            "channel": "single unit-capacity preemptive communication bottleneck",
            "compute": "ready compute starts immediately and progresses in parallel",
            "time": "positive integer quanta",
        },
        "benchmarks": results,
        "worst_instance_search": search,
        "real_reduction_error": reduction_error,
    }
    (output / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Report: {output / 'report.json'}")


if __name__ == "__main__":
    main()
