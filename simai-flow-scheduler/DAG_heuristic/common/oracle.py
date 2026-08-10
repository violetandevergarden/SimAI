"""Exact oracles for non-preemptive, optional-idle DAG scheduling.

The two solvers intentionally use different search strategies over the R0
transition system:

* ``exact_oracle`` is a memoized residual-cost dynamic program;
* ``branch_and_bound_oracle`` is a forward DFS with a feasible incumbent,
  residual lower bounds, dominance, and hard state/time limits.

Both can solve the true optional-idle problem or its work-conserving
restriction.  They return whole-flow/WAIT actions and reconstruct a continuous
timeline through :mod:`DAG_heuristic.common.model`.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from time import perf_counter
from typing import Literal, Sequence

from DAG_heuristic.common.benchmark import BenchmarkDAG, lower_bounds, topological_order
from DAG_heuristic.common.model import (
    Action,
    NonPreemptiveDAGModel,
    RuntimeTask,
    ScheduleState,
    ScheduleTrace,
    assert_nonpreemptive_trace,
)


OracleMode = Literal["optional_idle", "work_conserving"]
StateKey = tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class NonPreemptiveOracleResult:
    mode: OracleMode
    makespan: int
    actions: tuple[Action, ...]
    trace: ScheduleTrace
    explored_states: int
    cache_hits: int
    runtime_ms: float
    lower_bounds: dict[str, int]
    voluntary_waits: int
    forced_waits: int
    voluntary_wait_time: int
    forced_wait_time: int
    wait_time: int


@dataclass(frozen=True)
class OracleComparison:
    optional_idle: NonPreemptiveOracleResult
    work_conserving: NonPreemptiveOracleResult

    @property
    def idle_regret(self) -> int:
        return self.work_conserving.makespan - self.optional_idle.makespan


def _state_key(state: ScheduleState) -> StateKey:
    if state.active_flow is not None:
        raise ValueError("oracle keys are defined only at idle-channel decisions")
    return tuple((runtime.status, runtime.remaining) for runtime in state.tasks)


def _state_from_key(key: StateKey) -> ScheduleState:
    runtimes: list[RuntimeTask] = []
    for status, remaining in key:
        if status == "pending":
            runtimes.append(RuntimeTask())
        elif status == "running":
            runtimes.append(RuntimeTask("running", remaining, 0, None))
        elif status == "completed":
            runtimes.append(RuntimeTask("completed", 0, 0, 0))
        else:
            raise ValueError(f"unknown runtime status in oracle key: {status}")
    return ScheduleState(0, tuple(runtimes))


def _candidate_actions(
    model: NonPreemptiveDAGModel,
    state: ScheduleState,
    mode: OracleMode,
) -> tuple[Action, ...]:
    ready = model.ready_flows(state)
    flows = tuple(Action.flow(task_id) for task_id in ready)
    can_wait = bool(model.active_computes(state))
    if mode == "work_conserving":
        if flows:
            return flows
        return (Action.wait(),) if can_wait else ()
    if mode != "optional_idle":
        raise ValueError(f"unknown oracle mode: {mode}")
    return (*flows, Action.wait()) if can_wait else flows


def _successors(
    model: NonPreemptiveDAGModel,
    state: ScheduleState,
    mode: OracleMode,
) -> list[tuple[Action, int, StateKey]]:
    """Enumerate actions, merging only actions with exactly equal successors."""

    result: list[tuple[Action, int, StateKey]] = []
    seen: set[StateKey] = set()
    for action in _candidate_actions(model, state, mode):
        transition = model.step(state, action)
        successor = _state_key(transition.after)
        if successor in seen:
            continue
        seen.add(successor)
        result.append((action, transition.after.time - state.time, successor))
    return result


def _tail_lengths(dag: BenchmarkDAG) -> dict[str, int]:
    order = topological_order(dag)
    tasks = dag.task_map()
    children: dict[str, list[str]] = {task_id: [] for task_id in order}
    for task in tasks.values():
        for parent in task.deps:
            children[parent].append(task.task_id)
    tail: dict[str, int] = {}
    for task_id in reversed(order):
        tail[task_id] = max(
            (tasks[child].duration + tail[child] for child in children[task_id]),
            default=0,
        )
    return tail


def _greedy_completion(
    model: NonPreemptiveDAGModel,
    state: ScheduleState,
    *,
    wait_when_ready: bool,
    longest_tail: bool,
) -> tuple[int, tuple[Action, ...]]:
    elapsed = 0
    result: list[Action] = []
    tails = _tail_lengths(model.dag)
    while not model.is_finished(state):
        ready = model.ready_flows(state)
        active = model.active_computes(state)
        if active and (wait_when_ready or not ready):
            action = Action.wait()
        elif ready:
            if longest_tail:
                task_id = max(ready, key=lambda item: (tails[item], item))
            else:
                task_id = ready[0]
            action = Action.flow(task_id)
        else:
            raise RuntimeError("unfinished DAG has no ready flow or active compute")
        result.append(action)
        transition = model.step(state, action)
        elapsed += transition.after.time - state.time
        state = transition.after
    return elapsed, tuple(result)


def _incumbent_actions(
    model: NonPreemptiveDAGModel, mode: OracleMode
) -> tuple[Action, ...]:
    candidates = [
        _greedy_completion(
            model,
            model.initial_state(),
            wait_when_ready=False,
            longest_tail=False,
        ),
        _greedy_completion(
            model,
            model.initial_state(),
            wait_when_ready=False,
            longest_tail=True,
        ),
    ]
    if mode == "optional_idle":
        candidates.extend(
            [
                _greedy_completion(
                    model,
                    model.initial_state(),
                    wait_when_ready=True,
                    longest_tail=False,
                ),
                _greedy_completion(
                    model,
                    model.initial_state(),
                    wait_when_ready=True,
                    longest_tail=True,
                ),
            ]
        )
    return min(candidates, key=lambda item: item[0])[1]


def nonpreemptive_release_tail_bound(dag: BenchmarkDAG) -> int:
    """A safe non-preemptive release/tail subset lower bound.

    Communication durations are ignored when deriving releases and tails, so
    the selected flows' serialized work is never counted twice.  For every
    pair of release/tail thresholds, all matching flows form a valid subset
    bound ``r + sum(p) + q``.
    """

    order = topological_order(dag)
    tasks = dag.task_map()
    children: dict[str, list[str]] = {task_id: [] for task_id in order}
    for task in tasks.values():
        for parent in task.deps:
            children[parent].append(task.task_id)

    compute_earliest: dict[str, int] = {}
    for task_id in order:
        task = tasks[task_id]
        compute_earliest[task_id] = max(
            (compute_earliest[parent] for parent in task.deps), default=0
        ) + (task.duration if task.kind == "compute" else 0)

    compute_tail: dict[str, int] = {}
    for task_id in reversed(order):
        compute_tail[task_id] = max(
            (
                (tasks[child].duration if tasks[child].kind == "compute" else 0)
                + compute_tail[child]
                for child in children[task_id]
            ),
            default=0,
        )

    comms = [task for task in dag.tasks if task.kind == "comm"]
    if not comms:
        return max(compute_earliest.values(), default=0)
    releases = {
        task.task_id: max(
            (compute_earliest[parent] for parent in task.deps), default=0
        )
        for task in comms
    }
    release_values = {0, *(releases[task.task_id] for task in comms)}
    tail_values = {0, *(compute_tail[task.task_id] for task in comms)}
    best = 0
    for release in release_values:
        for tail in tail_values:
            work = sum(
                task.duration
                for task in comms
                if releases[task.task_id] >= release
                and compute_tail[task.task_id] >= tail
            )
            if work:
                best = max(best, release + work + tail)
    return best


def oracle_lower_bounds(dag: BenchmarkDAG) -> dict[str, int]:
    result = dict(lower_bounds(dag))
    result["release_tail"] = nonpreemptive_release_tail_bound(dag)
    model = NonPreemptiveDAGModel(dag)
    initial = model.initial_state()
    result["next_event"] = (
        min(
            model.task_runtime(initial, task_id).remaining
            for task_id in model.active_computes(initial)
        )
        if not model.ready_flows(initial) and model.active_computes(initial)
        else 0
    )
    result["combined"] = max(result.values())
    return result


def _residual_lower_bound(model: NonPreemptiveDAGModel, key: StateKey) -> int:
    durations = []
    for task, (status, remaining) in zip(model.tasks, key, strict=True):
        if status == "completed":
            durations.append(0)
        elif status == "running":
            durations.append(remaining)
        else:
            durations.append(task.duration)

    communication = sum(
        duration
        for task, duration in zip(model.tasks, durations, strict=True)
        if task.kind == "comm"
    )
    path: list[int] = [0] * len(model.tasks)
    for index in reversed(range(len(model.tasks))):
        children = [
            child
            for child, parents in enumerate(model.deps)
            if index in parents
        ]
        path[index] = durations[index] + max(
            (path[child] for child in children), default=0
        )
    return max(communication, max(path, default=0))


def _replay_result(
    model: NonPreemptiveDAGModel,
    mode: OracleMode,
    actions: Sequence[Action],
    explored_states: int,
    cache_hits: int,
    runtime_ms: float,
) -> NonPreemptiveOracleResult:
    trace = model.run(actions)
    if not model.is_finished(trace.final_state):
        raise AssertionError("oracle action path did not finish the DAG")
    assert_nonpreemptive_trace(trace)
    voluntary_waits = 0
    forced_waits = 0
    voluntary_wait_time = 0
    forced_wait_time = 0
    wait_time = 0
    for transition in trace.transitions:
        if transition.action.kind != "wait":
            continue
        duration = transition.after.time - transition.before.time
        wait_time += duration
        if model.ready_flows(transition.before):
            voluntary_waits += 1
            voluntary_wait_time += duration
        else:
            forced_waits += 1
            forced_wait_time += duration
    bounds = oracle_lower_bounds(model.dag)
    if bounds["combined"] > trace.makespan:
        raise AssertionError(
            f"invalid lower bound {bounds} > optimum {trace.makespan}"
        )
    return NonPreemptiveOracleResult(
        mode,
        trace.makespan,
        tuple(actions),
        trace,
        explored_states,
        cache_hits,
        runtime_ms,
        bounds,
        voluntary_waits,
        forced_waits,
        voluntary_wait_time,
        forced_wait_time,
        wait_time,
    )


def exact_oracle(
    dag: BenchmarkDAG,
    *,
    mode: OracleMode = "optional_idle",
    max_states: int = 2_000_000,
    time_limit_s: float = 30.0,
) -> NonPreemptiveOracleResult:
    """Solve a small DAG by memoized residual-cost recursion."""

    model = NonPreemptiveDAGModel(dag)
    initial = _state_key(model.initial_state())
    explored = 0
    started = perf_counter()

    @lru_cache(maxsize=None)
    def solve(key: StateKey) -> tuple[int, tuple[Action, ...]]:
        nonlocal explored
        explored += 1
        if explored > max_states:
            raise RuntimeError(f"exact oracle exceeded max_states={max_states}")
        if perf_counter() - started > time_limit_s:
            raise TimeoutError(f"exact oracle exceeded time_limit_s={time_limit_s}")
        state = _state_from_key(key)
        if model.is_finished(state):
            return 0, ()
        successors = _successors(model, state, mode)
        if not successors:
            raise RuntimeError("unfinished DAG has no legal action")
        greedy_candidates = [
            _greedy_completion(
                model, state, wait_when_ready=False, longest_tail=False
            ),
            _greedy_completion(
                model, state, wait_when_ready=False, longest_tail=True
            ),
        ]
        if mode == "optional_idle":
            greedy_candidates.extend(
                [
                    _greedy_completion(
                        model, state, wait_when_ready=True, longest_tail=False
                    ),
                    _greedy_completion(
                        model, state, wait_when_ready=True, longest_tail=True
                    ),
                ]
            )
        best_value, best_path = min(greedy_candidates, key=lambda item: item[0])
        ranked = sorted(
            successors,
            key=lambda item: (
                item[1] + _residual_lower_bound(model, item[2]),
                item[0].kind == "wait",
                item[0].task_id or "",
            ),
        )
        for action, elapsed, successor in ranked:
            if elapsed + _residual_lower_bound(model, successor) >= best_value:
                continue
            child_value, child_path = solve(successor)
            value = elapsed + child_value
            tie = "~WAIT" if action.kind == "wait" else action.task_id or ""
            best_tie = (
                "~WAIT"
                if best_path and best_path[0].kind == "wait"
                else (best_path[0].task_id or "") if best_path else ""
            )
            if (value, tie) < (best_value, best_tie):
                best_value = value
                best_path = (action, *child_path)
        return best_value, best_path

    optimum, actions = solve(initial)
    runtime_ms = (perf_counter() - started) * 1000
    result = _replay_result(
        model,
        mode,
        actions,
        explored,
        solve.cache_info().hits,
        runtime_ms,
    )
    if result.makespan != optimum:
        raise AssertionError(f"replay {result.makespan} != DP optimum {optimum}")
    return result


def branch_and_bound_oracle(
    dag: BenchmarkDAG,
    *,
    mode: OracleMode = "optional_idle",
    max_states: int = 2_000_000,
    time_limit_s: float = 30.0,
) -> NonPreemptiveOracleResult:
    """Independently solve a small DAG by forward branch-and-bound DFS."""

    model = NonPreemptiveDAGModel(dag)
    incumbent = _incumbent_actions(model, mode)
    best_actions = incumbent
    best_time = model.run(incumbent).makespan
    initial = _state_key(model.initial_state())
    seen_elapsed: dict[StateKey, int] = {}
    explored = 0
    started = perf_counter()

    def visit(key: StateKey, elapsed: int, path: tuple[Action, ...]) -> None:
        nonlocal best_actions, best_time, explored
        explored += 1
        if explored > max_states:
            raise RuntimeError(
                f"branch-and-bound exceeded max_states={max_states}"
            )
        if perf_counter() - started > time_limit_s:
            raise TimeoutError(
                f"branch-and-bound exceeded time_limit_s={time_limit_s}"
            )
        state = _state_from_key(key)
        if model.is_finished(state):
            if elapsed < best_time:
                best_time = elapsed
                best_actions = path
            return
        if elapsed + _residual_lower_bound(model, key) >= best_time:
            return
        if seen_elapsed.get(key, best_time + 1) <= elapsed:
            return
        seen_elapsed[key] = elapsed
        successors = _successors(model, state, mode)
        ranked = sorted(
            successors,
            key=lambda item: (
                elapsed + item[1] + _residual_lower_bound(model, item[2]),
                item[0].kind == "wait",
                item[0].task_id or "",
            ),
        )
        for action, duration, successor in ranked:
            visit(successor, elapsed + duration, (*path, action))

    visit(initial, 0, ())
    runtime_ms = (perf_counter() - started) * 1000
    result = _replay_result(
        model, mode, best_actions, explored, 0, runtime_ms
    )
    if result.makespan != best_time:
        raise AssertionError(f"replay {result.makespan} != B&B optimum {best_time}")
    return result


def compare_oracles(
    dag: BenchmarkDAG,
    *,
    max_states: int = 2_000_000,
    time_limit_s: float = 30.0,
) -> OracleComparison:
    optional = exact_oracle(
        dag,
        mode="optional_idle",
        max_states=max_states,
        time_limit_s=time_limit_s,
    )
    work_conserving = exact_oracle(
        dag,
        mode="work_conserving",
        max_states=max_states,
        time_limit_s=time_limit_s,
    )
    return OracleComparison(optional, work_conserving)
