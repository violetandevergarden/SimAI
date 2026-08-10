"""R4: non-preemptive optional-idle scheduling on route resources.

Every started flow reserves all resources on its route until completion.  At a
task-completion event the scheduler may start a compatible subset of ready
flows, or start nothing and wait for the next active task completion.  The
module is isolated research infrastructure and does not change the production
executor.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass, replace
from functools import lru_cache
import json
from pathlib import Path
import random
from statistics import mean
import sys
from time import perf_counter
from typing import Hashable, Iterable, Literal


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from DAG_heuristic.common.benchmark import BenchmarkDAG, _Builder
from DAG_heuristic.muti_channel.legacy_generator import (
    MultiResourceInstance,
    random_topology_instance,
    route_resource_sets,
)
from DAG_heuristic.muti_channel.topology_fixtures import (
    four_rack_core_topology,
    single_switch_topology,
    two_rack_topology,
)
from src.static_analysis.passes.routing import BfsStrategy
from src.workload_format.schema import Meta, P2PWorkload, Task, TaskType


Resource = Hashable
Status = Literal["pending", "running", "completed"]
OracleMode = Literal["optional_idle", "work_conserving"]


@dataclass(frozen=True)
class ResourceRuntime:
    status: Status = "pending"
    remaining: int = 0
    started_at: int | None = None
    completed_at: int | None = None


@dataclass(frozen=True)
class ResourceState:
    time: int
    tasks: tuple[ResourceRuntime, ...]


@dataclass(frozen=True)
class ResourceAction:
    starts: tuple[str, ...] = ()

    @property
    def kind(self) -> str:
        return "start" if self.starts else "wait"

    @classmethod
    def start(cls, task_ids: Iterable[str]) -> ResourceAction:
        values = tuple(sorted(task_ids))
        if not values:
            raise ValueError("START action requires at least one flow")
        return cls(values)

    @classmethod
    def wait(cls) -> ResourceAction:
        return cls(())


@dataclass(frozen=True)
class ResourceInterval:
    task_id: str
    kind: str
    start: int
    end: int


@dataclass(frozen=True)
class ResourceTransition:
    action: ResourceAction
    before: ResourceState
    after: ResourceState
    completed: tuple[str, ...]
    intervals: tuple[ResourceInterval, ...]


@dataclass(frozen=True)
class ResourceSchedule:
    makespan: int
    actions: tuple[ResourceAction, ...]
    intervals: tuple[ResourceInterval, ...]
    runtime_ms: float
    explored_states: int = 0
    voluntary_waits: int = 0
    voluntary_wait_time: int = 0
    forced_waits: int = 0
    forced_wait_time: int = 0
    start_events: int = 0
    flows_started: int = 0
    candidate_actions: int = 0
    conflict_events: int = 0
    lower_bounds: dict[str, int] | None = None
    fallback: bool = False


class NonPreemptiveMultiResourceDAG:
    """Event-driven state machine with persistent route reservations."""

    def __init__(self, instance: MultiResourceInstance):
        errors = instance.validate()
        if errors:
            raise ValueError(errors)
        tasks = instance.dag.task_map()
        order = _topological_order(instance.dag)
        self.instance = instance
        self.order = tuple(order)
        self.tasks = tuple(tasks[task_id] for task_id in order)
        self.index = {task_id: index for index, task_id in enumerate(order)}
        self.deps = tuple(
            tuple(self.index[parent] for parent in task.deps) for task in self.tasks
        )
        children: list[list[int]] = [[] for _ in self.tasks]
        for child, parents in enumerate(self.deps):
            for parent in parents:
                children[parent].append(child)
        self.children = tuple(tuple(items) for items in children)
        self.resources = tuple(
            instance.resources.get(task.task_id, frozenset()) for task in self.tasks
        )

    def initial_state(self) -> ResourceState:
        state = ResourceState(0, tuple(ResourceRuntime() for _ in self.tasks))
        return self._start_ready_computes(state)[0]

    def is_finished(self, state: ResourceState) -> bool:
        return all(runtime.status == "completed" for runtime in state.tasks)

    def ready_flows(self, state: ResourceState) -> tuple[str, ...]:
        return tuple(
            task.task_id
            for index, task in enumerate(self.tasks)
            if task.kind == "comm"
            and state.tasks[index].status == "pending"
            and self._deps_completed(state.tasks, index)
        )

    def active_flows(self, state: ResourceState) -> tuple[str, ...]:
        return tuple(
            task.task_id
            for index, task in enumerate(self.tasks)
            if task.kind == "comm" and state.tasks[index].status == "running"
        )

    def active_computes(self, state: ResourceState) -> tuple[str, ...]:
        return tuple(
            task.task_id
            for index, task in enumerate(self.tasks)
            if task.kind == "compute" and state.tasks[index].status == "running"
        )

    def occupied_resources(self, state: ResourceState) -> frozenset[Resource]:
        occupied: set[Resource] = set()
        for task_id in self.active_flows(state):
            occupied.update(self.resources[self.index[task_id]])
        return frozenset(occupied)

    def compatible(self, state: ResourceState, task_ids: Iterable[str]) -> bool:
        occupied = set(self.occupied_resources(state))
        for task_id in task_ids:
            resources = self.resources[self.index[task_id]]
            if occupied & resources:
                return False
            occupied.update(resources)
        return True

    def start_subsets(
        self, state: ResourceState, *, maximal_only: bool
    ) -> tuple[tuple[str, ...], ...]:
        ready = tuple(sorted(self.ready_flows(state)))
        occupied = self.occupied_resources(state)
        subsets: list[tuple[str, ...]] = []

        def visit(
            position: int,
            chosen: tuple[str, ...],
            used: frozenset[Resource],
        ) -> None:
            if position == len(ready):
                if chosen:
                    subsets.append(chosen)
                return
            task_id = ready[position]
            visit(position + 1, chosen, used)
            resources = self.resources[self.index[task_id]]
            if not (used & resources):
                visit(position + 1, (*chosen, task_id), used | resources)

        visit(0, (), occupied)
        unique = sorted(set(subsets))
        if not maximal_only:
            return tuple(unique)
        return tuple(
            selected
            for selected in unique
            if not any(set(selected) < set(other) for other in unique)
        )

    def legal_actions(
        self, state: ResourceState, mode: OracleMode
    ) -> tuple[ResourceAction, ...]:
        if mode == "work_conserving":
            starts = self.start_subsets(state, maximal_only=True)
            if starts:
                return tuple(ResourceAction.start(items) for items in starts)
        elif mode == "optional_idle":
            starts = self.start_subsets(state, maximal_only=False)
            actions = [ResourceAction.start(items) for items in starts]
            if self._has_active_task(state):
                actions.append(ResourceAction.wait())
            return tuple(actions)
        else:
            raise ValueError(f"unknown oracle mode: {mode}")
        return (ResourceAction.wait(),) if self._has_active_task(state) else ()

    def step(
        self, state: ResourceState, action: ResourceAction
    ) -> ResourceTransition:
        starts = action.starts
        if starts:
            if len(starts) != len(set(starts)):
                raise ValueError("START contains duplicate flow ids")
            ready = set(self.ready_flows(state))
            if any(task_id not in ready for task_id in starts):
                raise ValueError("START contains a flow that is not ready")
            if not self.compatible(state, starts):
                raise ValueError("START conflicts with active or newly started routes")
        elif not self._has_active_task(state):
            raise ValueError("WAIT requires an active completion event")

        values = list(state.tasks)
        for task_id in starts:
            index = self.index[task_id]
            values[index] = ResourceRuntime(
                "running", self.tasks[index].duration, state.time, None
            )
        working = ResourceState(state.time, tuple(values))
        running = [
            runtime.remaining
            for runtime in working.tasks
            if runtime.status == "running"
        ]
        if not running:
            raise RuntimeError("action did not create a future event")
        delta = min(running)
        end = state.time + delta
        completed: list[str] = []
        intervals: list[ResourceInterval] = []
        values = list(working.tasks)
        for index, runtime in enumerate(working.tasks):
            if runtime.status != "running":
                continue
            remaining = runtime.remaining - delta
            if remaining:
                values[index] = ResourceRuntime(
                    "running", remaining, runtime.started_at, None
                )
                continue
            values[index] = ResourceRuntime(
                "completed", 0, runtime.started_at, end
            )
            completed.append(self.tasks[index].task_id)
            assert runtime.started_at is not None
            intervals.append(
                ResourceInterval(
                    self.tasks[index].task_id,
                    self.tasks[index].kind,
                    runtime.started_at,
                    end,
                )
            )
        after = ResourceState(end, tuple(values))
        after, compute_intervals = self._start_ready_computes(after)
        intervals.extend(compute_intervals)
        return ResourceTransition(
            action,
            state,
            after,
            tuple(sorted(completed)),
            tuple(intervals),
        )

    def residual_features(
        self, state: ResourceState
    ) -> tuple[tuple[int, ...], tuple[int, ...]]:
        remaining = [self.remaining(state, index) for index in range(len(self.tasks))]
        path = [0] * len(self.tasks)
        tail = [0] * len(self.tasks)
        for index in reversed(range(len(self.tasks))):
            if not remaining[index]:
                continue
            tail[index] = max(
                (path[child] for child in self.children[index]), default=0
            )
            path[index] = remaining[index] + tail[index]
        return tuple(path), tuple(tail)

    def remaining(self, state: ResourceState, index: int) -> int:
        runtime = state.tasks[index]
        if runtime.status == "completed":
            return 0
        if runtime.status == "running":
            return runtime.remaining
        return self.tasks[index].duration

    def _start_ready_computes(
        self, state: ResourceState
    ) -> tuple[ResourceState, list[ResourceInterval]]:
        values = list(state.tasks)
        intervals: list[ResourceInterval] = []
        changed = True
        while changed:
            changed = False
            for index, task in enumerate(self.tasks):
                if task.kind != "compute" or values[index].status != "pending":
                    continue
                if not self._deps_completed(values, index):
                    continue
                if task.duration == 0:
                    values[index] = ResourceRuntime(
                        "completed", 0, state.time, state.time
                    )
                    intervals.append(
                        ResourceInterval(task.task_id, "compute", state.time, state.time)
                    )
                else:
                    values[index] = ResourceRuntime(
                        "running", task.duration, state.time, None
                    )
                changed = True
        return ResourceState(state.time, tuple(values)), intervals

    def _deps_completed(
        self, runtimes: Iterable[ResourceRuntime], task_index: int
    ) -> bool:
        values = tuple(runtimes)
        return all(values[parent].status == "completed" for parent in self.deps[task_index])

    def _has_active_task(self, state: ResourceState) -> bool:
        return any(runtime.status == "running" for runtime in state.tasks)


def _topological_order(dag: BenchmarkDAG) -> list[str]:
    tasks = dag.task_map()
    degree = {task_id: len(task.deps) for task_id, task in tasks.items()}
    children: dict[str, list[str]] = defaultdict(list)
    for task in tasks.values():
        for parent in task.deps:
            children[parent].append(task.task_id)
    ready = sorted(task_id for task_id, value in degree.items() if value == 0)
    order = []
    while ready:
        task_id = ready.pop(0)
        order.append(task_id)
        for child in children[task_id]:
            degree[child] -= 1
            if degree[child] == 0:
                ready.append(child)
        ready.sort()
    if len(order) != len(tasks):
        raise ValueError("DAG contains a cycle")
    return order


def residual_resource_loads(
    model: NonPreemptiveMultiResourceDAG, state: ResourceState
) -> dict[Resource, int]:
    loads: dict[Resource, int] = defaultdict(int)
    for index, task in enumerate(model.tasks):
        if task.kind != "comm":
            continue
        for resource in model.resources[index]:
            loads[resource] += model.remaining(state, index)
    return dict(loads)


def lower_bounds(
    model: NonPreemptiveMultiResourceDAG, state: ResourceState
) -> dict[str, int]:
    path, _tail = model.residual_features(state)
    loads = residual_resource_loads(model, state)
    result = {
        "critical_path": max(path, default=0),
        "max_resource_load": max(loads.values(), default=0),
    }
    result["combined"] = max(result.values())
    return result


StateKey = tuple[tuple[str, int], ...]


def _state_key(state: ResourceState) -> StateKey:
    return tuple((runtime.status, runtime.remaining) for runtime in state.tasks)


def _state_from_key(key: StateKey) -> ResourceState:
    values = []
    for status, remaining in key:
        if status == "pending":
            values.append(ResourceRuntime())
        elif status == "running":
            values.append(ResourceRuntime("running", remaining, 0, None))
        else:
            values.append(ResourceRuntime("completed", 0, 0, 0))
    return ResourceState(0, tuple(values))


def _replay(
    model: NonPreemptiveMultiResourceDAG,
    actions: Iterable[ResourceAction],
    *,
    runtime_ms: float,
    explored_states: int = 0,
    candidate_actions: int = 0,
    lower: dict[str, int] | None = None,
    fallback: bool = False,
) -> ResourceSchedule:
    state = model.initial_state()
    path = tuple(actions)
    intervals: list[ResourceInterval] = []
    voluntary_waits = voluntary_wait_time = 0
    forced_waits = forced_wait_time = 0
    conflict_events = 0
    for action in path:
        if len(model.ready_flows(state)) >= 2:
            resources = [
                model.resources[model.index[task_id]]
                for task_id in model.ready_flows(state)
            ]
            conflict_events += any(
                resources[left] & resources[right]
                for left in range(len(resources))
                for right in range(left + 1, len(resources))
            )
        had_compatible = bool(model.start_subsets(state, maximal_only=False))
        transition = model.step(state, action)
        duration = transition.after.time - transition.before.time
        if action.kind == "wait":
            if had_compatible:
                voluntary_waits += 1
                voluntary_wait_time += duration
            else:
                forced_waits += 1
                forced_wait_time += duration
        intervals.extend(transition.intervals)
        state = transition.after
    if not model.is_finished(state):
        raise AssertionError("schedule did not finish")
    _assert_route_reservations(model, intervals)
    return ResourceSchedule(
        state.time,
        path,
        tuple(intervals),
        runtime_ms,
        explored_states,
        voluntary_waits,
        voluntary_wait_time,
        forced_waits,
        forced_wait_time,
        sum(action.kind == "start" for action in path),
        sum(len(action.starts) for action in path),
        candidate_actions,
        conflict_events,
        lower,
        fallback,
    )


def _assert_route_reservations(
    model: NonPreemptiveMultiResourceDAG,
    intervals: Iterable[ResourceInterval],
) -> None:
    comms = [interval for interval in intervals if interval.kind == "comm"]
    by_task: dict[str, int] = defaultdict(int)
    for interval in comms:
        if interval.end <= interval.start:
            raise AssertionError(f"invalid communication interval: {interval}")
        by_task[interval.task_id] += 1
    if any(count != 1 for count in by_task.values()):
        raise AssertionError("a flow was split into multiple intervals")
    for left, first in enumerate(comms):
        for second in comms[left + 1 :]:
            overlap = first.start < second.end and second.start < first.end
            if not overlap:
                continue
            first_resources = model.resources[model.index[first.task_id]]
            second_resources = model.resources[model.index[second.task_id]]
            if first_resources & second_resources:
                raise AssertionError(
                    f"overlapping flows share route resources: {first}, {second}"
                )


def exact_oracle(
    instance: MultiResourceInstance,
    *,
    mode: OracleMode = "optional_idle",
    max_states: int = 1_000_000,
    time_limit_s: float = 30.0,
) -> ResourceSchedule:
    started = perf_counter()
    model = NonPreemptiveMultiResourceDAG(instance)
    initial = model.initial_state()
    choices: dict[StateKey, ResourceAction] = {}
    explored = 0

    @lru_cache(maxsize=None)
    def solve(key: StateKey) -> int:
        nonlocal explored
        explored += 1
        if explored > max_states:
            raise RuntimeError("non-preemptive multi-resource oracle state limit")
        if perf_counter() - started > time_limit_s:
            raise TimeoutError("non-preemptive multi-resource oracle time limit")
        state = _state_from_key(key)
        if model.is_finished(state):
            return 0
        actions = model.legal_actions(state, mode)
        if not actions:
            raise RuntimeError("unfinished multi-resource state has no action")
        best = sys.maxsize
        best_action = actions[0]
        for action in actions:
            transition = model.step(state, action)
            delta = transition.after.time - transition.before.time
            value = delta + solve(_state_key(transition.after))
            tie = (action.kind == "wait", len(action.starts), action.starts)
            best_tie = (
                best_action.kind == "wait",
                len(best_action.starts),
                best_action.starts,
            )
            if (value, tie) < (best, best_tie):
                best = value
                best_action = action
        choices[key] = best_action
        return best

    optimum = solve(_state_key(initial))
    actions = []
    state = initial
    while not model.is_finished(state):
        action = choices[_state_key(state)]
        actions.append(action)
        state = model.step(state, action).after
    bounds = lower_bounds(model, initial)
    result = _replay(
        model,
        actions,
        runtime_ms=(perf_counter() - started) * 1000,
        explored_states=explored,
        candidate_actions=sum(
            len(model.legal_actions(_state_from_key(key), mode)) for key in choices
        ),
        lower=bounds,
    )
    if result.makespan != optimum or bounds["combined"] > optimum:
        raise AssertionError("multi-resource exact replay/lower-bound mismatch")
    return result


def _ranked_ready(
    model: NonPreemptiveMultiResourceDAG,
    state: ResourceState,
    policy: str,
) -> list[str]:
    ready = list(model.ready_flows(state))
    _path, tail = model.residual_features(state)
    loads = residual_resource_loads(model, state)

    def bottleneck(task_id: str) -> int:
        index = model.index[task_id]
        return max((loads[item] for item in model.resources[index]), default=0)

    def key(task_id: str):
        index = model.index[task_id]
        duration = model.remaining(state, index)
        if policy == "dynamic_tail":
            return tail[index], -duration, task_id
        if policy == "resource_tail":
            return tail[index], bottleneck(task_id), -duration, task_id
        if policy == "bottleneck_first":
            return bottleneck(task_id), tail[index], -duration, task_id
        if policy == "spt":
            return -duration, tail[index], task_id
        if policy == "lpt":
            return duration, tail[index], task_id
        raise ValueError(f"unknown multi-resource policy: {policy}")

    return sorted(ready, key=key, reverse=True)


def greedy_action(
    model: NonPreemptiveMultiResourceDAG,
    state: ResourceState,
    policy: str,
) -> ResourceAction:
    selected = []
    occupied = set(model.occupied_resources(state))
    for task_id in _ranked_ready(model, state, policy):
        resources = model.resources[model.index[task_id]]
        if occupied & resources:
            continue
        selected.append(task_id)
        occupied.update(resources)
    return ResourceAction.start(selected) if selected else ResourceAction.wait()


def _complete(
    model: NonPreemptiveMultiResourceDAG,
    state: ResourceState,
    policy: str,
) -> tuple[int, tuple[ResourceAction, ...]]:
    start = state.time
    actions = []
    while not model.is_finished(state):
        action = greedy_action(model, state, policy)
        actions.append(action)
        state = model.step(state, action).after
    return state.time - start, tuple(actions)


def schedule_greedy(
    instance: MultiResourceInstance,
    policy: str = "dynamic_tail",
) -> ResourceSchedule:
    started = perf_counter()
    model = NonPreemptiveMultiResourceDAG(instance)
    _elapsed, actions = _complete(model, model.initial_state(), policy)
    return _replay(model, actions, runtime_ms=(perf_counter() - started) * 1000)


def _rollout_candidates(
    model: NonPreemptiveMultiResourceDAG,
    state: ResourceState,
    *,
    top_k: int,
    optional_actions: bool,
) -> tuple[ResourceAction, ...]:
    subsets = model.start_subsets(state, maximal_only=not optional_actions)
    _path, tail = model.residual_features(state)
    ranked = sorted(
        subsets,
        key=lambda selected: (
            sum(tail[model.index[item]] for item in selected),
            -sum(model.remaining(state, model.index[item]) for item in selected),
            selected,
        ),
        reverse=True,
    )[:top_k]
    result = [ResourceAction.start(items) for items in ranked]
    for policy in ("dynamic_tail", "resource_tail", "bottleneck_first", "spt"):
        action = greedy_action(model, state, policy)
        if action.kind == "start" and action not in result:
            result.append(action)
    if optional_actions and model._has_active_task(state):
        result.append(ResourceAction.wait())
    if not result and model._has_active_task(state):
        result.append(ResourceAction.wait())
    return tuple(dict.fromkeys(result))


def schedule_rollout(
    instance: MultiResourceInstance,
    *,
    top_k: int = 2,
    optional_actions: bool,
    time_limit_s: float = 2.0,
) -> ResourceSchedule:
    started = perf_counter()
    model = NonPreemptiveMultiResourceDAG(instance)
    baseline = schedule_greedy(instance)
    state = model.initial_state()
    actions = []
    candidate_actions = 0
    fallback = False
    while not model.is_finished(state):
        base = greedy_action(model, state, "dynamic_tail")
        if perf_counter() - started > time_limit_s:
            action = base
            fallback = True
        else:
            candidates = _rollout_candidates(
                model,
                state,
                top_k=top_k,
                optional_actions=optional_actions,
            )
            if base not in candidates:
                candidates = (*candidates, base)
            candidate_actions += len(candidates)
            scored = []
            for action in candidates:
                transition = model.step(state, action)
                delta = transition.after.time - transition.before.time
                value = delta + _complete(
                    model, transition.after, "dynamic_tail"
                )[0]
                scored.append(
                    (
                        value,
                        action != base,
                        action.kind == "wait",
                        len(action.starts),
                        action.starts,
                        action,
                    )
                )
            action = min(scored, key=lambda item: item[:-1])[-1]
        actions.append(action)
        state = model.step(state, action).after
    result = _replay(
        model,
        actions,
        runtime_ms=(perf_counter() - started) * 1000,
        candidate_actions=candidate_actions,
        fallback=fallback,
    )
    if baseline.makespan < result.makespan:
        return replace(
            baseline,
            runtime_ms=(perf_counter() - started) * 1000,
            candidate_actions=candidate_actions,
            fallback=fallback,
        )
    return result


def topology_motifs() -> list[MultiResourceInstance]:
    result = []

    builder = _Builder("disjoint_routes_np", "r4_motif", "disjoint overlap")
    left = builder.add("left", "comm", 4)
    right = builder.add("right", "comm", 4)
    builder.add("left_tail", "compute", 3, (left,))
    builder.add("right_tail", "compute", 3, (right,))
    result.append(
        MultiResourceInstance(
            builder.finish(),
            {"left": frozenset({"r0"}), "right": frozenset({"r1"})},
        )
    )

    builder = _Builder("shared_route_np", "r4_motif", "shared bottleneck")
    left = builder.add("left", "comm", 4)
    right = builder.add("right", "comm", 4)
    builder.add("left_tail", "compute", 3, (left,))
    builder.add("right_tail", "compute", 3, (right,))
    result.append(
        MultiResourceInstance(
            builder.finish(),
            {"left": frozenset({"shared"}), "right": frozenset({"shared"})},
        )
    )

    builder = _Builder(
        "nonmaximal_start_np",
        "r4_motif",
        "Starting every compatible ready flow blocks a future critical release.",
    )
    release = builder.add("release_c", "compute", 1)
    a = builder.add("a", "comm", 4)
    b = builder.add("b", "comm", 5)
    c = builder.add("c", "comm", 1, (release,))
    builder.add("c_tail", "compute", 6, (c,))
    result.append(
        MultiResourceInstance(
            builder.finish(),
            {
                "a": frozenset({"r0"}),
                "b": frozenset({"r1"}),
                "c": frozenset({"r1"}),
            },
        )
    )

    builder = _Builder(
        "active_reservation_np",
        "r4_motif",
        "A newly ready flow cannot take a route held by an active flow.",
    )
    a = builder.add("a", "comm", 4)
    b = builder.add("b", "comm", 1)
    c = builder.add("c", "comm", 1, (b,))
    result.append(
        MultiResourceInstance(
            builder.finish(),
            {
                "a": frozenset({"shared"}),
                "b": frozenset({"other"}),
                "c": frozenset({"shared"}),
            },
        )
    )
    return result


def _single_channel(instance: MultiResourceInstance) -> MultiResourceInstance:
    return MultiResourceInstance(
        replace(
            instance.dag,
            name=f"{instance.dag.name}_single_channel",
            category="r4_single_channel",
        ),
        {
            task.task_id: frozenset({"single_channel"})
            for task in instance.dag.tasks
            if task.kind == "comm"
        },
    )


def manual_route_topology_instances() -> list[MultiResourceInstance]:
    """Build transparent route-conflict cases through the real BFS adapter."""

    scenarios = {
        "single_switch": (
            single_switch_topology(),
            ((0, 1), (2, 3), (4, 5), (6, 7)),
        ),
        "two_rack": (
            two_rack_topology(),
            ((0, 4), (1, 5), (2, 3), (6, 7)),
        ),
        "four_rack_core": (
            four_rack_core_topology(),
            ((0, 4), (1, 5), (2, 6), (3, 7)),
        ),
    }
    result = []
    for name, (topology, pairs) in scenarios.items():
        builder = _Builder(
            f"manual_route_{name}_np",
            "r4_route_topology",
            "Small route instance produced through TopologyLoader-compatible BFS.",
        )
        workload_tasks = []
        for index, ((src, dst), duration) in enumerate(
            zip(pairs, (4, 3, 2, 1), strict=True), start=1
        ):
            flow_id = builder.add(f"f{index}", "comm", duration)
            builder.add(f"tail{index}", "compute", 6 - index, (flow_id,))
            workload_tasks.append(
                Task(
                    index,
                    0,
                    TaskType.FLOW,
                    src=src,
                    dst=dst,
                    size_bytes=duration,
                )
            )
        workload = P2PWorkload(
            version="1.0",
            meta=Meta(num_jobs=1, num_nodes=8),
            tasks=workload_tasks,
        )
        routes = BfsStrategy().compute_routes(workload, topology)
        raw_resources = route_resource_sets(
            workload,
            routes,
            include_source_nic=True,
            include_destination_nic=True,
        )
        resources = {
            f"f{index}": raw_resources[str(index)]
            for index in range(1, len(pairs) + 1)
        }
        result.append(MultiResourceInstance(builder.finish(), resources))
    return result


def benchmark_suite(samples: int, seed: int) -> list[MultiResourceInstance]:
    rng = random.Random(seed)
    return [
        *topology_motifs(),
        *manual_route_topology_instances(),
        *(random_topology_instance(rng, index) for index in range(samples)),
    ]


def evaluate(
    instances: Iterable[MultiResourceInstance],
    *,
    max_states: int,
    time_limit_s: float,
    progress: bool = False,
) -> dict:
    rows = []
    for number, instance in enumerate(instances, start=1):
        if progress:
            print(f"[{number}] {instance.dag.name}", flush=True)
        optional = exact_oracle(
            instance,
            mode="optional_idle",
            max_states=max_states,
            time_limit_s=time_limit_s,
        )
        work_conserving = exact_oracle(
            instance,
            mode="work_conserving",
            max_states=max_states,
            time_limit_s=time_limit_s,
        )
        single = exact_oracle(
            _single_channel(instance),
            mode="optional_idle",
            max_states=max_states,
            time_limit_s=time_limit_s,
        )
        methods = {
            "dynamic_pack": schedule_greedy(instance, "dynamic_tail"),
            "resource_pack": schedule_greedy(instance, "resource_tail"),
            "bottleneck_pack": schedule_greedy(instance, "bottleneck_first"),
            "rollout_maximal2": schedule_rollout(
                instance, top_k=2, optional_actions=False
            ),
            "rollout_optional2": schedule_rollout(
                instance, top_k=2, optional_actions=True
            ),
            "rollout_optional4": schedule_rollout(
                instance, top_k=4, optional_actions=True
            ),
        }
        rows.append(
            {
                "name": instance.dag.name,
                "category": instance.dag.category,
                "tasks": len(instance.dag.tasks),
                "flows": sum(task.kind == "comm" for task in instance.dag.tasks),
                "opt_idle": optional.makespan,
                "opt_wc": work_conserving.makespan,
                "idle_value": work_conserving.makespan - optional.makespan,
                "single_channel_opt": single.makespan,
                "single_channel_overestimate": single.makespan / optional.makespan - 1,
                "exact_states": optional.explored_states,
                "exact_actions": optional.candidate_actions,
                "methods": {
                    name: {
                        "makespan": result.makespan,
                        "ratio": result.makespan / optional.makespan,
                        "runtime_ms": result.runtime_ms,
                        "voluntary_waits": result.voluntary_waits,
                        "voluntary_wait_time": result.voluntary_wait_time,
                        "start_events": result.start_events,
                        "flows_started": result.flows_started,
                        "candidate_actions": result.candidate_actions,
                        "conflict_events": result.conflict_events,
                        "fallback": result.fallback,
                    }
                    for name, result in methods.items()
                },
                "oracle_actions": [
                    list(action.starts) if action.starts else ["WAIT"]
                    for action in optional.actions
                ],
            }
        )
    names = tuple(rows[0]["methods"]) if rows else ()
    hard = [row for row in rows if row["methods"]["dynamic_pack"]["makespan"] > row["opt_idle"]]
    return {
        "model": {
            "flow": "non-preemptive; full route reserved until completion",
            "compute": "non-preemptive immediate-start",
            "action": "compatible ready-flow subset or WAIT at completion events",
        },
        "instances": len(rows),
        "summary": {
            name: {
                "optimal": sum(
                    row["methods"][name]["makespan"] == row["opt_idle"]
                    for row in rows
                ),
                "mean_ratio": mean(row["methods"][name]["ratio"] for row in rows),
                "max_ratio": max(row["methods"][name]["ratio"] for row in rows),
                "mean_runtime_ms": mean(
                    row["methods"][name]["runtime_ms"] for row in rows
                ),
                "fallbacks": sum(row["methods"][name]["fallback"] for row in rows),
            }
            for name in names
        },
        "hard_subset": {
            "count": len(hard),
            "methods": {
                name: {
                    "improved": sum(
                        row["methods"][name]["makespan"]
                        < row["methods"]["dynamic_pack"]["makespan"]
                        for row in hard
                    ),
                    "exact": sum(
                        row["methods"][name]["makespan"] == row["opt_idle"]
                        for row in hard
                    ),
                }
                for name in names
            },
        },
        "topology_concurrency": {
            "mean_single_channel_overestimate": mean(
                row["single_channel_overestimate"] for row in rows
            ),
            "max_single_channel_overestimate": max(
                row["single_channel_overestimate"] for row in rows
            ),
        },
        "optional_idle": {
            "helped": sum(row["idle_value"] > 0 for row in rows),
            "max_value": max(row["idle_value"] for row in rows),
        },
        "rows": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=30)
    parser.add_argument("--seed", type=int, default=260819)
    parser.add_argument("--max-states", type=int, default=500_000)
    parser.add_argument("--time-limit-s", type=float, default=30.0)
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "outputs/nonpreemptive_multiresource/r4_summary.json",
    )
    args = parser.parse_args()
    report = evaluate(
        benchmark_suite(args.samples, args.seed),
        max_states=args.max_states,
        time_limit_s=args.time_limit_s,
        progress=True,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(json.dumps({key: value for key, value in report.items() if key != "rows"}, indent=2))
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
