"""Non-preemptive single-channel DAG scheduling semantics.

This module is deliberately isolated from the production executor.  It is the
reference transition system for the revised heuristic studies:

* every communication occupies the full channel in one contiguous interval;
* every compute runs continuously after its dependencies complete;
* computes may overlap one another and the active communication;
* scheduling decisions are made only while the channel is idle;
* an explicit WAIT action may advance to the next compute completion.

The benchmark DAG uses integer time quanta.  A stable ``ScheduleState`` is a
decision epoch, so ``active_flow`` is always ``None`` at its boundary.  A FLOW
transition records the busy interval and all compute events inside it, but it
does not expose those intermediate events as preemption points.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Sequence

from DAG_heuristic.common.benchmark import BenchmarkDAG, BenchTask, topological_order


TaskStatus = Literal["pending", "running", "completed"]
ActionKind = Literal["flow", "wait"]


@dataclass(frozen=True)
class RuntimeTask:
    """Runtime state for one immutable benchmark task."""

    status: TaskStatus = "pending"
    remaining: int = 0
    started_at: int | None = None
    completed_at: int | None = None


@dataclass(frozen=True)
class ScheduleState:
    """A stable decision state at which the communication channel is idle."""

    time: int
    tasks: tuple[RuntimeTask, ...]
    active_flow: str | None = None


@dataclass(frozen=True)
class Action:
    kind: ActionKind
    task_id: str | None = None

    @classmethod
    def flow(cls, task_id: str) -> Action:
        return cls("flow", task_id)

    @classmethod
    def wait(cls) -> Action:
        return cls("wait")


@dataclass(frozen=True)
class TimelineEvent:
    time: int
    kind: str
    task_id: str


@dataclass(frozen=True)
class ExecutionInterval:
    task_id: str
    kind: Literal["compute", "comm"]
    start: int
    end: int


@dataclass(frozen=True)
class Transition:
    action: Action
    before: ScheduleState
    after: ScheduleState
    events: tuple[TimelineEvent, ...]
    intervals: tuple[ExecutionInterval, ...]


@dataclass(frozen=True)
class ScheduleTrace:
    final_state: ScheduleState
    transitions: tuple[Transition, ...]
    events: tuple[TimelineEvent, ...]
    intervals: tuple[ExecutionInterval, ...]
    task_ids: tuple[str, ...]

    @property
    def makespan(self) -> int:
        return self.final_state.time


class NonPreemptiveDAGModel:
    """Reference state machine for whole-flow, optional-idle scheduling."""

    def __init__(self, dag: BenchmarkDAG):
        errors = dag.validate()
        if errors:
            raise ValueError(f"invalid benchmark DAG {dag.name}: {errors}")
        if any(task.kind == "comm" and task.duration <= 0 for task in dag.tasks):
            raise ValueError("R0 reference model requires positive flow durations")

        order = topological_order(dag)
        task_map = dag.task_map()
        self.dag = dag
        self.task_ids = tuple(order)
        self.tasks = tuple(task_map[task_id] for task_id in order)
        self.index = {task_id: index for index, task_id in enumerate(order)}
        self.deps = tuple(
            tuple(self.index[parent] for parent in task.deps) for task in self.tasks
        )

    def initial_state(self) -> ScheduleState:
        state = ScheduleState(0, tuple(RuntimeTask() for _ in self.tasks))
        state, _events, _intervals = self._start_ready_computes(state)
        return state

    def initial_events(self, state: ScheduleState) -> tuple[TimelineEvent, ...]:
        if state.time != 0:
            raise ValueError("initial events require the t=0 state")
        events: list[TimelineEvent] = []
        for task, runtime in zip(self.tasks, state.tasks, strict=True):
            if task.kind != "compute" or runtime.started_at != 0:
                continue
            events.append(TimelineEvent(0, "compute_started", task.task_id))
            if runtime.status == "completed" and runtime.completed_at == 0:
                events.append(TimelineEvent(0, "compute_completed", task.task_id))
        return tuple(self._sort_events(events))

    def initial_intervals(self, state: ScheduleState) -> tuple[ExecutionInterval, ...]:
        return tuple(
            ExecutionInterval(task.task_id, "compute", 0, 0)
            for task, runtime in zip(self.tasks, state.tasks, strict=True)
            if task.kind == "compute"
            and runtime.status == "completed"
            and runtime.started_at == 0
            and runtime.completed_at == 0
        )

    def task_runtime(self, state: ScheduleState, task_id: str) -> RuntimeTask:
        return state.tasks[self.index[task_id]]

    def ready_flows(self, state: ScheduleState) -> tuple[str, ...]:
        self._require_stable(state)
        ready = []
        for index, task in enumerate(self.tasks):
            runtime = state.tasks[index]
            if (
                task.kind == "comm"
                and runtime.status == "pending"
                and self._deps_completed(state.tasks, index)
            ):
                ready.append(task.task_id)
        return tuple(ready)

    def active_computes(self, state: ScheduleState) -> tuple[str, ...]:
        return tuple(
            task.task_id
            for task, runtime in zip(self.tasks, state.tasks, strict=True)
            if task.kind == "compute" and runtime.status == "running"
        )

    def legal_actions(self, state: ScheduleState) -> tuple[Action, ...]:
        """Return whole-flow starts plus WAIT when a real future event exists."""

        actions = [Action.flow(task_id) for task_id in self.ready_flows(state)]
        if self.active_computes(state):
            actions.append(Action.wait())
        return tuple(actions)

    def is_finished(self, state: ScheduleState) -> bool:
        return all(runtime.status == "completed" for runtime in state.tasks)

    def step(self, state: ScheduleState, action: Action) -> Transition:
        self._require_stable(state)
        if action not in self.legal_actions(state):
            raise ValueError(f"illegal action at t={state.time}: {action}")
        if action.kind == "wait":
            return self._wait(state, action)
        assert action.task_id is not None
        return self._run_flow(state, action)

    def run(self, actions: Sequence[Action]) -> ScheduleTrace:
        state = self.initial_state()
        transitions: list[Transition] = []
        events = list(self.initial_events(state))
        intervals = list(self.initial_intervals(state))
        for action in actions:
            transition = self.step(state, action)
            transitions.append(transition)
            events.extend(transition.events)
            intervals.extend(transition.intervals)
            state = transition.after
        return ScheduleTrace(
            state,
            tuple(transitions),
            tuple(self._sort_events(events)),
            tuple(intervals),
            self.task_ids,
        )

    def _run_flow(self, state: ScheduleState, action: Action) -> Transition:
        task_id = action.task_id
        assert task_id is not None
        index = self.index[task_id]
        task = self.tasks[index]
        start = state.time
        end = start + task.duration
        values = list(state.tasks)
        values[index] = RuntimeTask("running", task.duration, start, None)
        working = ScheduleState(start, tuple(values), task_id)
        events: list[TimelineEvent] = [TimelineEvent(start, "flow_started", task_id)]
        compute_intervals: list[ExecutionInterval] = []

        # Compute completions are observed for dependency release, but never
        # become scheduler decision points while this flow owns the channel.
        while working.time < end:
            running = self._running_compute_indices(working)
            next_compute = min(
                (working.tasks[item].remaining for item in running), default=None
            )
            delta = end - working.time
            if next_compute is not None:
                delta = min(delta, next_compute)
            working, new_events, new_intervals = self._advance_computes(working, delta)
            events.extend(new_events)
            compute_intervals.extend(new_intervals)

        values = list(working.tasks)
        values[index] = RuntimeTask("completed", 0, start, end)
        working = ScheduleState(end, tuple(values), None)
        events.append(TimelineEvent(end, "flow_completed", task_id))
        working, start_events, start_intervals = self._start_ready_computes(working)
        events.extend(start_events)
        compute_intervals.extend(start_intervals)

        interval = ExecutionInterval(task_id, "comm", start, end)
        return Transition(
            action,
            state,
            working,
            tuple(self._sort_events(events)),
            (interval, *compute_intervals),
        )

    def _wait(self, state: ScheduleState, action: Action) -> Transition:
        running = self._running_compute_indices(state)
        if not running:
            raise ValueError("WAIT requires an active compute completion event")
        delta = min(state.tasks[index].remaining for index in running)
        after, events, intervals = self._advance_computes(state, delta)
        return Transition(action, state, after, tuple(events), tuple(intervals))

    def _advance_computes(
        self, state: ScheduleState, delta: int
    ) -> tuple[ScheduleState, list[TimelineEvent], list[ExecutionInterval]]:
        if delta <= 0:
            raise ValueError("time advancement must be positive")
        end = state.time + delta
        values = list(state.tasks)
        events: list[TimelineEvent] = []
        intervals: list[ExecutionInterval] = []
        for index in self._running_compute_indices(state):
            runtime = values[index]
            remaining = runtime.remaining - delta
            if remaining == 0:
                values[index] = RuntimeTask(
                    "completed", 0, runtime.started_at, end
                )
                task_id = self.tasks[index].task_id
                events.append(TimelineEvent(end, "compute_completed", task_id))
                assert runtime.started_at is not None
                intervals.append(
                    ExecutionInterval(task_id, "compute", runtime.started_at, end)
                )
            else:
                values[index] = RuntimeTask(
                    "running", remaining, runtime.started_at, None
                )
        if state.active_flow is not None:
            flow_index = self.index[state.active_flow]
            flow_runtime = values[flow_index]
            flow_remaining = flow_runtime.remaining - delta
            if flow_remaining < 0:
                raise AssertionError("advanced beyond active flow completion")
            values[flow_index] = RuntimeTask(
                "running", flow_remaining, flow_runtime.started_at, None
            )
        advanced = ScheduleState(end, tuple(values), state.active_flow)
        advanced, start_events, start_intervals = self._start_ready_computes(advanced)
        events.extend(start_events)
        intervals.extend(start_intervals)
        return advanced, self._sort_events(events), intervals

    def _start_ready_computes(
        self, state: ScheduleState
    ) -> tuple[
        ScheduleState, list[TimelineEvent], list[ExecutionInterval]
    ]:
        values = list(state.tasks)
        events: list[TimelineEvent] = []
        intervals: list[ExecutionInterval] = []
        changed = True
        while changed:
            changed = False
            for index, task in enumerate(self.tasks):
                if task.kind != "compute" or values[index].status != "pending":
                    continue
                if self._deps_completed(values, index):
                    events.append(
                        TimelineEvent(state.time, "compute_started", task.task_id)
                    )
                    if task.duration == 0:
                        values[index] = RuntimeTask(
                            "completed", 0, state.time, state.time
                        )
                        events.append(
                            TimelineEvent(
                                state.time, "compute_completed", task.task_id
                            )
                        )
                        intervals.append(
                            ExecutionInterval(
                                task.task_id, "compute", state.time, state.time
                            )
                        )
                    else:
                        values[index] = RuntimeTask(
                            "running", task.duration, state.time, None
                        )
                    changed = True
        return (
            ScheduleState(state.time, tuple(values), state.active_flow),
            self._sort_events(events),
            intervals,
        )

    def _deps_completed(
        self, runtimes: Sequence[RuntimeTask], task_index: int
    ) -> bool:
        return all(
            runtimes[parent].status == "completed" for parent in self.deps[task_index]
        )

    def _running_compute_indices(self, state: ScheduleState) -> list[int]:
        return [
            index
            for index, (task, runtime) in enumerate(
                zip(self.tasks, state.tasks, strict=True)
            )
            if task.kind == "compute" and runtime.status == "running"
        ]

    @staticmethod
    def _sort_events(events: list[TimelineEvent]) -> list[TimelineEvent]:
        order = {
            "compute_completed": 0,
            "flow_completed": 1,
            "compute_started": 2,
            "flow_started": 3,
        }
        return sorted(events, key=lambda event: (event.time, order[event.kind], event.task_id))

    @staticmethod
    def _require_stable(state: ScheduleState) -> None:
        if state.active_flow is not None:
            raise ValueError("scheduler decisions require an idle channel")


def assert_nonpreemptive_trace(trace: ScheduleTrace) -> None:
    """Validate that every completed task has exactly one positive interval."""

    by_task: dict[str, list[ExecutionInterval]] = {}
    for interval in trace.intervals:
        if interval.end < interval.start:
            raise AssertionError(f"negative interval: {interval}")
        if interval.kind == "comm" and interval.end == interval.start:
            raise AssertionError(f"zero-duration communication: {interval}")
        by_task.setdefault(interval.task_id, []).append(interval)
    duplicates = {task_id: spans for task_id, spans in by_task.items() if len(spans) > 1}
    if duplicates:
        raise AssertionError(f"tasks executed in multiple intervals: {duplicates}")
    for task_id, runtime in zip(trace.task_ids, trace.final_state.tasks, strict=True):
        spans = by_task.get(task_id, [])
        if runtime.status == "completed" and len(spans) != 1:
            raise AssertionError(
                f"completed task {task_id} has {len(spans)} execution intervals"
            )
        if spans and (
            spans[0].start != runtime.started_at
            or spans[0].end != runtime.completed_at
        ):
            raise AssertionError(
                f"interval/runtime mismatch for {task_id}: {spans[0]} vs {runtime}"
            )
    comms = sorted(
        (interval for interval in trace.intervals if interval.kind == "comm"),
        key=lambda interval: interval.start,
    )
    for previous, current in zip(comms, comms[1:]):
        if previous.end > current.start:
            raise AssertionError(
                f"communication intervals overlap: {previous} and {current}"
            )
