"""Semantic regressions for the revised non-preemptive research model."""

import pytest

from DAG_heuristic.common.benchmark import BenchmarkDAG, BenchTask
from DAG_heuristic.common.model import (
    Action,
    NonPreemptiveDAGModel,
    assert_nonpreemptive_trace,
)


def _overlap_dag() -> BenchmarkDAG:
    return BenchmarkDAG(
        "flow_atomicity",
        "r0_semantics",
        (
            BenchTask("release", "compute", 2),
            BenchTask("long_flow", "comm", 5),
            BenchTask("new_flow", "comm", 1, ("release",)),
            BenchTask("compute_tail", "compute", 3, ("new_flow",)),
        ),
    )


def _waiting_counterexample(magnitude: int = 10) -> BenchmarkDAG:
    return BenchmarkDAG(
        "waiting_is_necessary",
        "r0_semantics",
        (
            BenchTask("release_b", "compute", 1),
            BenchTask("A", "comm", magnitude),
            BenchTask("B", "comm", 1, ("release_b",)),
            BenchTask("tail_b", "compute", magnitude, ("B",)),
        ),
    )


def test_flow_is_atomic_while_compute_completion_releases_a_new_flow() -> None:
    model = NonPreemptiveDAGModel(_overlap_dag())
    transition = model.step(model.initial_state(), Action.flow("long_flow"))

    assert transition.before.time == 0
    assert transition.after.time == 5
    assert model.task_runtime(transition.after, "release").completed_at == 2
    assert model.task_runtime(transition.after, "new_flow").status == "pending"
    assert model.ready_flows(transition.after) == ("new_flow",)
    assert [(span.task_id, span.start, span.end) for span in transition.intervals] == [
        ("long_flow", 0, 5),
        ("release", 0, 2),
    ]
    assert not any(
        event.kind == "flow_started" and event.task_id == "new_flow"
        for event in transition.events
    )


def test_compute_is_nonpreemptive_and_can_overlap_communication() -> None:
    model = NonPreemptiveDAGModel(_overlap_dag())
    state = model.initial_state()

    assert model.task_runtime(state, "release").remaining == 2
    after_flow = model.step(state, Action.flow("long_flow")).after
    assert model.task_runtime(after_flow, "release").started_at == 0
    assert model.task_runtime(after_flow, "release").completed_at == 2


def test_wait_can_be_voluntary_and_repeated_until_distinct_releases() -> None:
    dag = BenchmarkDAG(
        "repeated_wait",
        "r0_semantics",
        (
            BenchTask("release_1", "compute", 1),
            BenchTask("release_2", "compute", 3),
            BenchTask("ready_now", "comm", 4),
            BenchTask("flow_1", "comm", 1, ("release_1",)),
            BenchTask("flow_2", "comm", 1, ("release_2",)),
        ),
    )
    model = NonPreemptiveDAGModel(dag)
    state = model.initial_state()

    assert Action.wait() in model.legal_actions(state)
    first = model.step(state, Action.wait())
    assert first.after.time == 1
    assert set(model.ready_flows(first.after)) == {"ready_now", "flow_1"}
    assert Action.wait() in model.legal_actions(first.after)

    second = model.step(first.after, Action.wait())
    assert second.after.time == 3
    assert set(model.ready_flows(second.after)) == {
        "ready_now",
        "flow_1",
        "flow_2",
    }
    assert Action.wait() not in model.legal_actions(second.after)
    with pytest.raises(ValueError, match="illegal action"):
        model.step(second.after, Action.wait())

    trace = model.run(
        [
            Action.wait(),
            Action.wait(),
            Action.flow("flow_1"),
            Action.flow("flow_2"),
            Action.flow("ready_now"),
        ]
    )
    assert trace.makespan == 9
    assert model.is_finished(trace.final_state)
    assert_nonpreemptive_trace(trace)


def test_optional_wait_beats_every_work_conserving_first_action() -> None:
    model = NonPreemptiveDAGModel(_waiting_counterexample(10))

    work_conserving = model.run(
        [Action.flow("A"), Action.flow("B"), Action.wait()]
    )
    optional_idle = model.run(
        [Action.wait(), Action.flow("B"), Action.flow("A")]
    )

    assert work_conserving.makespan == 21
    assert optional_idle.makespan == 12
    assert model.is_finished(work_conserving.final_state)
    assert model.is_finished(optional_idle.final_state)
    assert_nonpreemptive_trace(work_conserving)
    assert_nonpreemptive_trace(optional_idle)


def test_task_ids_dependencies_and_dag_are_not_mutated() -> None:
    dag = _overlap_dag()
    original = tuple((task.task_id, task.deps) for task in dag.tasks)
    model = NonPreemptiveDAGModel(dag)
    trace = model.run(
        [Action.flow("long_flow"), Action.flow("new_flow"), Action.wait()]
    )

    assert model.is_finished(trace.final_state)
    assert trace.makespan == 9
    assert dag.validate() == []
    assert tuple((task.task_id, task.deps) for task in dag.tasks) == original
    assert_nonpreemptive_trace(trace)


def test_invalid_dag_and_zero_duration_flow_are_rejected() -> None:
    cyclic = BenchmarkDAG(
        "cycle",
        "r0_semantics",
        (
            BenchTask("a", "comm", 1, ("b",)),
            BenchTask("b", "compute", 1, ("a",)),
        ),
    )
    with pytest.raises(ValueError, match="cycle"):
        NonPreemptiveDAGModel(cyclic)

    zero_flow = BenchmarkDAG(
        "zero_flow", "r0_semantics", (BenchTask("a", "comm", 0),)
    )
    with pytest.raises(ValueError, match="positive flow"):
        NonPreemptiveDAGModel(zero_flow)


def test_zero_duration_compute_is_an_instantaneous_closure() -> None:
    dag = BenchmarkDAG(
        "zero_compute",
        "r0_semantics",
        (
            BenchTask("zero", "compute", 0),
            BenchTask("flow", "comm", 1, ("zero",)),
        ),
    )
    model = NonPreemptiveDAGModel(dag)
    initial = model.initial_state()

    assert model.task_runtime(initial, "zero").completed_at == 0
    trace = model.run([Action.flow("flow")])
    assert trace.makespan == 1
    assert model.is_finished(trace.final_state)
    assert_nonpreemptive_trace(trace)
