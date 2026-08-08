"""Regression checks for stage-3 general-DAG scheduling research."""

import random

from scripts.benchmark_dag_oracle import _Builder, exact_oracle
from scripts.study_general_dag_heuristics import (
    ResidualDAG,
    last_blocker_overboost_counterexample,
    local_beam_schedule,
    random_join_dag,
    rollout_schedule,
    schedule_policy,
)


def _replay_is_legal(dag, decisions: list[str | None]) -> bool:
    model = ResidualDAG(dag)
    state = model.initial
    by_name = {task_id: index for index, task_id in enumerate(model.order)}
    for decision in decisions:
        ready = model.ready(state)
        if decision is None:
            assert not ready
            selected = None
        else:
            selected = by_name[decision]
            assert selected in ready
        state = model.tick(state, selected)
    return all(value == 0 for value in state)


def test_join_gate_gain_identifies_the_last_blocking_ready_flow() -> None:
    builder = _Builder("gate", "test", "two ready inputs feed one join")
    slow = builder.add("slow", "comm", 3)
    fast = builder.add("fast", "comm", 1)
    join = builder.add("join", "compute", 2, (slow, fast))
    builder.add("sink", "compute", 2, (join,))
    model = ResidualDAG(builder.finish())

    analysis = model.analyze(model.initial)
    index = {task_id: position for position, task_id in enumerate(model.order)}

    assert analysis.gate_gain[index["slow"]] == 2
    assert analysis.gate_gain[index["fast"]] == 0
    assert analysis.gate_tail[index["slow"]] > analysis.tail[index["slow"]]


def test_residual_tail_decreases_when_downstream_compute_progresses() -> None:
    builder = _Builder("dynamic", "test", "active compute changes another flow's tail")
    trigger = builder.add("trigger", "comm", 1)
    compute = builder.add("compute", "compute", 5, (trigger,))
    later = builder.add("later", "comm", 1, (compute,))
    side = builder.add("side", "comm", 3)
    builder.add("side_sink", "compute", 1, (side,))
    model = ResidualDAG(builder.finish())
    index = {task_id: position for position, task_id in enumerate(model.order)}

    state = model.tick(model.initial, index["trigger"])
    before = model.analyze(state).earliest_finish[index["later"]]
    state = model.tick(state, index["side"])
    after = model.analyze(state).earliest_finish[index["later"]]

    assert after == before - 1


def test_event_rollout_and_beam_return_legal_schedules() -> None:
    dag = random_join_dag(random.Random(17), 0)
    for result in (
        rollout_schedule(dag, top_k=4),
        local_beam_schedule(dag, width=8, event_depth=3),
    ):
        assert len(result.decisions) == result.makespan
        assert _replay_is_legal(dag, result.decisions)


def test_enhanced_searches_keep_dynamic_tail_incumbent() -> None:
    rng = random.Random(260817)
    for index in range(20):
        dag = random_join_dag(rng, index)
        baseline = schedule_policy(dag, "dynamic_tail").makespan

        assert rollout_schedule(dag, top_k=4).makespan <= baseline
        assert local_beam_schedule(dag, width=8, event_depth=3).makespan <= baseline


def test_general_dag_methods_never_beat_the_exact_oracle() -> None:
    rng = random.Random(23)
    for index in range(8):
        dag = random_join_dag(rng, index)
        optimum = exact_oracle(dag, max_states=250_000).makespan
        for result in (
            schedule_policy(dag, "dynamic_tail"),
            schedule_policy(dag, "gate_dynamic_tail"),
            rollout_schedule(dag, top_k=4),
            local_beam_schedule(dag, width=8, event_depth=2),
        ):
            assert result.makespan >= optimum


def test_raw_last_blocker_bonus_is_not_a_safe_standalone_priority() -> None:
    dag = last_blocker_overboost_counterexample()

    assert exact_oracle(dag).makespan == 20
    assert schedule_policy(dag, "dynamic_tail").makespan == 20
    assert schedule_policy(dag, "gate_dynamic_tail").makespan == 21
    assert rollout_schedule(dag, top_k=2).makespan == 20
