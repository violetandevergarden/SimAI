"""Checks for stage-3.5 hard filtering and counterfactual ablations."""

from scripts.benchmark_dag_oracle import exact_oracle
from scripts.study_counterfactual_bonus import (
    ABLATIONS,
    effective_graph_to_benchmark,
    evaluate_hard_suite,
    generate_hard_dags,
)
from scripts.study_general_dag_heuristics import rollout_schedule, schedule_policy


def test_hard_generator_only_keeps_positive_dynamic_tail_regret() -> None:
    dags, search = generate_hard_dags(2, 260818, max_attempts=100)

    assert search["found"] == len(dags) == 2
    for dag in dags:
        optimum = exact_oracle(dag).makespan
        assert schedule_policy(dag, "dynamic_tail").makespan > optimum


def test_all_candidate_and_horizon_ablations_keep_the_incumbent() -> None:
    dags, _search = generate_hard_dags(1, 260818, max_attempts=20)
    dag = dags[0]
    baseline = schedule_policy(dag, "dynamic_tail").makespan
    optimum = exact_oracle(dag).makespan

    for arguments in ABLATIONS.values():
        result = rollout_schedule(dag, top_k=2, **arguments)
        assert optimum <= result.makespan <= baseline


def test_hard_ablation_report_records_gap_closure() -> None:
    dags, _search = generate_hard_dags(2, 260818, max_attempts=100)
    report = evaluate_hard_suite(dags, top_k=2)

    assert report["instances"] == 2
    assert set(report["summary"]) == set(ABLATIONS)
    for summary in report["summary"].values():
        assert 0 <= summary["repair_fraction"] <= 1
        assert 0 <= summary["mean_gap_closed"] <= 1


def test_effective_graph_conversion_preserves_dependencies() -> None:
    graph = {
        "nodes": [
            {"id": 0, "type": "compute", "duration_us": 5.0, "phase": "f", "stage": 0},
            {"id": 1, "type": "flow", "duration_us": 20.0, "phase": "pp", "stage": 0},
            {"id": 2, "type": "compute", "duration_us": 10.0, "phase": "f", "stage": 1},
        ],
        "edges": [
            {"source": 0, "target": 1},
            {"source": 1, "target": 2},
        ],
    }

    dag = effective_graph_to_benchmark(graph, quantum_us=10.0)
    tasks = dag.task_map()

    assert not dag.validate()
    assert tasks["t1"].duration == 2
    assert tasks["t1"].deps == ("t0",)
    assert tasks["t2"].deps == ("t1",)
