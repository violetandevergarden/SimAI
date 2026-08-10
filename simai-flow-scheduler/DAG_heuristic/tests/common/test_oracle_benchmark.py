"""Smoke tests for the reproducible R1 benchmark runner."""

from DAG_heuristic.common.oracle_benchmark import benchmark_cases, evaluate


def test_small_r1_matrix_has_exact_agreement_and_both_idle_behaviors() -> None:
    dags = benchmark_cases(
        random_chains=3,
        random_joins=2,
        real_windows=0,
        real_max_flows=1,
        chain_seed=260813,
        join_seed=260817,
    )
    report = evaluate(dags, max_states=500_000, time_limit_s=10)

    assert report["oracle_agreement"] is True
    assert report["count"] == 22
    assert report["idle_helped"] >= 1
    for row in report["rows"]:
        assert row["optional_idle"]["makespan"] <= (
            row["work_conserving"]["makespan"]
        )
        assert row["optional_idle"]["lower_bounds"]["combined"] <= (
            row["optional_idle"]["makespan"]
        )
