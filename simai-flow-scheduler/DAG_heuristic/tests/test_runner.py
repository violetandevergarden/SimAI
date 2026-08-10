from DAG_heuristic.run import run


def test_unified_runner() -> None:
    report = run(
        "parallel_chain",
        "rollout_wait2",
        "adversarial",
        samples=1,
        seed=1,
    )
    assert report["algorithm"] == "rollout_wait2"
    assert report["cases"]
    assert all(row["makespan"] > 0 for row in report["cases"])
