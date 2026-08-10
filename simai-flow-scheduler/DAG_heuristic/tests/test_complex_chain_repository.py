from DAG_heuristic.single_channel.complex_chain.interface import ALGORITHMS
from DAG_heuristic.single_channel.complex_chain.testsets import cases


def test_three_complex_chain_testset_categories_run() -> None:
    selected = [
        cases("random", samples=1, seed=7)[0],
        cases("adversarial")[0],
        cases("real")[0],
    ]
    for case in selected:
        result = ALGORITHMS["longest_tail"](case.instance)
        assert result.makespan > 0

