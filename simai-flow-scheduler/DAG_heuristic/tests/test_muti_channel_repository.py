from DAG_heuristic.muti_channel.interface import ALGORITHMS
from DAG_heuristic.muti_channel.testsets import cases


def test_three_muti_channel_testset_categories_run() -> None:
    selected = [
        cases("random", samples=1, seed=7)[0],
        cases("adversarial")[0],
        cases("real")[0],
    ]
    for case in selected:
        result = ALGORITHMS["longest_tail_pack"](case.instance)
        assert result.makespan > 0

