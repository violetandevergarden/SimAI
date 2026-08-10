from DAG_heuristic.common.interface import AlgorithmSpec
from DAG_heuristic.single_channel.parallel_chain.interface import (
    ALGORITHMS as PARALLEL_ALGORITHMS,
    register as register_parallel,
)


def test_parallel_registry_rejects_duplicate_names() -> None:
    duplicate = AlgorithmSpec("longest_tail", lambda case: case, "duplicate")
    try:
        register_parallel(duplicate)
    except ValueError as error:
        assert "already registered" in str(error)
    else:
        raise AssertionError("duplicate algorithm registration must fail")


def test_parallel_registry_exposes_our_algorithms() -> None:
    assert {"longest_tail", "rollout_wait2", "beam_wait8", "exact_optional"} <= set(
        PARALLEL_ALGORITHMS
    )

