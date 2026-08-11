"""Checks for the small parallel-chain research oracle."""

from scripts.evaluate_chain_heuristics import (
    Chain,
    heuristic_makespan,
    optimal_makespan,
)


POLICIES = ("fifo", "spt", "lpt", "longest_delay", "ltf", "lrpt")


def test_group_meeting_motivating_example() -> None:
    chains = (
        Chain((2, 1), (3, 1)),
        Chain((1, 2), (2, 1)),
    )

    assert optimal_makespan(chains) == 8
    assert heuristic_makespan(chains, "spt") == 8
    assert heuristic_makespan(chains, "ltf") == 9


def test_bad_work_conserving_order_approaches_factor_two() -> None:
    chains = (
        Chain((20,), (0,)),
        Chain((1, 1), (20, 0)),
    )

    optimum = optimal_makespan(chains)
    lpt = heuristic_makespan(chains, "lpt")

    assert optimum == 22
    assert lpt == 42
    assert lpt < 2 * optimum


def test_all_policies_respect_two_approximation_on_small_instance() -> None:
    chains = (
        Chain((2, 1, 2), (1, 3, 0)),
        Chain((1, 2), (4, 1)),
        Chain((3,), (2,)),
    )
    optimum = optimal_makespan(chains)

    for policy in POLICIES:
        assert heuristic_makespan(chains, policy) <= 2 * optimum
