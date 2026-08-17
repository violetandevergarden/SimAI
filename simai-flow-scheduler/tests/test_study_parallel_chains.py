"""Regression and theory checks for the stage-2 parallel-chain study."""

import random

from scripts.evaluate_chain_heuristics import Chain, optimal_makespan
from scripts.study_parallel_chains import (
    beam_search,
    binary_search_exact,
    monte_carlo_best,
    priority_selector,
    pseudo_polynomial_dp,
    random_instance,
    residual_bounds,
    rollout_selector,
    scaled_five_four_counterexample,
    scaled_tail_counterexample,
    simulate,
    tight_two_family,
)


def test_two_exact_chain_oracles_agree() -> None:
    chains = (
        Chain((2, 1), (3, 1)),
        Chain((1, 2), (2, 1)),
    )

    direct = pseudo_polynomial_dp(chains)
    binary = binary_search_exact(chains)

    assert direct.makespan == optimal_makespan(chains) == 8
    assert binary.makespan == direct.makespan
    assert residual_bounds(chains, tuple((0, chain.comm[0], 0) for chain in chains))["combined"] <= 8


def test_scaled_longest_tail_and_lrpt_counterexample_is_nine_over_eight() -> None:
    for scale in (1, 2, 4):
        chains = scaled_tail_counterexample(scale)
        optimum = pseudo_polynomial_dp(chains).makespan

        assert optimum == 8 * scale
        assert simulate(chains, priority_selector("longest_tail")).makespan == 9 * scale
        assert simulate(chains, priority_selector("lrpt")).makespan == 9 * scale


def test_generic_work_conserving_bound_can_approach_two() -> None:
    chains = tight_two_family(100)
    optimum = pseudo_polynomial_dp(chains).makespan
    lpt = simulate(chains, priority_selector("lpt")).makespan

    assert optimum == 102
    assert lpt == 202
    assert lpt / optimum > 1.98
    assert lpt < 2 * optimum


def test_tail_and_rollout_have_a_scalable_five_four_lower_bound() -> None:
    for scale in (2, 4, 8):
        chains = scaled_five_four_counterexample(scale)
        optimum = pseudo_polynomial_dp(chains).makespan

        assert optimum == 8 * scale + 1
        assert simulate(chains, priority_selector("longest_tail")).makespan == 10 * scale
        assert simulate(chains, rollout_selector(2)).makespan == 10 * scale


def test_fixed_width_beam_loses_five_four_family_when_scale_exceeds_width() -> None:
    width = 8
    scale = width + 1
    chains = scaled_five_four_counterexample(scale)

    assert beam_search(chains, width=width).makespan == 10 * scale


def test_top_two_rollout_never_worsens_its_base_policy() -> None:
    rng = random.Random(260815)
    for _ in range(50):
        chains = random_instance(rng, max_chains=4, max_operations=3)
        base = simulate(chains, priority_selector("longest_tail")).makespan
        rollout = simulate(chains, rollout_selector(2)).makespan

        assert rollout <= base


def test_one_flow_longest_tail_is_optimal() -> None:
    rng = random.Random(260813)
    for _ in range(20):
        chains = tuple(
            Chain((rng.randint(1, 6),), (rng.randint(0, 10),))
            for _ in range(rng.randint(2, 6))
        )
        optimum = pseudo_polynomial_dp(chains).makespan

        assert simulate(chains, priority_selector("longest_tail")).makespan == optimum


def test_bounded_searches_return_feasible_schedules() -> None:
    chains = random_instance(random.Random(7), max_chains=4)
    optimum = pseudo_polynomial_dp(chains).makespan
    rollout = simulate(chains, rollout_selector(4))
    beam = beam_search(chains, width=16)
    monte_carlo = monte_carlo_best(chains, samples=32, seed=7)

    assert rollout.makespan >= optimum
    assert beam.makespan >= optimum
    assert monte_carlo.makespan >= optimum
    assert beam.makespan <= simulate(chains, priority_selector("lpt")).makespan


def test_schedule_metrics_and_two_bound() -> None:
    chains = random_instance(random.Random(9), max_chains=4)
    total_communication = sum(sum(chain.comm) for chain in chains)
    q = max(sum(chain.delay) for chain in chains)

    for policy in ("fifo", "spt", "lpt", "longest_tail", "tictac"):
        result = simulate(chains, priority_selector(policy))
        assert result.network_busy == total_communication
        assert result.network_busy + result.network_idle == result.makespan
        assert result.makespan <= total_communication + q
        assert 0 <= result.overlap_fraction <= 1


def test_identical_chain_symmetry_preserves_optimum_and_reduces_states() -> None:
    chain = Chain((2, 1), (2, 1))
    chains = (chain, chain, chain, Chain((1,), (3,)))

    symmetric = pseudo_polynomial_dp(chains, symmetry=True)
    plain = pseudo_polynomial_dp(chains, symmetry=False)

    assert symmetric.makespan == plain.makespan
    assert symmetric.explored_states <= plain.explored_states


def test_earliest_slack_and_lrpt_are_equivalent_in_chain_model() -> None:
    chains = random_instance(random.Random(11), max_chains=4)

    earliest_slack = simulate(chains, priority_selector("earliest_slack"))
    lrpt = simulate(chains, priority_selector("lrpt"))

    assert earliest_slack.decisions == lrpt.decisions
