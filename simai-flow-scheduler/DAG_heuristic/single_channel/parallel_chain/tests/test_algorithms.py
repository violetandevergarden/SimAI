"""R2 non-preemptive parallel-chain regression tests."""

import random

from DAG_heuristic.common.oracle import exact_oracle
from DAG_heuristic.single_channel.parallel_chain.algorithms import (
    ParallelChain,
    binary_search_exact,
    beam_search,
    exact_dp,
    evaluate_random,
    fixed_beam_counterexample,
    monte_carlo_best,
    schedule_priority,
    schedule_rollout,
    scaled_five_four_family,
    tight_optional_wait_family,
    to_benchmark_dag,
    verify_schedule,
    restricted_cases,
)
from DAG_heuristic.single_channel.parallel_chain.legacy_generator import random_instance


def test_compact_dp_matches_r1_dag_oracle_in_both_idle_modes() -> None:
    rng = random.Random(260813)
    for _ in range(12):
        chains = tuple(
            ParallelChain.from_legacy(chain) for chain in random_instance(rng)
        )
        dag, _flow_ids = to_benchmark_dag(chains)
        for optional_idle, mode in (
            (True, "optional_idle"),
            (False, "work_conserving"),
        ):
            compact = exact_dp(chains, optional_idle=optional_idle)
            general = exact_oracle(dag, mode=mode)
            assert compact.makespan == general.makespan


def test_binary_feasibility_matches_direct_operation_dp() -> None:
    rng = random.Random(19)
    for _ in range(8):
        chains = tuple(
            ParallelChain.from_legacy(chain)
            for chain in random_instance(rng, max_chains=4)
        )
        for optional_idle in (False, True):
            direct = exact_dp(chains, optional_idle=optional_idle)
            binary = binary_search_exact(chains, optional_idle=optional_idle)
            assert binary.makespan == direct.makespan


def test_optional_wait_family_makes_dynamic_tail_tight_at_two() -> None:
    for magnitude in (10, 100):
        chains = tight_optional_wait_family(magnitude)
        optimum = exact_dp(chains, optional_idle=True)
        work_conserving = schedule_priority(chains, "dynamic_tail")
        rollout = schedule_rollout(chains, top_k=2, allow_wait=True)

        assert optimum.makespan == magnitude + 2
        assert work_conserving.makespan == 2 * magnitude + 1
        assert rollout.makespan == optimum.makespan
        verify_schedule(chains, work_conserving)
        verify_schedule(chains, rollout)


def test_scaled_five_four_ordering_counterexample_survives() -> None:
    for scale in (2, 4, 8):
        chains = scaled_five_four_family(scale)
        optimum = exact_dp(chains, optional_idle=True)
        dynamic = schedule_priority(chains, "dynamic_tail")

        assert optimum.makespan == 8 * scale + 1
        assert dynamic.makespan == 10 * scale
        verify_schedule(chains, dynamic)


def test_wait_rollout_never_worsens_flow_only_rollout_on_fixed_sample() -> None:
    rng = random.Random(260813)
    for _ in range(30):
        chains = tuple(
            ParallelChain.from_legacy(chain) for chain in random_instance(rng)
        )
        flow_only = schedule_rollout(chains, top_k=2, allow_wait=False)
        wait_aware = schedule_rollout(chains, top_k=2, allow_wait=True)
        assert wait_aware.makespan <= flow_only.makespan
        verify_schedule(chains, wait_aware)


def test_bounded_monte_carlo_keeps_dynamic_incumbent_and_is_atomic() -> None:
    chains = (
        ParallelChain((2, 1), (3, 1)),
        ParallelChain((1, 2), (2, 1)),
    )
    dynamic = schedule_priority(chains, "dynamic_tail")
    sampled = monte_carlo_best(chains, samples=32, seed=7, allow_wait=True)

    assert sampled.makespan <= dynamic.makespan
    verify_schedule(chains, sampled)


def test_fixed_width_beams_are_not_exact_under_whole_flow_actions() -> None:
    chains = fixed_beam_counterexample()
    optimum = exact_dp(chains, optional_idle=True)
    beam8 = beam_search(chains, width=8, allow_wait=True)
    beam32 = beam_search(chains, width=32, allow_wait=True)

    assert optimum.makespan == 46
    assert beam8.makespan == 47
    assert beam32.makespan == 47
    verify_schedule(chains, beam8)
    verify_schedule(chains, beam32)


def test_one_flow_initially_ready_dynamic_tail_is_optimal() -> None:
    rng = random.Random(23)
    for _ in range(20):
        chains = tuple(
            ParallelChain((rng.randint(1, 8),), (rng.randint(0, 12),))
            for _ in range(rng.randint(2, 7))
        )
        optimum = exact_dp(chains, optional_idle=True)
        dynamic = schedule_priority(chains, "dynamic_tail")
        assert dynamic.makespan == optimum.makespan


def test_every_work_conserving_priority_respects_p_plus_q_bound() -> None:
    rng = random.Random(29)
    for _ in range(30):
        chains = tuple(
            ParallelChain.from_legacy(chain) for chain in random_instance(rng)
        )
        total_communication = sum(sum(chain.comm) for chain in chains)
        max_compute = max(sum(chain.compute) for chain in chains)
        optimum = exact_dp(chains, optional_idle=True).makespan
        for policy in ("fifo", "spt", "lpt", "dynamic_tail", "tictac"):
            schedule = schedule_priority(chains, policy)
            assert schedule.makespan <= total_communication + max_compute
            assert schedule.makespan <= 2 * optimum
            assert schedule.preemptions == 0


def test_bounded_searches_fall_back_to_dynamic_incumbent() -> None:
    chains = fixed_beam_counterexample()
    dynamic = schedule_priority(chains, "dynamic_tail")
    rollout = schedule_rollout(
        chains, top_k=2, allow_wait=True, time_limit_s=-1
    )
    beam = beam_search(
        chains, width=8, allow_wait=True, state_budget=0
    )

    assert rollout.fallback is True
    assert beam.fallback is True
    assert rollout.makespan == dynamic.makespan
    assert beam.makespan == dynamic.makespan
    verify_schedule(chains, rollout)
    verify_schedule(chains, beam)


def test_full_factor_matrix_contains_every_old_search_configuration() -> None:
    report = evaluate_random(3, 260813)
    expected = {
        "rollout_flow2",
        "rollout_flow4",
        "rollout_wait2",
        "rollout_wait4",
        "beam_flow8",
        "beam_flow32",
        "beam_wait8",
        "beam_wait32",
        "mc_flow64",
        "mc_wait64",
    }
    assert expected <= report["algorithms"].keys()
    assert all(report["algorithms"][name]["fallbacks"] == 0 for name in expected)


def test_restricted_case_matrix_is_reproduced_nonpreemptively() -> None:
    cases = restricted_cases(260814)

    assert cases["one_flow_per_chain_initially_ready"]["instances"] == 200
    assert cases["zero_compute_delay_initially_ready"]["instances"] == 100
    assert cases["equal_communication"]["instances"] == 729
    assert cases["nonincreasing_compute_lags"]["instances"] == 576
    assert (
        cases["one_flow_per_chain_initially_ready"]["worst_ratio"][
            "dynamic_tail"
        ]
        == 1.0
    )
    assert all(
        ratio == 1.0
        for ratio in cases["zero_compute_delay_initially_ready"][
            "worst_ratio"
        ].values()
    )
