"""Stage-2 study of single-bottleneck parallel communication chains.

This module compares polynomial priority rules with pseudo-polynomial exact
DP, binary-search feasibility DP, bounded beam search, one-step rollout, and a
simple Monte-Carlo schedule sampler.  It also reports execution characteristics
and restricted-case experiments.

The active non-preemptive algorithms import the instance generators and
selected search helpers from this module.  Use the public scenario runner
rather than this module's historical command-line entry point.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import asdict, dataclass
from functools import lru_cache
from itertools import product
import json
import math
from pathlib import Path
import random
from statistics import mean
import sys
from time import perf_counter
from typing import Callable


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from DAG_heuristic.single_channel.parallel_chain.legacy_types import (  # noqa: E402
    Chain,
    State,
    _advance,
    _finished,
    _initial_state,
    _ready,
    _tail,
)


@dataclass
class ScheduleMetrics:
    makespan: int
    decisions: list[int | None]
    network_busy: int
    network_idle: int
    compute_active_ticks: int
    compute_capacity_ticks: int
    overlap_ticks: int
    preemptions: int
    runtime_ms: float = 0.0

    @property
    def network_idle_fraction(self) -> float:
        return self.network_idle / max(self.makespan, 1)

    @property
    def compute_idle_fraction(self) -> float:
        return 1 - self.compute_active_ticks / max(self.compute_capacity_ticks, 1)

    @property
    def overlap_fraction(self) -> float:
        return self.overlap_ticks / max(self.makespan, 1)


@dataclass
class SearchResult:
    makespan: int
    decisions: list[int | None]
    explored_states: int
    runtime_ms: float


Selector = Callable[[tuple[Chain, ...], State, list[int], int], int]


def _is_finished(chains: tuple[Chain, ...], state: State) -> bool:
    return all(_finished(chain, item) for chain, item in zip(chains, state))


def residual_bounds(chains: tuple[Chain, ...], state: State) -> dict[str, int]:
    """P/Q/L bounds for a residual independent-chain state."""

    communication = 0
    pure_compute = 0
    chain_length = 0
    for chain, (operation, remaining, cooldown) in zip(chains, state):
        if operation == len(chain.comm):
            communication_chain = 0
            compute_chain = cooldown
        else:
            communication_chain = remaining + sum(chain.comm[operation + 1:])
            compute_chain = cooldown + sum(chain.delay[operation:])
        communication += communication_chain
        pure_compute = max(pure_compute, compute_chain)
        chain_length = max(chain_length, communication_chain + compute_chain)
    return {
        "P": communication,
        "Q": pure_compute,
        "L": chain_length,
        "combined": max(communication, pure_compute, chain_length),
    }


def _remaining_comm(state: State, index: int) -> int:
    return state[index][1]


def _next_delay(chains: tuple[Chain, ...], state: State, index: int) -> int:
    return chains[index].delay[state[index][0]]


def _select_tictac(chains: tuple[Chain, ...], state: State, ready: list[int]) -> int:
    """Pairwise local comparator inspired by TIC/TAC induction.

    For two candidates, compare completing A then B against B then A using the
    two newly released compute tails.  Apply the comparator as a deterministic
    tournament when more than two flows are ready.
    """

    winner = ready[0]
    for candidate in ready[1:]:
        a, b = winner, candidate
        pa, pb = _remaining_comm(state, a), _remaining_comm(state, b)
        qa, qb = _next_delay(chains, state, a), _next_delay(chains, state, b)
        ab = max(pa + qa, pa + pb + qb)
        ba = max(pb + qb, pb + pa + qa)
        if ba < ab or (ba == ab and b < a):
            winner = b
    return winner


def priority_selector(policy: str) -> Selector:
    def select(chains: tuple[Chain, ...], state: State, ready: list[int], _elapsed: int) -> int:
        if policy == "fifo":
            return min(ready)
        if policy == "spt":
            return min(ready, key=lambda index: (_remaining_comm(state, index), index))
        if policy == "lpt":
            return max(ready, key=lambda index: (_remaining_comm(state, index), -index))
        if policy == "longest_delay":
            return max(ready, key=lambda index: (_next_delay(chains, state, index), -index))
        if policy == "longest_tail":
            return max(
                ready,
                key=lambda index: (_tail(chains[index], state[index], False), -index),
            )
        if policy in {"lrpt", "earliest_slack"}:
            # In independent chains with a common makespan objective, minimum
            # latest-start slack is algebraically the same order as LRPT.
            return max(
                ready,
                key=lambda index: (_tail(chains[index], state[index], True), -index),
            )
        if policy == "tictac":
            return _select_tictac(chains, state, ready)
        raise ValueError(f"unknown chain policy: {policy}")

    return select


BASE_POLICIES = (
    "fifo",
    "spt",
    "lpt",
    "longest_delay",
    "longest_tail",
    "lrpt",
    "earliest_slack",
    "tictac",
)


def simulate(
    chains: tuple[Chain, ...],
    selector: Selector,
    *,
    initial_state: State | None = None,
    elapsed_offset: int = 0,
) -> ScheduleMetrics:
    started = perf_counter()
    state = _initial_state(chains) if initial_state is None else initial_state
    decisions: list[int | None] = []
    network_busy = 0
    network_idle = 0
    compute_active_ticks = 0
    overlap_ticks = 0
    preemptions = 0
    previous_selected: int | None = None
    elapsed = elapsed_offset
    while not _is_finished(chains, state):
        ready = _ready(chains, state)
        selected = selector(chains, state, ready, elapsed) if ready else None
        active_compute = sum(item[2] > 0 for item in state)
        compute_active_ticks += active_compute
        if selected is None:
            network_idle += 1
        else:
            network_busy += 1
            if active_compute:
                overlap_ticks += 1
            if (
                previous_selected is not None
                and previous_selected != selected
                and state[previous_selected][1] > 0
                and state[previous_selected][2] == 0
            ):
                preemptions += 1
        decisions.append(selected)
        state = _advance(chains, state, selected)
        previous_selected = selected
        elapsed += 1
    makespan = len(decisions)
    return ScheduleMetrics(
        makespan,
        decisions,
        network_busy,
        network_idle,
        compute_active_ticks,
        makespan * len(chains),
        overlap_ticks,
        preemptions,
        (perf_counter() - started) * 1000,
    )


def _greedy_remaining(
    chains: tuple[Chain, ...],
    state: State,
    policy: str = "longest_tail",
) -> int:
    return simulate(chains, priority_selector(policy), initial_state=state).makespan


def rollout_selector(top_k: int = 4, base_policy: str = "longest_tail") -> Selector:
    base = priority_selector(base_policy)

    def select(chains: tuple[Chain, ...], state: State, ready: list[int], elapsed: int) -> int:
        ranked = sorted(
            ready,
            key=lambda index: (_tail(chains[index], state[index], False), -index),
            reverse=True,
        )[:top_k]
        return min(
            ranked,
            key=lambda index: (
                1 + _greedy_remaining(chains, _advance(chains, state, index), base_policy),
                -_tail(chains[index], state[index], False),
                index,
            ),
        )

    return select


def _canonical_state(chains: tuple[Chain, ...], state: State) -> State:
    """Sort states only within groups of structurally identical chains."""

    values = list(state)
    groups: dict[Chain, list[int]] = defaultdict(list)
    for index, chain in enumerate(chains):
        groups[chain].append(index)
    for indices in groups.values():
        ordered = sorted(values[index] for index in indices)
        for index, item in zip(indices, ordered):
            values[index] = item
    return tuple(values)


def _canonicalizer(chains: tuple[Chain, ...]) -> Callable[[State], State]:
    """Precompute identical-chain symmetry groups for hot search loops."""

    grouped: dict[Chain, list[int]] = defaultdict(list)
    for index, chain in enumerate(chains):
        grouped[chain].append(index)
    groups = tuple(tuple(indices) for indices in grouped.values() if len(indices) > 1)
    if not groups:
        return lambda state: state

    def canonical(state: State) -> State:
        values = list(state)
        for indices in groups:
            ordered = sorted(values[index] for index in indices)
            for index, item in zip(indices, ordered):
                values[index] = item
        return tuple(values)

    return canonical


def pseudo_polynomial_dp(
    chains: tuple[Chain, ...],
    *,
    max_states: int = 2_000_000,
    symmetry: bool = True,
) -> SearchResult:
    """Exact unit-time DP, pseudo-polynomial in numeric durations."""

    started = perf_counter()
    canonical = _canonicalizer(chains) if symmetry else (lambda state: state)
    choice: dict[State, int | None] = {}
    explored = 0

    @lru_cache(maxsize=None)
    def solve(raw_state: State) -> int:
        nonlocal explored
        state = canonical(raw_state)
        explored += 1
        if explored > max_states:
            raise RuntimeError(f"pseudo-polynomial DP exceeded {max_states} states")
        if _is_finished(chains, state):
            return 0
        ready = _ready(chains, state)
        candidates: list[int | None] = ready if ready else [None]
        best = sys.maxsize
        selected_best = None
        for selected in candidates:
            successor = _advance(chains, state, selected)
            successor = canonical(successor)
            value = 1 + solve(successor)
            if value < best:
                best = value
                selected_best = selected
        choice[state] = selected_best
        return best

    initial = canonical(_initial_state(chains))
    makespan = solve(initial)
    decisions: list[int | None] = []
    state = initial
    while not _is_finished(chains, state):
        selected = choice[state]
        decisions.append(selected)
        state = _advance(chains, state, selected)
        state = canonical(state)
    return SearchResult(makespan, decisions, explored, (perf_counter() - started) * 1000)


def _feasible_within(
    chains: tuple[Chain, ...],
    horizon: int,
    *,
    max_states: int,
) -> tuple[bool, int]:
    explored = 0
    canonical = _canonicalizer(chains)

    @lru_cache(maxsize=None)
    def feasible(raw_state: State, budget: int) -> bool:
        nonlocal explored
        state = canonical(raw_state)
        explored += 1
        if explored > max_states:
            raise RuntimeError(f"binary feasibility DP exceeded {max_states} states")
        if _is_finished(chains, state):
            return True
        if budget <= 0 or residual_bounds(chains, state)["combined"] > budget:
            return False
        ready = _ready(chains, state)
        candidates: list[int | None] = ready if ready else [None]
        candidates.sort(
            key=lambda index: (
                -_tail(chains[index], state[index], False), index
            ) if index is not None else (0, 0)
        )
        return any(
            feasible(canonical(_advance(chains, state, selected)), budget - 1)
            for selected in candidates
        )

    return feasible(canonical(_initial_state(chains)), horizon), explored


def binary_search_exact(
    chains: tuple[Chain, ...],
    *,
    max_states: int = 2_000_000,
) -> SearchResult:
    """Exact makespan via binary search plus pseudo-polynomial feasibility DP."""

    started = perf_counter()
    lower = residual_bounds(chains, _initial_state(chains))["combined"]
    upper = simulate(chains, rollout_selector(4)).makespan
    explored = 0
    while lower < upper:
        middle = (lower + upper) // 2
        feasible, states = _feasible_within(chains, middle, max_states=max_states)
        explored += states
        if feasible:
            upper = middle
        else:
            lower = middle + 1
    return SearchResult(lower, [], explored, (perf_counter() - started) * 1000)


def beam_search(chains: tuple[Chain, ...], *, width: int = 32) -> SearchResult:
    """Bounded non-polynomial search; width controls quality/runtime."""

    started = perf_counter()
    canonical = _canonicalizer(chains)
    initial = canonical(_initial_state(chains))
    frontier: dict[State, list[int | None]] = {initial: []}
    explored = 0
    elapsed = 0
    while frontier:
        completed = [path for state, path in frontier.items() if _is_finished(chains, state)]
        if completed:
            path = min(completed, key=len)
            return SearchResult(elapsed, path, explored, (perf_counter() - started) * 1000)
        successors: dict[State, list[int | None]] = {}
        for state, path in frontier.items():
            ready = _ready(chains, state)
            for selected in (ready if ready else [None]):
                successor = canonical(_advance(chains, state, selected))
                successors.setdefault(successor, [*path, selected])
                explored += 1
        if len(successors) > width:
            ranked = sorted(
                successors,
                key=lambda state: (
                    residual_bounds(chains, state)["combined"],
                    _greedy_remaining(chains, state),
                ),
            )[:width]
            frontier = {state: successors[state] for state in ranked}
        else:
            frontier = successors
        elapsed += 1
    raise RuntimeError("beam search exhausted its frontier")


def monte_carlo_best(
    chains: tuple[Chain, ...],
    *,
    samples: int = 64,
    seed: int = 0,
    greedy_probability: float = 0.7,
) -> SearchResult:
    """Sample work-conserving schedules and return the best observed one."""

    started = perf_counter()
    rng = random.Random(seed)
    best: ScheduleMetrics | None = None
    for _ in range(samples):
        def selector(
            local_chains: tuple[Chain, ...],
            state: State,
            ready: list[int],
            _elapsed: int,
        ) -> int:
            if rng.random() < greedy_probability:
                best_tail = max(_tail(local_chains[index], state[index], False) for index in ready)
                near_best = [
                    index for index in ready
                    if _tail(local_chains[index], state[index], False) >= best_tail - 1
                ]
                return rng.choice(near_best)
            return rng.choice(ready)

        result = simulate(chains, selector)
        if best is None or result.makespan < best.makespan:
            best = result
    assert best is not None
    return SearchResult(
        best.makespan,
        best.decisions,
        samples,
        (perf_counter() - started) * 1000,
    )


def random_instance(
    rng: random.Random,
    *,
    min_chains: int = 2,
    max_chains: int = 5,
    max_operations: int = 3,
    max_comm: int = 4,
    max_delay: int = 6,
) -> tuple[Chain, ...]:
    return tuple(
        Chain(
            tuple(rng.randint(1, max_comm) for _ in range(operations)),
            tuple(rng.randint(0, max_delay) for _ in range(operations)),
        )
        for operations in (
            rng.randint(1, max_operations)
            for _ in range(rng.randint(min_chains, max_chains))
        )
    )


def scaled_tail_counterexample(scale: int) -> tuple[Chain, ...]:
    return (
        Chain((2 * scale, scale), (3 * scale, scale)),
        Chain((scale, 2 * scale), (2 * scale, scale)),
    )


def scaled_five_four_counterexample(scale: int) -> tuple[Chain, ...]:
    """Two-chain family separating tail/rollout/fixed-width beam from OPT.

    OPT takes ``8 * scale + 1`` ticks by starting the second chain.  The first
    chain has a *strictly* larger initial tail, so Longest-tail starts it and
    nearly synchronizes both releases, producing ``10 * scale``.  One-step
    rollout inherits that decision for ``scale >= 2`` because a one-tick
    deviation is followed by the same base policy.  A beam of width ``B`` also
    loses the continuously-progressed optimal branch when ``scale > B`` in the
    current deterministic ranking.  Thus all ratios converge to 5/4 without
    relying on a priority tie.
    """

    if scale < 1:
        raise ValueError("scale must be positive")
    return (
        Chain((2 * scale, 2 * scale), (3 * scale + 1, 0)),
        Chain((scale, 3 * scale), (2 * scale, 0)),
    )


def tight_two_family(length: int) -> tuple[Chain, ...]:
    return (
        Chain((length,), (0,)),
        Chain((1, 1), (length, 0)),
    )


def _percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    return ordered[round((len(ordered) - 1) * quantile)]


def evaluate_random(samples: int, seed: int) -> dict:
    rng = random.Random(seed)
    policy_names = (*BASE_POLICIES, "rollout2", "rollout4", "beam8", "beam32", "mc64")
    records: dict[str, list[dict]] = defaultdict(list)
    binary_records = []
    direct_records = []
    worst: dict[str, dict] = {}
    for sample in range(samples):
        chains = random_instance(rng)
        exact = pseudo_polynomial_dp(chains)
        direct_records.append({"states": exact.explored_states, "runtime_ms": exact.runtime_ms})
        if sample < min(samples, 40):
            binary = binary_search_exact(chains)
            if binary.makespan != exact.makespan:
                raise AssertionError("direct DP and binary feasibility DP disagree")
            binary_records.append({"states": binary.explored_states, "runtime_ms": binary.runtime_ms})

        results: dict[str, ScheduleMetrics | SearchResult] = {
            policy: simulate(chains, priority_selector(policy)) for policy in BASE_POLICIES
        }
        results["rollout2"] = simulate(chains, rollout_selector(2))
        results["rollout4"] = simulate(chains, rollout_selector(4))
        results["beam8"] = beam_search(chains, width=8)
        results["beam32"] = beam_search(chains, width=32)
        results["mc64"] = monte_carlo_best(chains, samples=64, seed=seed + sample)
        for name in policy_names:
            result = results[name]
            ratio = result.makespan / exact.makespan
            record = {
                "ratio": ratio,
                "runtime_ms": result.runtime_ms,
                "preemptions": getattr(result, "preemptions", None),
                "network_idle_fraction": getattr(result, "network_idle_fraction", None),
                "compute_idle_fraction": getattr(result, "compute_idle_fraction", None),
                "overlap_fraction": getattr(result, "overlap_fraction", None),
            }
            records[name].append(record)
            if name not in worst or ratio > worst[name]["ratio"]:
                worst[name] = {
                    "ratio": ratio,
                    "makespan": result.makespan,
                    "optimum": exact.makespan,
                    "chains": [asdict(chain) for chain in chains],
                }

    summary = {}
    for name, values in records.items():
        ratios = [item["ratio"] for item in values]
        metric = lambda key: [item[key] for item in values if item[key] is not None]
        summary[name] = {
            "mean_ratio": mean(ratios),
            "p50_ratio": _percentile(ratios, 0.50),
            "p95_ratio": _percentile(ratios, 0.95),
            "max_ratio": max(ratios),
            "mean_runtime_ms": mean(metric("runtime_ms")),
            "mean_preemptions": mean(metric("preemptions")) if metric("preemptions") else None,
            "mean_network_idle_fraction": mean(metric("network_idle_fraction")) if metric("network_idle_fraction") else None,
            "mean_compute_idle_fraction": mean(metric("compute_idle_fraction")) if metric("compute_idle_fraction") else None,
            "mean_overlap_fraction": mean(metric("overlap_fraction")) if metric("overlap_fraction") else None,
            "worst": worst[name],
        }
    return {
        "samples": samples,
        "seed": seed,
        "algorithms": summary,
        "exact_dp": {
            "direct_mean_states": mean(item["states"] for item in direct_records),
            "direct_p95_states": _percentile(
                [item["states"] for item in direct_records], 0.95
            ),
            "direct_mean_runtime_ms": mean(item["runtime_ms"] for item in direct_records),
            "direct_p95_runtime_ms": _percentile(
                [item["runtime_ms"] for item in direct_records], 0.95
            ),
            "binary_cross_checks": len(binary_records),
            "binary_mean_states": mean(item["states"] for item in binary_records),
            "binary_mean_runtime_ms": mean(item["runtime_ms"] for item in binary_records),
        },
    }


def restricted_cases(seed: int) -> dict:
    rng = random.Random(seed)
    cases: dict[str, dict] = {}

    def run(
        name: str,
        instances: list[tuple[Chain, ...]],
        policies: tuple[str, ...],
        known_optimum: Callable[[tuple[Chain, ...]], int] | None = None,
    ) -> None:
        worst = {policy: 1.0 for policy in policies}
        optimal_count = {policy: 0 for policy in policies}
        for chains in instances:
            optimum = (
                known_optimum(chains)
                if known_optimum is not None
                else pseudo_polynomial_dp(chains, max_states=300_000).makespan
            )
            for policy in policies:
                value = simulate(chains, priority_selector(policy)).makespan
                worst[policy] = max(worst[policy], value / optimum)
                optimal_count[policy] += value == optimum
        cases[name] = {
            "instances": len(instances),
            "worst_ratio": worst,
            "optimal_fraction": {
                policy: optimal_count[policy] / len(instances) for policy in policies
            },
        }

    one_flow = [
        tuple(
            Chain((rng.randint(1, 6),), (rng.randint(0, 10),))
            for _ in range(rng.randint(2, 8))
        )
        for _ in range(200)
    ]
    run(
        "one_flow_per_chain",
        one_flow,
        ("longest_delay", "longest_tail", "lrpt"),
        known_optimum=lambda chains: simulate(
            chains, priority_selector("longest_delay")
        ).makespan,
    )

    zero_delay = [
        tuple(
            Chain(
                tuple(rng.randint(1, 5) for _ in range(length)),
                tuple(0 for _ in range(length)),
            )
            for length in (rng.randint(1, 3) for _ in range(rng.randint(2, 6)))
        )
        for _ in range(100)
    ]
    run(
        "zero_compute_delay",
        zero_delay,
        BASE_POLICIES,
        known_optimum=lambda chains: sum(sum(chain.comm) for chain in chains),
    )

    equal_comm = [
        tuple(
            Chain((1, 1), (delays[index * 2], delays[index * 2 + 1]))
            for index in range(3)
        )
        for delays in product(range(3), repeat=6)
    ]
    run("equal_communication", equal_comm, ("longest_delay", "longest_tail", "lrpt"))

    monotone_shapes = [
        (communications, delays)
        for communications in product((1, 2), repeat=2)
        for delays in product(range(3), repeat=2)
        if delays[0] >= delays[1]
    ]
    monotone_delay = [
        tuple(Chain(communications, delays) for communications, delays in pair)
        for pair in product(monotone_shapes, repeat=2)
    ]
    run("nonincreasing_compute_lags", monotone_delay, ("longest_delay", "longest_tail", "lrpt"))
    return cases


def scale_study(seed: int) -> list[dict]:
    rng = random.Random(seed)
    rows = []
    for chain_count in (4, 6, 8, 10):
        for repetition in range(1):
            chains = random_instance(
                rng,
                min_chains=chain_count,
                max_chains=chain_count,
                max_operations=3,
                max_comm=3,
                max_delay=5,
            )
            row = {"chains": chain_count, "repetition": repetition}
            try:
                exact = pseudo_polynomial_dp(chains, max_states=30_000)
                row.update({
                    "exact": exact.makespan,
                    "dp_states": exact.explored_states,
                    "dp_runtime_ms": exact.runtime_ms,
                })
            except RuntimeError:
                row.update({"exact": None, "dp_states": None, "dp_runtime_ms": None})
            for width in (8, 32):
                beam = beam_search(chains, width=width)
                row[f"beam{width}"] = beam.makespan
                row[f"beam{width}_runtime_ms"] = beam.runtime_ms
            mc = monte_carlo_best(chains, samples=64, seed=seed + chain_count * 10 + repetition)
            row["mc64"] = mc.makespan
            row["mc64_runtime_ms"] = mc.runtime_ms
            rows.append(row)
    return rows


def counterexample_report() -> dict:
    tail_family = []
    for scale in (1, 2, 4, 8):
        chains = scaled_tail_counterexample(scale)
        optimum = pseudo_polynomial_dp(chains).makespan
        tail_family.append({
            "scale": scale,
            "optimum": optimum,
            "longest_tail": simulate(chains, priority_selector("longest_tail")).makespan,
            "lrpt": simulate(chains, priority_selector("lrpt")).makespan,
        })
    two_family = []
    for length in (10, 20, 50, 100):
        chains = tight_two_family(length)
        optimum = pseudo_polynomial_dp(chains).makespan
        bad = simulate(chains, priority_selector("lpt")).makespan
        two_family.append({
            "length": length,
            "optimum": optimum,
            "lpt": bad,
            "ratio": bad / optimum,
        })
    return {"tail_9_over_8_family": tail_family, "generic_two_tight_family": two_family}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--seed", type=int, default=260813)
    parser.add_argument("--out", default="outputs/parallel_chain_study/report.json")
    args = parser.parse_args()
    if args.samples < 1:
        parser.error("--samples must be positive")

    report = {
        "model": {
            "channel": "single unit-capacity preemptive bottleneck",
            "compute": "ready compute progresses immediately and in parallel",
            "complexity_parameters": ["ready chain count", "flows per chain", "integer P"],
        },
        "random_evaluation": evaluate_random(args.samples, args.seed),
        "restricted_cases": restricted_cases(args.seed + 1),
        "scale_study": scale_study(args.seed + 2),
        "counterexamples": counterexample_report(),
    }
    output = ROOT / args.out
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Report: {output}")
    for name, values in report["random_evaluation"]["algorithms"].items():
        print(
            f"{name:16} mean={values['mean_ratio']:.4f} "
            f"p95={values['p95_ratio']:.4f} max={values['max_ratio']:.4f} "
            f"runtime={values['mean_runtime_ms']:.3f}ms"
        )


if __name__ == "__main__":
    main()
