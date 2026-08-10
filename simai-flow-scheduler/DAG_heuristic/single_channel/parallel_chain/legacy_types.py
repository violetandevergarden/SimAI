"""Parallel-chain instance types and historical comparison utilities.

Only the data types are part of the current algorithm path.  Scheduling uses
the non-preemptive implementation in ``algorithms.py``.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from functools import lru_cache
import random
from statistics import mean


@dataclass(frozen=True)
class Chain:
    """Alternating communication sizes and following compute delays."""

    comm: tuple[int, ...]
    delay: tuple[int, ...]

    def __post_init__(self) -> None:
        if not self.comm or len(self.comm) != len(self.delay):
            raise ValueError("comm and delay must have the same positive length")
        if any(value <= 0 for value in self.comm):
            raise ValueError("communication sizes must be positive")
        if any(value < 0 for value in self.delay):
            raise ValueError("compute delays must be non-negative")


# Per-chain state: (next communication index, remaining work, cooldown).
State = tuple[tuple[int, int, int], ...]


def _initial_state(chains: tuple[Chain, ...]) -> State:
    return tuple((0, chain.comm[0], 0) for chain in chains)


def _finished(chain: Chain, state: tuple[int, int, int]) -> bool:
    index, remaining, cooldown = state
    return index == len(chain.comm) and remaining == 0 and cooldown == 0


def _ready(chains: tuple[Chain, ...], state: State) -> list[int]:
    return [
        index
        for index, (chain, (operation, remaining, cooldown))
        in enumerate(zip(chains, state))
        if operation < len(chain.comm) and remaining > 0 and cooldown == 0
    ]


def _advance(chains: tuple[Chain, ...], state: State, selected: int | None) -> State:
    result: list[tuple[int, int, int]] = []
    for chain_index, (chain, item) in enumerate(zip(chains, state)):
        operation, remaining, cooldown = item
        if cooldown > 0:
            result.append((operation, remaining, cooldown - 1))
            continue
        if chain_index != selected:
            result.append(item)
            continue

        remaining -= 1
        if remaining > 0:
            result.append((operation, remaining, 0))
            continue

        delay = chain.delay[operation]
        operation += 1
        next_remaining = chain.comm[operation] if operation < len(chain.comm) else 0
        result.append((operation, next_remaining, delay))
    return tuple(result)


def optimal_makespan(chains: tuple[Chain, ...]) -> int:
    """Return the exact discrete-time preemptive optimum."""

    @lru_cache(maxsize=None)
    def solve(state: State) -> int:
        if all(_finished(chain, item) for chain, item in zip(chains, state)):
            return 0
        ready = _ready(chains, state)
        choices: list[int | None] = ready if ready else [None]
        return 1 + min(solve(_advance(chains, state, choice)) for choice in choices)

    return solve(_initial_state(chains))


def _tail(chain: Chain, item: tuple[int, int, int], include_current: bool) -> int:
    operation, remaining, cooldown = item
    if operation == len(chain.comm):
        return cooldown
    downstream = sum(chain.delay[operation:]) + sum(chain.comm[operation + 1:])
    return downstream + (remaining if include_current else 0)


def heuristic_makespan(chains: tuple[Chain, ...], policy: str) -> int:
    """Run one work-conserving priority policy."""

    state = _initial_state(chains)
    elapsed = 0
    while not all(_finished(chain, item) for chain, item in zip(chains, state)):
        ready = _ready(chains, state)
        selected: int | None = None
        if ready:
            if policy == "fifo":
                selected = min(ready)
            elif policy == "spt":
                selected = min(ready, key=lambda i: (state[i][1], i))
            elif policy == "lpt":
                selected = max(ready, key=lambda i: (state[i][1], -i))
            elif policy == "longest_delay":
                selected = max(
                    ready,
                    key=lambda i: (chains[i].delay[state[i][0]], -i),
                )
            elif policy == "ltf":
                selected = max(
                    ready,
                    key=lambda i: (_tail(chains[i], state[i], False), -i),
                )
            elif policy == "lrpt":
                selected = max(
                    ready,
                    key=lambda i: (_tail(chains[i], state[i], True), -i),
                )
            else:
                raise ValueError(f"unknown policy: {policy}")
        state = _advance(chains, state, selected)
        elapsed += 1
    return elapsed


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = round((len(ordered) - 1) * percentile)
    return ordered[index]


def _format_instance(chains: tuple[Chain, ...]) -> str:
    return " | ".join(
        " -> ".join(
            f"C{size}/L{delay}" for size, delay in zip(chain.comm, chain.delay)
        )
        for chain in chains
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=260804)
    parser.add_argument("--min-chains", type=int, default=2)
    parser.add_argument("--max-chains", type=int, default=4)
    parser.add_argument("--max-operations", type=int, default=3)
    parser.add_argument("--max-comm", type=int, default=3)
    parser.add_argument("--max-delay", type=int, default=4)
    args = parser.parse_args()
    if args.samples < 1:
        parser.error("--samples must be positive")

    rng = random.Random(args.seed)
    policies = ("fifo", "spt", "lpt", "longest_delay", "ltf", "lrpt")
    ratios = {policy: [] for policy in policies}
    worst: dict[str, tuple[float, int, int, tuple[Chain, ...]]] = {}

    for _ in range(args.samples):
        chains = tuple(
            Chain(
                comm=tuple(
                    rng.randint(1, args.max_comm)
                    for _ in range(operation_count)
                ),
                delay=tuple(
                    rng.randint(0, args.max_delay)
                    for _ in range(operation_count)
                ),
            )
            for operation_count in (
                rng.randint(1, args.max_operations)
                for _ in range(rng.randint(args.min_chains, args.max_chains))
            )
        )
        optimum = optimal_makespan(chains)
        for policy in policies:
            makespan = heuristic_makespan(chains, policy)
            ratio = makespan / optimum
            ratios[policy].append(ratio)
            if policy not in worst or ratio > worst[policy][0]:
                worst[policy] = (ratio, makespan, optimum, chains)

    print(f"samples={args.samples} seed={args.seed}")
    print("policy          mean      p95       max   worst (heuristic/optimal)")
    for policy in policies:
        ratio, makespan, optimum, chains = worst[policy]
        print(
            f"{policy:14} {mean(ratios[policy]):.4f}    "
            f"{_percentile(ratios[policy], 0.95):.4f}    {ratio:.4f}   "
            f"{makespan}/{optimum}  {_format_instance(chains)}"
        )


if __name__ == "__main__":
    main()
