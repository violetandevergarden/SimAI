"""R2 study for non-preemptive single-channel parallel chains.

Every action completes one whole communication or waits to the next compute
completion.  The compact chain state is cross-checked against the R0 DAG state
machine and the R1 exact oracle on small instances.

Use the public runner documented in ``DAG_heuristic/README.md``.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import asdict, dataclass, replace
from functools import lru_cache
from itertools import product
import json
from pathlib import Path
import random
from statistics import mean
import sys
from time import perf_counter
from typing import Literal


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from DAG_heuristic.common.benchmark import _Builder  # noqa: E402
from DAG_heuristic.single_channel.parallel_chain.legacy_types import Chain  # noqa: E402
from DAG_heuristic.common.model import (  # noqa: E402
    Action as DAGAction,
    NonPreemptiveDAGModel,
    assert_nonpreemptive_trace,
)
from DAG_heuristic.single_channel.parallel_chain.legacy_generator import (  # noqa: E402
    random_instance,
)


ChainState = tuple[tuple[int, int], ...]  # (next communication, compute cooldown)
ActionKind = Literal["flow", "wait"]


@dataclass(frozen=True)
class ParallelChain:
    comm: tuple[int, ...]
    compute: tuple[int, ...]
    initial_delay: int = 0

    def __post_init__(self) -> None:
        if not self.comm or len(self.comm) != len(self.compute):
            raise ValueError("comm and compute must have the same positive length")
        if any(value <= 0 for value in self.comm):
            raise ValueError("communication durations must be positive")
        if self.initial_delay < 0 or any(value < 0 for value in self.compute):
            raise ValueError("compute durations must be non-negative")

    @classmethod
    def from_legacy(cls, chain: Chain) -> ParallelChain:
        return cls(chain.comm, chain.delay)


@dataclass(frozen=True)
class ChainAction:
    kind: ActionKind
    chain: int | None = None

    @classmethod
    def flow(cls, chain: int) -> ChainAction:
        return cls("flow", chain)

    @classmethod
    def wait(cls) -> ChainAction:
        return cls("wait")


@dataclass(frozen=True)
class ChainSchedule:
    makespan: int
    actions: tuple[ChainAction, ...]
    network_busy: int
    voluntary_idle: int
    forced_idle: int
    compute_active_time: int
    compute_capacity_time: int
    overlap_time: int
    runtime_ms: float
    fallback: bool = False

    @property
    def network_idle(self) -> int:
        return self.voluntary_idle + self.forced_idle

    @property
    def preemptions(self) -> int:
        return 0


@dataclass(frozen=True)
class ChainSearchResult:
    makespan: int
    actions: tuple[ChainAction, ...]
    explored_states: int
    runtime_ms: float
    fallback: bool = False


def initial_state(chains: tuple[ParallelChain, ...]) -> ChainState:
    return tuple((0, chain.initial_delay) for chain in chains)


def is_finished(chains: tuple[ParallelChain, ...], state: ChainState) -> bool:
    return all(
        operation == len(chain.comm) and cooldown == 0
        for chain, (operation, cooldown) in zip(chains, state, strict=True)
    )


def ready_chains(chains: tuple[ParallelChain, ...], state: ChainState) -> list[int]:
    return [
        index
        for index, (chain, (operation, cooldown)) in enumerate(
            zip(chains, state, strict=True)
        )
        if operation < len(chain.comm) and cooldown == 0
    ]


def active_computes(state: ChainState) -> list[int]:
    return [index for index, (_operation, cooldown) in enumerate(state) if cooldown > 0]


def legal_actions(
    chains: tuple[ParallelChain, ...],
    state: ChainState,
    *,
    optional_idle: bool,
) -> tuple[ChainAction, ...]:
    ready = ready_chains(chains, state)
    flows = tuple(ChainAction.flow(index) for index in ready)
    can_wait = bool(active_computes(state))
    if optional_idle:
        return (*flows, ChainAction.wait()) if can_wait else flows
    if flows:
        return flows
    return (ChainAction.wait(),) if can_wait else ()


def advance(
    chains: tuple[ParallelChain, ...],
    state: ChainState,
    action: ChainAction,
) -> tuple[ChainState, int]:
    if action.kind == "wait":
        active = active_computes(state)
        if not active:
            raise ValueError("WAIT requires an active compute")
        duration = min(state[index][1] for index in active)
        return tuple(
            (operation, max(0, cooldown - duration))
            for operation, cooldown in state
        ), duration

    if action.chain is None or action.chain not in ready_chains(chains, state):
        raise ValueError(f"flow action is not ready: {action}")
    selected = action.chain
    operation = state[selected][0]
    duration = chains[selected].comm[operation]
    values = [
        (item_operation, max(0, cooldown - duration))
        for item_operation, cooldown in state
    ]
    values[selected] = (operation + 1, chains[selected].compute[operation])
    return tuple(values), duration


def residual_bounds(
    chains: tuple[ParallelChain, ...], state: ChainState
) -> dict[str, int]:
    communication = 0
    compute = 0
    chain_path = 0
    for chain, (operation, cooldown) in zip(chains, state, strict=True):
        remaining_comm = sum(chain.comm[operation:])
        remaining_compute = cooldown + sum(chain.compute[operation:])
        communication += remaining_comm
        compute = max(compute, remaining_compute)
        chain_path = max(chain_path, remaining_comm + remaining_compute)
    return {
        "P": communication,
        "Q": compute,
        "L": chain_path,
        "combined": max(communication, compute, chain_path),
    }


def _flow_tail(
    chains: tuple[ParallelChain, ...], state: ChainState, index: int
) -> int:
    operation = state[index][0]
    chain = chains[index]
    return sum(chain.compute[operation:]) + sum(chain.comm[operation + 1 :])


def _flow_duration(
    chains: tuple[ParallelChain, ...], state: ChainState, index: int
) -> int:
    return chains[index].comm[state[index][0]]


BASE_POLICIES = (
    "fifo",
    "spt",
    "lpt",
    "longest_delay",
    "dynamic_tail",
    "lrpt",
    "earliest_slack",
    "tictac",
)


def select_flow(
    policy: str,
    chains: tuple[ParallelChain, ...],
    state: ChainState,
    ready: list[int],
) -> int:
    if policy == "fifo":
        return min(ready)
    if policy == "spt":
        return min(ready, key=lambda item: (_flow_duration(chains, state, item), item))
    if policy == "lpt":
        return max(ready, key=lambda item: (_flow_duration(chains, state, item), -item))
    if policy == "longest_delay":
        return max(
            ready,
            key=lambda item: (
                chains[item].compute[state[item][0]],
                -item,
            ),
        )
    if policy == "dynamic_tail":
        return max(ready, key=lambda item: (_flow_tail(chains, state, item), -item))
    if policy in {"lrpt", "earliest_slack"}:
        return max(
            ready,
            key=lambda item: (
                _flow_duration(chains, state, item)
                + _flow_tail(chains, state, item),
                -item,
            ),
        )
    if policy == "tictac":
        winner = ready[0]
        for candidate in ready[1:]:
            pa = _flow_duration(chains, state, winner)
            pb = _flow_duration(chains, state, candidate)
            qa = _flow_tail(chains, state, winner)
            qb = _flow_tail(chains, state, candidate)
            ab = max(pa + qa, pa + pb + qb)
            ba = max(pb + qb, pb + pa + qa)
            if (ba, candidate) < (ab, winner):
                winner = candidate
        return winner
    raise ValueError(f"unknown non-preemptive chain policy: {policy}")


def priority_action(
    policy: str,
    chains: tuple[ParallelChain, ...],
    state: ChainState,
) -> ChainAction:
    ready = ready_chains(chains, state)
    if ready:
        return ChainAction.flow(select_flow(policy, chains, state, ready))
    if active_computes(state):
        return ChainAction.wait()
    raise RuntimeError("unfinished chain state has no legal action")


def _simulate_actions(
    chains: tuple[ParallelChain, ...],
    choose,
    *,
    start_state: ChainState | None = None,
    collect_metrics: bool = True,
) -> ChainSchedule:
    started = perf_counter()
    state = initial_state(chains) if start_state is None else start_state
    actions: list[ChainAction] = []
    elapsed = 0
    network_busy = 0
    voluntary_idle = 0
    forced_idle = 0
    compute_active_time = 0
    overlap_time = 0
    while not is_finished(chains, state):
        action = choose(state)
        if action not in legal_actions(chains, state, optional_idle=True):
            raise ValueError(f"scheduler returned illegal action: {action}")
        cooldowns = [cooldown for _operation, cooldown in state]
        had_ready = bool(ready_chains(chains, state))
        successor, duration = advance(chains, state, action)
        compute_active_time += sum(min(cooldown, duration) for cooldown in cooldowns)
        if action.kind == "flow":
            network_busy += duration
            overlap_time += min(max(cooldowns, default=0), duration)
        elif had_ready:
            voluntary_idle += duration
        else:
            forced_idle += duration
        actions.append(action)
        elapsed += duration
        state = successor
    return ChainSchedule(
        elapsed,
        tuple(actions),
        network_busy,
        voluntary_idle,
        forced_idle,
        compute_active_time,
        elapsed * len(chains),
        overlap_time,
        (perf_counter() - started) * 1000 if collect_metrics else 0.0,
    )


def schedule_priority(
    chains: tuple[ParallelChain, ...], policy: str = "dynamic_tail"
) -> ChainSchedule:
    return _simulate_actions(
        chains, lambda state: priority_action(policy, chains, state)
    )


def _completion_cost(
    chains: tuple[ParallelChain, ...], state: ChainState, policy: str
) -> int:
    return _simulate_actions(
        chains,
        lambda item: priority_action(policy, chains, item),
        start_state=state,
        collect_metrics=False,
    ).makespan


def schedule_rollout(
    chains: tuple[ParallelChain, ...],
    *,
    top_k: int = 2,
    allow_wait: bool,
    base_policy: str = "dynamic_tail",
    time_limit_s: float = 2.0,
) -> ChainSchedule:
    started = perf_counter()
    fallback = False

    def choose(state: ChainState) -> ChainAction:
        nonlocal fallback
        base_action = priority_action(base_policy, chains, state)
        if perf_counter() - started > time_limit_s:
            fallback = True
            return base_action
        ready = ready_chains(chains, state)
        ranked = sorted(
            ready,
            key=lambda item: (_flow_tail(chains, state, item), -item),
            reverse=True,
        )[:top_k]
        candidates = [ChainAction.flow(index) for index in ranked]
        if base_action not in candidates:
            candidates.append(base_action)
        if allow_wait and active_computes(state):
            candidates.append(ChainAction.wait())
        scored = []
        for action in dict.fromkeys(candidates):
            successor, duration = advance(chains, state, action)
            score = duration + _completion_cost(chains, successor, base_policy)
            scored.append((score, action != base_action, action.kind == "wait", action))
        return min(scored, key=lambda item: item[:3])[3]

    result = _simulate_actions(chains, choose)
    return replace(result, fallback=fallback)


def _canonicalizer(chains: tuple[ParallelChain, ...]):
    groups: dict[ParallelChain, list[int]] = defaultdict(list)
    for index, chain in enumerate(chains):
        groups[chain].append(index)
    repeated = tuple(tuple(items) for items in groups.values() if len(items) > 1)

    def canonical(state: ChainState) -> ChainState:
        values = list(state)
        for indices in repeated:
            ordered = sorted(values[index] for index in indices)
            for index, value in zip(indices, ordered, strict=True):
                values[index] = value
        return tuple(values)

    return canonical


def exact_dp(
    chains: tuple[ParallelChain, ...],
    *,
    optional_idle: bool,
    max_states: int = 2_000_000,
    time_limit_s: float = 30.0,
    symmetry: bool = True,
) -> ChainSearchResult:
    started = perf_counter()
    canonical = _canonicalizer(chains) if symmetry else (lambda state: state)
    explored = 0

    @lru_cache(maxsize=None)
    def solve(raw_state: ChainState) -> int:
        nonlocal explored
        state = canonical(raw_state)
        explored += 1
        if explored > max_states:
            raise RuntimeError(f"chain exact DP exceeded max_states={max_states}")
        if perf_counter() - started > time_limit_s:
            raise TimeoutError(f"chain exact DP exceeded time_limit_s={time_limit_s}")
        if is_finished(chains, state):
            return 0
        actions = legal_actions(chains, state, optional_idle=optional_idle)
        if not actions:
            raise RuntimeError("unfinished chain state has no legal action")
        return min(
            duration + solve(canonical(successor))
            for action in actions
            for successor, duration in (advance(chains, state, action),)
        )

    optimum = solve(canonical(initial_state(chains)))
    return ChainSearchResult(
        optimum, (), explored, (perf_counter() - started) * 1000
    )


def _feasible_within(
    chains: tuple[ParallelChain, ...],
    horizon: int,
    *,
    optional_idle: bool,
    max_states: int,
) -> tuple[bool, int]:
    canonical = _canonicalizer(chains)
    explored = 0

    @lru_cache(maxsize=None)
    def feasible(raw_state: ChainState, budget: int) -> bool:
        nonlocal explored
        state = canonical(raw_state)
        explored += 1
        if explored > max_states:
            raise RuntimeError("chain binary feasibility DP exceeded state budget")
        if is_finished(chains, state):
            return True
        if budget < residual_bounds(chains, state)["combined"]:
            return False
        for action in legal_actions(chains, state, optional_idle=optional_idle):
            successor, duration = advance(chains, state, action)
            if duration <= budget and feasible(canonical(successor), budget - duration):
                return True
        return False

    return feasible(canonical(initial_state(chains)), horizon), explored


def binary_search_exact(
    chains: tuple[ParallelChain, ...],
    *,
    optional_idle: bool,
    max_states: int = 2_000_000,
) -> ChainSearchResult:
    started = perf_counter()
    lower = residual_bounds(chains, initial_state(chains))["combined"]
    upper = schedule_rollout(chains, top_k=2, allow_wait=optional_idle).makespan
    explored = 0
    while lower < upper:
        middle = (lower + upper) // 2
        feasible, states = _feasible_within(
            chains,
            middle,
            optional_idle=optional_idle,
            max_states=max_states,
        )
        explored += states
        if feasible:
            upper = middle
        else:
            lower = middle + 1
    return ChainSearchResult(
        lower, (), explored, (perf_counter() - started) * 1000
    )


def beam_search(
    chains: tuple[ParallelChain, ...],
    *,
    width: int,
    allow_wait: bool,
    state_budget: int = 100_000,
    time_limit_s: float = 2.0,
) -> ChainSearchResult:
    started = perf_counter()
    incumbent = schedule_priority(chains)
    best_time = incumbent.makespan
    best_actions = incumbent.actions
    frontier: dict[ChainState, tuple[int, tuple[ChainAction, ...]]] = {
        initial_state(chains): (0, ())
    }
    explored = 0
    fallback = False
    while frontier:
        successors: dict[ChainState, tuple[int, tuple[ChainAction, ...]]] = {}
        for state, (elapsed, path) in frontier.items():
            for action in legal_actions(chains, state, optional_idle=allow_wait):
                successor, duration = advance(chains, state, action)
                new_elapsed = elapsed + duration
                explored += 1
                if explored > state_budget or perf_counter() - started > time_limit_s:
                    fallback = True
                    frontier = {}
                    break
                if is_finished(chains, successor):
                    if new_elapsed < best_time:
                        best_time = new_elapsed
                        best_actions = (*path, action)
                    continue
                if new_elapsed + residual_bounds(chains, successor)["combined"] >= best_time:
                    continue
                old = successors.get(successor)
                if old is None or new_elapsed < old[0]:
                    successors[successor] = (new_elapsed, (*path, action))
            if not frontier:
                break
        if not successors:
            break
        ranked = sorted(
            successors.items(),
            key=lambda item: (
                item[1][0] + residual_bounds(chains, item[0])["combined"],
                item[1][0] + _completion_cost(chains, item[0], "dynamic_tail"),
            ),
        )[:width]
        frontier = dict(ranked)
    return ChainSearchResult(
        best_time,
        best_actions,
        explored,
        (perf_counter() - started) * 1000,
        fallback,
    )


def monte_carlo_best(
    chains: tuple[ParallelChain, ...],
    *,
    samples: int,
    seed: int,
    allow_wait: bool,
    wait_probability: float = 0.15,
) -> ChainSearchResult:
    started = perf_counter()
    rng = random.Random(seed)
    incumbent = schedule_priority(chains)
    best_time = incumbent.makespan
    best_actions = incumbent.actions
    for _ in range(samples):
        def choose(state: ChainState) -> ChainAction:
            actions = list(legal_actions(chains, state, optional_idle=allow_wait))
            waits = [action for action in actions if action.kind == "wait"]
            flows = [action for action in actions if action.kind == "flow"]
            if waits and flows and rng.random() < wait_probability:
                return waits[0]
            if flows:
                if rng.random() < 0.7:
                    flows.sort(
                        key=lambda action: _flow_tail(
                            chains, state, int(action.chain)
                        ),
                        reverse=True,
                    )
                    return rng.choice(flows[: min(2, len(flows))])
                return rng.choice(flows)
            return waits[0]

        candidate = _simulate_actions(chains, choose)
        if candidate.makespan < best_time:
            best_time = candidate.makespan
            best_actions = candidate.actions
    return ChainSearchResult(
        best_time,
        best_actions,
        samples,
        (perf_counter() - started) * 1000,
    )


def to_benchmark_dag(chains: tuple[ParallelChain, ...]):
    builder = _Builder(
        "nonpreemptive_parallel_chains", "r2_chain", "Compact-chain replay DAG."
    )
    flow_ids: dict[tuple[int, int], str] = {}
    for chain_index, chain in enumerate(chains):
        previous = None
        if chain.initial_delay:
            previous = builder.add(
                f"c{chain_index}_release", "compute", chain.initial_delay
            )
        for operation, (comm, compute) in enumerate(
            zip(chain.comm, chain.compute, strict=True)
        ):
            flow_id = builder.add(
                f"c{chain_index}_flow{operation}",
                "comm",
                comm,
                () if previous is None else (previous,),
            )
            flow_ids[chain_index, operation] = flow_id
            previous = builder.add(
                f"c{chain_index}_compute{operation}",
                "compute",
                compute,
                (flow_id,),
            )
    return builder.finish(), flow_ids


def verify_schedule(
    chains: tuple[ParallelChain, ...], schedule: ChainSchedule | ChainSearchResult
) -> None:
    dag, flow_ids = to_benchmark_dag(chains)
    model = NonPreemptiveDAGModel(dag)
    compact_state = initial_state(chains)
    dag_actions: list[DAGAction] = []
    for action in schedule.actions:
        if action.kind == "wait":
            dag_actions.append(DAGAction.wait())
        else:
            assert action.chain is not None
            operation = compact_state[action.chain][0]
            dag_actions.append(DAGAction.flow(flow_ids[action.chain, operation]))
        compact_state, _duration = advance(chains, compact_state, action)
    trace = model.run(dag_actions)
    if trace.makespan != schedule.makespan or not model.is_finished(trace.final_state):
        raise AssertionError(
            f"compact/R0 replay mismatch: {schedule.makespan} vs {trace.makespan}"
        )
    assert_nonpreemptive_trace(trace)


def tight_optional_wait_family(magnitude: int) -> tuple[ParallelChain, ...]:
    return (
        ParallelChain((magnitude,), (0,)),
        ParallelChain((1,), (magnitude,), initial_delay=1),
    )


def scaled_five_four_family(scale: int) -> tuple[ParallelChain, ...]:
    return (
        ParallelChain((2 * scale, 2 * scale), (3 * scale + 1, 0)),
        ParallelChain((scale, 3 * scale), (2 * scale, 0)),
    )


def fixed_beam_counterexample() -> tuple[ParallelChain, ...]:
    """A fixed instance where both Beam-8 and Beam-32 return 47 vs OPT 46."""

    return (
        ParallelChain((6,), (6,)),
        ParallelChain((8,), (1,)),
        ParallelChain((1, 6, 4, 1), (8, 12, 9, 3)),
        ParallelChain((3,), (3,)),
        ParallelChain((2, 1), (2, 9)),
        ParallelChain((2, 1, 8), (1, 7, 8)),
    )


def _percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    return ordered[round((len(ordered) - 1) * quantile)]


def evaluate_random(samples: int, seed: int) -> dict:
    rng = random.Random(seed)
    rows = []
    methods = (
        *BASE_POLICIES,
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
    )
    records: dict[str, list[dict]] = defaultdict(list)
    exact_records = []
    binary_records = []
    for sample in range(samples):
        chains = tuple(
            ParallelChain.from_legacy(chain) for chain in random_instance(rng)
        )
        optimum_idle = exact_dp(chains, optional_idle=True)
        optimum_wc = exact_dp(chains, optional_idle=False)
        exact_records.append(
            {
                "idle_states": optimum_idle.explored_states,
                "idle_runtime_ms": optimum_idle.runtime_ms,
                "wc_states": optimum_wc.explored_states,
                "wc_runtime_ms": optimum_wc.runtime_ms,
            }
        )
        if sample < min(samples, 40):
            binary_idle = binary_search_exact(chains, optional_idle=True)
            binary_wc = binary_search_exact(chains, optional_idle=False)
            if (
                binary_idle.makespan != optimum_idle.makespan
                or binary_wc.makespan != optimum_wc.makespan
            ):
                raise AssertionError("direct and binary chain DPs disagree")
            binary_records.append(
                {
                    "idle_states": binary_idle.explored_states,
                    "idle_runtime_ms": binary_idle.runtime_ms,
                    "wc_states": binary_wc.explored_states,
                    "wc_runtime_ms": binary_wc.runtime_ms,
                }
            )
        results: dict[str, ChainSchedule | ChainSearchResult] = {
            policy: schedule_priority(chains, policy) for policy in BASE_POLICIES
        }
        results["rollout_flow2"] = schedule_rollout(
            chains, top_k=2, allow_wait=False
        )
        results["rollout_flow4"] = schedule_rollout(
            chains, top_k=4, allow_wait=False
        )
        results["rollout_wait2"] = schedule_rollout(
            chains, top_k=2, allow_wait=True
        )
        results["rollout_wait4"] = schedule_rollout(
            chains, top_k=4, allow_wait=True
        )
        results["beam_flow8"] = beam_search(chains, width=8, allow_wait=False)
        results["beam_flow32"] = beam_search(chains, width=32, allow_wait=False)
        results["beam_wait8"] = beam_search(chains, width=8, allow_wait=True)
        results["beam_wait32"] = beam_search(chains, width=32, allow_wait=True)
        results["mc_flow64"] = monte_carlo_best(
            chains, samples=64, seed=seed + sample, allow_wait=False
        )
        results["mc_wait64"] = monte_carlo_best(
            chains, samples=64, seed=seed + sample, allow_wait=True
        )
        for name, result in results.items():
            verify_schedule(chains, result)
            records[name].append(
                {
                    "ratio": result.makespan / optimum_idle.makespan,
                    "optimal": result.makespan == optimum_idle.makespan,
                    "gap_to_opt_idle": result.makespan - optimum_idle.makespan,
                    "gap_to_opt_wc": result.makespan - optimum_wc.makespan,
                    "runtime_ms": result.runtime_ms,
                    "fallback": getattr(result, "fallback", False),
                }
            )
        rows.append(
            {
                "sample": sample,
                "chains": [asdict(chain) for chain in chains],
                "opt_idle": optimum_idle.makespan,
                "opt_wc": optimum_wc.makespan,
                "idle_regret": optimum_wc.makespan - optimum_idle.makespan,
                "dynamic": results["dynamic_tail"].makespan,
                **{name: results[name].makespan for name in methods},
            }
        )

    summary = {}
    for method in methods:
        values = records[method]
        ratios = [item["ratio"] for item in values]
        summary[method] = {
            "work_conserving": method in (
                *BASE_POLICIES,
                "rollout_flow2",
                "rollout_flow4",
                "beam_flow8",
                "beam_flow32",
                "mc_flow64",
            ),
            "optimal": sum(item["optimal"] for item in values),
            "mean_ratio": mean(ratios),
            "p95_ratio": _percentile(ratios, 0.95),
            "max_ratio": max(ratios),
            "mean_gap_to_opt_idle": mean(
                item["gap_to_opt_idle"] for item in values
            ),
            "mean_gap_to_opt_wc": mean(item["gap_to_opt_wc"] for item in values),
            "mean_runtime_ms": mean(item["runtime_ms"] for item in values),
            "fallbacks": sum(item["fallback"] for item in values),
            "worst_sample": max(
                range(len(values)), key=lambda index: values[index]["ratio"]
            ),
        }

    def compare(left: str, right: str) -> dict[str, int]:
        return {
            "improved": sum(row[right] < row[left] for row in rows),
            "same": sum(row[right] == row[left] for row in rows),
            "hurt": sum(row[right] > row[left] for row in rows),
        }

    return {
        "samples": samples,
        "seed": seed,
        "idle_hard": sum(row["idle_regret"] > 0 for row in rows),
        "ordering_hard_dynamic": sum(
            row["dynamic"] > row["opt_wc"] for row in rows
        ),
        "combined_hard_dynamic": sum(
            row["idle_regret"] > 0 and row["dynamic"] > row["opt_wc"]
            for row in rows
        ),
        "wait_rollout_improved": sum(
            row["rollout_wait2"] < row["rollout_flow2"] for row in rows
        ),
        "wait_rollout_hurt": sum(
            row["rollout_wait2"] > row["rollout_flow2"] for row in rows
        ),
        "factor_ablation": {
            "rollout_flow2_to_flow4": compare(
                "rollout_flow2", "rollout_flow4"
            ),
            "rollout_wait2_to_wait4": compare(
                "rollout_wait2", "rollout_wait4"
            ),
            "beam_flow8_to_wait8": compare("beam_flow8", "beam_wait8"),
            "beam_flow32_to_wait32": compare("beam_flow32", "beam_wait32"),
            "mc_flow64_to_wait64": compare("mc_flow64", "mc_wait64"),
        },
        "exact_dp": {
            "mean_optional_states": mean(
                item["idle_states"] for item in exact_records
            ),
            "p95_optional_states": _percentile(
                [item["idle_states"] for item in exact_records], 0.95
            ),
            "mean_optional_runtime_ms": mean(
                item["idle_runtime_ms"] for item in exact_records
            ),
            "mean_wc_states": mean(item["wc_states"] for item in exact_records),
            "mean_wc_runtime_ms": mean(
                item["wc_runtime_ms"] for item in exact_records
            ),
            "binary_cross_checks": len(binary_records),
            "binary_mean_optional_states": mean(
                item["idle_states"] for item in binary_records
            ),
            "binary_mean_optional_runtime_ms": mean(
                item["idle_runtime_ms"] for item in binary_records
            ),
        },
        "algorithms": summary,
        "rows": rows,
    }


def restricted_cases(seed: int) -> dict:
    rng = random.Random(seed)
    cases = {}

    def run(
        name: str,
        instances: list[tuple[ParallelChain, ...]],
        policies: tuple[str, ...],
        known_optimum=None,
    ) -> None:
        worst = {policy: 1.0 for policy in policies}
        optimal = {policy: 0 for policy in policies}
        idle_hard = 0
        for chains in instances:
            optimum = (
                known_optimum(chains)
                if known_optimum is not None
                else exact_dp(chains, optional_idle=True).makespan
            )
            wc = exact_dp(chains, optional_idle=False).makespan
            idle_hard += wc > optimum
            for policy in policies:
                value = schedule_priority(chains, policy).makespan
                worst[policy] = max(worst[policy], value / optimum)
                optimal[policy] += value == optimum
        cases[name] = {
            "instances": len(instances),
            "idle_hard": idle_hard,
            "worst_ratio": worst,
            "optimal_fraction": {
                policy: optimal[policy] / len(instances) for policy in policies
            },
        }

    one_flow = [
        tuple(
            ParallelChain((rng.randint(1, 6),), (rng.randint(0, 10),))
            for _ in range(rng.randint(2, 8))
        )
        for _ in range(200)
    ]
    run(
        "one_flow_per_chain_initially_ready",
        one_flow,
        ("longest_delay", "dynamic_tail", "lrpt"),
        known_optimum=lambda chains: schedule_priority(
            chains, "dynamic_tail"
        ).makespan,
    )

    zero_compute = [
        tuple(
            ParallelChain(
                tuple(rng.randint(1, 5) for _ in range(length)),
                tuple(0 for _ in range(length)),
            )
            for length in (
                rng.randint(1, 3) for _ in range(rng.randint(2, 6))
            )
        )
        for _ in range(100)
    ]
    run(
        "zero_compute_delay_initially_ready",
        zero_compute,
        BASE_POLICIES,
        known_optimum=lambda chains: sum(sum(chain.comm) for chain in chains),
    )

    equal_comm = [
        tuple(
            ParallelChain((1, 1), (delays[index * 2], delays[index * 2 + 1]))
            for index in range(3)
        )
        for delays in product(range(3), repeat=6)
    ]
    run(
        "equal_communication",
        equal_comm,
        ("longest_delay", "dynamic_tail", "lrpt"),
    )

    monotone_shapes = [
        (communications, delays)
        for communications in product((1, 2), repeat=2)
        for delays in product(range(3), repeat=2)
        if delays[0] >= delays[1]
    ]
    monotone = [
        tuple(
            ParallelChain(communications, delays)
            for communications, delays in pair
        )
        for pair in product(monotone_shapes, repeat=2)
    ]
    run(
        "nonincreasing_compute_lags",
        monotone,
        ("longest_delay", "dynamic_tail", "lrpt"),
    )
    return cases


def scale_study(seed: int) -> list[dict]:
    rng = random.Random(seed)
    rows = []
    for chain_count in (4, 6, 8, 10):
        chains = tuple(
            ParallelChain.from_legacy(chain)
            for chain in random_instance(
                rng,
                min_chains=chain_count,
                max_chains=chain_count,
                max_operations=3,
                max_comm=3,
                max_delay=5,
            )
        )
        row = {"chains": chain_count, "flows": sum(len(item.comm) for item in chains)}
        try:
            exact = exact_dp(
                chains,
                optional_idle=True,
                max_states=30_000,
                time_limit_s=2.0,
            )
            row.update(
                {
                    "exact": exact.makespan,
                    "dp_states": exact.explored_states,
                    "dp_runtime_ms": exact.runtime_ms,
                }
            )
        except (RuntimeError, TimeoutError):
            row.update({"exact": None, "dp_states": None, "dp_runtime_ms": None})
        for width in (8, 32):
            for allow_wait, suffix in ((False, "flow"), (True, "wait")):
                beam = beam_search(
                    chains,
                    width=width,
                    allow_wait=allow_wait,
                    state_budget=30_000,
                    time_limit_s=2.0,
                )
                row[f"beam_{suffix}{width}"] = beam.makespan
                row[f"beam_{suffix}{width}_runtime_ms"] = beam.runtime_ms
                row[f"beam_{suffix}{width}_fallback"] = beam.fallback
        for allow_wait, suffix in ((False, "flow"), (True, "wait")):
            sampled = monte_carlo_best(
                chains,
                samples=64,
                seed=seed + chain_count,
                allow_wait=allow_wait,
            )
            row[f"mc_{suffix}64"] = sampled.makespan
            row[f"mc_{suffix}64_runtime_ms"] = sampled.runtime_ms
        rows.append(row)
    return rows


def counterexample_report() -> dict:
    five_four = []
    for scale in (1, 2, 4, 8, 16):
        chains = scaled_five_four_family(scale)
        optimum = exact_dp(chains, optional_idle=True).makespan
        five_four.append(
            {
                "scale": scale,
                "optimum": optimum,
                "dynamic_tail": schedule_priority(chains).makespan,
                "rollout_flow2": schedule_rollout(
                    chains, top_k=2, allow_wait=False
                ).makespan,
                "rollout_wait2": schedule_rollout(
                    chains, top_k=2, allow_wait=True
                ).makespan,
            }
        )
    tight_two = []
    for magnitude in (10, 20, 50, 100):
        chains = tight_optional_wait_family(magnitude)
        optimum = exact_dp(chains, optional_idle=True).makespan
        dynamic = schedule_priority(chains).makespan
        tight_two.append(
            {
                "magnitude": magnitude,
                "optimum": optimum,
                "dynamic_tail": dynamic,
                "ratio": dynamic / optimum,
                "rollout_wait2": schedule_rollout(
                    chains, top_k=2, allow_wait=True
                ).makespan,
            }
        )
    beam_chains = fixed_beam_counterexample()
    return {
        "dynamic_five_four_family": five_four,
        "work_conserving_tight_two_family": tight_two,
        "fixed_beam": {
            "optimum": exact_dp(beam_chains, optional_idle=True).makespan,
            "beam_flow8": beam_search(
                beam_chains, width=8, allow_wait=False
            ).makespan,
            "beam_flow32": beam_search(
                beam_chains, width=32, allow_wait=False
            ).makespan,
            "beam_wait8": beam_search(
                beam_chains, width=8, allow_wait=True
            ).makespan,
            "beam_wait32": beam_search(
                beam_chains, width=32, allow_wait=True
            ).makespan,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--seed", type=int, default=260813)
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "outputs/nonpreemptive_parallel_chains/r2_summary.json",
    )
    args = parser.parse_args()
    report = evaluate_random(args.samples, args.seed)
    report["restricted_cases"] = restricted_cases(args.seed + 1)
    report["scale_study"] = scale_study(args.seed + 2)
    report["counterexamples"] = counterexample_report()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(json.dumps({key: value for key, value in report.items() if key != "rows"}, indent=2))
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
