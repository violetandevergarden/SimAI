"""Public algorithm registry for fixed-route multi-resource DAGs."""

from __future__ import annotations

from DAG_heuristic.common.interface import AlgorithmSpec
from DAG_heuristic.muti_channel import algorithms as ours


ALGORITHMS = {
    "longest_tail_pack": AlgorithmSpec(
        "longest_tail_pack",
        lambda case: ours.schedule_greedy(case, "dynamic_tail"),
        "Greedily pack compatible flows in residual longest-tail order.",
    ),
    "resource_pack": AlgorithmSpec(
        "resource_pack",
        lambda case: ours.schedule_greedy(case, "resource_tail"),
        "Longest-tail packing with residual resource-load tie breaking.",
    ),
    "bottleneck_pack": AlgorithmSpec(
        "bottleneck_pack",
        lambda case: ours.schedule_greedy(case, "bottleneck_first"),
        "Pack flows on the most heavily loaded remaining route resource first.",
    ),
    "rollout_maximal2": AlgorithmSpec(
        "rollout_maximal2",
        lambda case: ours.schedule_rollout(case, top_k=2, optional_actions=False),
        "Roll out maximal compatible start sets only.",
    ),
    "rollout_optional2": AlgorithmSpec(
        "rollout_optional2",
        lambda case: ours.schedule_rollout(case, top_k=2, optional_actions=True),
        "Roll out maximal/non-maximal start sets and WAIT.",
        supports_wait=True,
    ),
    "exact_optional": AlgorithmSpec(
        "exact_optional",
        lambda case: ours.exact_oracle(case, mode="optional_idle"),
        "Exact compatible-subset and WAIT oracle for small topologies.",
        exact=True,
        supports_wait=True,
    ),
}


def register(spec: AlgorithmSpec) -> None:
    if spec.name in ALGORITHMS:
        raise ValueError(f"algorithm already registered: {spec.name}")
    ALGORITHMS[spec.name] = spec

