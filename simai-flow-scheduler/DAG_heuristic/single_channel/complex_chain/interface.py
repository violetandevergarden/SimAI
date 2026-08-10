"""Public algorithm registry for general single-channel DAGs."""

from __future__ import annotations

from DAG_heuristic.common.interface import AlgorithmSpec
from DAG_heuristic.common.oracle import exact_oracle
from DAG_heuristic.single_channel.complex_chain import algorithms as ours


ALGORITHMS = {
    "longest_tail": AlgorithmSpec(
        "longest_tail",
        ours.schedule_priority,
        "Dynamic residual longest-tail priority.",
    ),
    "join_bonus": AlgorithmSpec(
        "join_bonus",
        lambda case: ours.schedule_priority(case, "raw_join"),
        "Longest tail plus optimistic last-join-blocker bonus.",
    ),
    "rollout_flow2": AlgorithmSpec(
        "rollout_flow2",
        lambda case: ours.schedule_rollout(case, top_k=2, allow_wait=False),
        "Two whole-flow candidates with longest-tail completion.",
    ),
    "rollout_wait2": AlgorithmSpec(
        "rollout_wait2",
        lambda case: ours.schedule_rollout(case, top_k=2, allow_wait=True),
        "Two whole-flow candidates plus optional WAIT.",
        supports_wait=True,
    ),
    "depth2_wait2": AlgorithmSpec(
        "depth2_wait2",
        lambda case: ours.schedule_rollout(
            case,
            top_k=2,
            allow_wait=True,
            candidate_mode="hybrid",
            depth=2,
        ),
        "Two-event hybrid lookahead with WAIT.",
        supports_wait=True,
    ),
    "beam_wait8": AlgorithmSpec(
        "beam_wait8",
        lambda case: ours.beam_search(case, width=8),
        "Beam search with eight hybrid optional-idle states per layer.",
        supports_wait=True,
    ),
    "exact_optional": AlgorithmSpec(
        "exact_optional",
        lambda case: exact_oracle(case, mode="optional_idle"),
        "Exact optional-idle oracle for small DAGs.",
        exact=True,
        supports_wait=True,
    ),
}


def register(spec: AlgorithmSpec) -> None:
    if spec.name in ALGORITHMS:
        raise ValueError(f"algorithm already registered: {spec.name}")
    ALGORITHMS[spec.name] = spec

