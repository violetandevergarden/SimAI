"""Public algorithm registry for independent parallel chains."""

from __future__ import annotations

from DAG_heuristic.common.interface import AlgorithmSpec
from DAG_heuristic.single_channel.parallel_chain import algorithms as ours


ChainInstance = tuple[ours.ParallelChain, ...]


ALGORITHMS = {
    "longest_tail": AlgorithmSpec(
        "longest_tail",
        lambda case: ours.schedule_priority(case, "dynamic_tail"),
        "Recompute residual longest tail at every completion event.",
    ),
    "rollout_flow2": AlgorithmSpec(
        "rollout_flow2",
        lambda case: ours.schedule_rollout(case, top_k=2, allow_wait=False),
        "Evaluate two whole-flow candidates with longest-tail completion.",
    ),
    "rollout_wait2": AlgorithmSpec(
        "rollout_wait2",
        lambda case: ours.schedule_rollout(case, top_k=2, allow_wait=True),
        "Add optional WAIT to two whole-flow rollout candidates.",
        supports_wait=True,
    ),
    "beam_wait8": AlgorithmSpec(
        "beam_wait8",
        lambda case: ours.beam_search(case, width=8, allow_wait=True),
        "Keep eight optional-idle partial schedules per search layer.",
        supports_wait=True,
    ),
    "beam_wait32": AlgorithmSpec(
        "beam_wait32",
        lambda case: ours.beam_search(case, width=32, allow_wait=True),
        "Keep 32 optional-idle partial schedules per search layer.",
        supports_wait=True,
    ),
    "exact_optional": AlgorithmSpec(
        "exact_optional",
        lambda case: ours.exact_dp(case, optional_idle=True),
        "Memoized exact oracle for small chain instances.",
        exact=True,
        supports_wait=True,
    ),
}


def register(spec: AlgorithmSpec[ChainInstance, object]) -> None:
    """Register a collaborator algorithm and reject accidental name reuse."""

    if spec.name in ALGORITHMS:
        raise ValueError(f"algorithm already registered: {spec.name}")
    ALGORITHMS[spec.name] = spec

