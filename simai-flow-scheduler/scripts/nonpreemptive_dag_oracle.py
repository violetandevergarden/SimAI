"""Compatibility import for the migrated non-preemptive oracle."""

from DAG_heuristic.common import oracle as _impl
from DAG_heuristic.common.oracle import *  # noqa: F401,F403


def __getattr__(name: str):
    return getattr(_impl, name)
