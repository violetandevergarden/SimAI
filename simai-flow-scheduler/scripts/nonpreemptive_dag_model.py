"""Compatibility import for the migrated DAG heuristic model."""

from DAG_heuristic.common import model as _impl
from DAG_heuristic.common.model import *  # noqa: F401,F403


def __getattr__(name: str):
    return getattr(_impl, name)
