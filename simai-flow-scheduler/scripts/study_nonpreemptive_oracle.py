"""Compatibility entry point for the migrated oracle benchmark."""

from DAG_heuristic.common import oracle_benchmark as _impl
from DAG_heuristic.common.oracle_benchmark import *  # noqa: F401,F403


def __getattr__(name: str):
    return getattr(_impl, name)


if __name__ == "__main__":
    _impl.main()
