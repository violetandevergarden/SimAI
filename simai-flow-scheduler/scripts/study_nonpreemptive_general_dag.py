"""Compatibility entry point for migrated general-DAG algorithms."""

from DAG_heuristic.single_channel.complex_chain import algorithms as _impl
from DAG_heuristic.single_channel.complex_chain.algorithms import *  # noqa: F401,F403


def __getattr__(name: str):
    return getattr(_impl, name)


if __name__ == "__main__":
    _impl.main()
