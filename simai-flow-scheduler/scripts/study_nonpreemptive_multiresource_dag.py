"""Compatibility entry point for migrated multi-channel algorithms."""

from DAG_heuristic.muti_channel import algorithms as _impl
from DAG_heuristic.muti_channel.algorithms import *  # noqa: F401,F403


def __getattr__(name: str):
    return getattr(_impl, name)


if __name__ == "__main__":
    _impl.main()
