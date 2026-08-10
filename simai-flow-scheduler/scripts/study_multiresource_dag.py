"""Compatibility import for migrated legacy multi-resource fixtures."""

from DAG_heuristic.muti_channel import legacy_generator as _impl
from DAG_heuristic.muti_channel.legacy_generator import *  # noqa: F401,F403


def __getattr__(name: str):
    return getattr(_impl, name)


if __name__ == "__main__":
    _impl.main()
