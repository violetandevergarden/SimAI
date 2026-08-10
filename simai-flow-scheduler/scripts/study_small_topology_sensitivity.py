"""Compatibility import for migrated topology fixtures."""

from DAG_heuristic.muti_channel import topology_fixtures as _impl
from DAG_heuristic.muti_channel.topology_fixtures import *  # noqa: F401,F403


def __getattr__(name: str):
    return getattr(_impl, name)


if __name__ == "__main__":
    _impl.main()
