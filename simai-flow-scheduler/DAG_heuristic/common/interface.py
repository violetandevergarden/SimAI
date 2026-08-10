"""Small stable interfaces used by every scenario in this repository."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Generic, TypeVar


InstanceT = TypeVar("InstanceT")
ResultT = TypeVar("ResultT")


@dataclass(frozen=True)
class TestCase(Generic[InstanceT]):
    """A named instance with an auditable provenance category."""

    name: str
    category: str
    instance: InstanceT


@dataclass(frozen=True)
class AlgorithmSpec(Generic[InstanceT, ResultT]):
    """A registered algorithm callable with lightweight capability metadata."""

    name: str
    solve: Callable[[InstanceT], ResultT]
    description: str
    exact: bool = False
    supports_wait: bool = False

    def __call__(self, instance: InstanceT) -> ResultT:
        return self.solve(instance)


def makespan_of(result: object) -> int:
    """Read the common makespan field without coupling runners to result types."""

    value = getattr(result, "makespan", None)
    if not isinstance(value, int):
        raise TypeError("algorithm result must expose an integer 'makespan'")
    return value

