"""Fixed instances designed to expose ordering, waiting, and beam failures."""

from DAG_heuristic.common.interface import TestCase
from DAG_heuristic.single_channel.parallel_chain.algorithms import (
    fixed_beam_counterexample,
    scaled_five_four_family,
    tight_optional_wait_family,
)


def cases() -> list[TestCase]:
    return [
        TestCase("tight_optional_wait_m20", "adversarial", tight_optional_wait_family(20)),
        TestCase("scaled_five_four_s4", "adversarial", scaled_five_four_family(4)),
        TestCase("fixed_beam_counterexample", "adversarial", fixed_beam_counterexample()),
    ]

