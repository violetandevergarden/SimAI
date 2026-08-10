"""Small chain projections extracted from repeating pipeline motifs.

These retain the alternating release/communication structure used for the
single-channel theory.  Full fork/join exports belong to ``complex_chain``.
"""

from DAG_heuristic.common.interface import TestCase
from DAG_heuristic.single_channel.parallel_chain.algorithms import ParallelChain


def cases() -> list[TestCase]:
    one_f_one_b_projection = (
        ParallelChain((2, 2, 1), (3, 2, 1), initial_delay=0),
        ParallelChain((1, 2, 2), (2, 3, 1), initial_delay=1),
        ParallelChain((2, 1, 2), (2, 2, 2), initial_delay=2),
    )
    zero_bubble_projection = (
        ParallelChain((2, 1, 1), (3, 1, 2), initial_delay=0),
        ParallelChain((1, 1, 2), (2, 2, 1), initial_delay=1),
        ParallelChain((1, 2), (1, 3), initial_delay=2),
    )
    return [
        TestCase("1f1b_chain_projection", "real_derived", one_f_one_b_projection),
        TestCase("zero_bubble_chain_projection", "real_derived", zero_bubble_projection),
    ]

