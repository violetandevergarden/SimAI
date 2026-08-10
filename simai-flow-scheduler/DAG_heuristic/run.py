"""Unified command-line runner for every DAG heuristic scenario."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from time import perf_counter

from DAG_heuristic.common.interface import makespan_of


def _scenario(name: str):
    if name == "parallel_chain":
        from DAG_heuristic.single_channel.parallel_chain.interface import ALGORITHMS
        from DAG_heuristic.single_channel.parallel_chain.testsets import cases

        return ALGORITHMS, cases
    if name == "complex_chain":
        from DAG_heuristic.single_channel.complex_chain.interface import ALGORITHMS
        from DAG_heuristic.single_channel.complex_chain.testsets import cases

        return ALGORITHMS, cases
    if name == "muti_channel":
        from DAG_heuristic.muti_channel.interface import ALGORITHMS
        from DAG_heuristic.muti_channel.testsets import cases

        return ALGORITHMS, cases
    raise ValueError(f"unknown scenario: {name}")


def run(
    scenario: str,
    algorithm: str,
    category: str,
    *,
    samples: int,
    seed: int,
) -> dict:
    algorithms, load_cases = _scenario(scenario)
    if algorithm not in algorithms:
        available = ", ".join(sorted(algorithms))
        raise ValueError(f"unknown algorithm '{algorithm}'; choose one of: {available}")
    spec = algorithms[algorithm]
    rows = []
    for case in load_cases(category, samples=samples, seed=seed):
        started = perf_counter()
        result = spec(case.instance)
        rows.append(
            {
                "name": case.name,
                "category": case.category,
                "makespan": makespan_of(result),
                "runtime_ms": (perf_counter() - started) * 1000,
            }
        )
    return {
        "scenario": scenario,
        "algorithm": algorithm,
        "description": spec.description,
        "exact": spec.exact,
        "supports_wait": spec.supports_wait,
        "cases": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "scenario",
        choices=("parallel_chain", "complex_chain", "muti_channel"),
    )
    parser.add_argument("--algorithm", required=True)
    parser.add_argument(
        "--category",
        choices=("random", "adversarial", "real", "all"),
        default="adversarial",
    )
    parser.add_argument("--samples", type=int, default=10)
    parser.add_argument("--seed", type=int, default=260819)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--list-algorithms", action="store_true")
    args = parser.parse_args()

    algorithms, _load_cases = _scenario(args.scenario)
    if args.list_algorithms:
        for name, spec in sorted(algorithms.items()):
            print(f"{name}: {spec.description}")
        return

    report = run(
        args.scenario,
        args.algorithm,
        args.category,
        samples=args.samples,
        seed=args.seed,
    )
    payload = json.dumps(report, indent=2, ensure_ascii=False)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n", encoding="utf-8")
        print(f"wrote {args.output}")
    else:
        print(payload)


if __name__ == "__main__":
    main()

