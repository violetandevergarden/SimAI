"""Run the revised R1 exact-oracle benchmark matrix.

Example:
    python scripts/study_nonpreemptive_oracle.py
    python scripts/study_nonpreemptive_oracle.py --random-chains 20 --random-joins 10
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import random
from statistics import mean
import sys
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from DAG_heuristic.common.benchmark import (
    BenchmarkDAG,
    BenchTask,
    _chain_dag,
    adversarial_benchmarks,
    llm_motif_benchmarks,
    reduce_effective_dag,
)
from DAG_heuristic.common.oracle import (
    branch_and_bound_oracle,
    exact_oracle,
)
from DAG_heuristic.single_channel.complex_chain.legacy_generator import random_join_dag
from DAG_heuristic.single_channel.parallel_chain.legacy_generator import (
    Chain,
    random_instance,
    scaled_five_four_counterexample,
    scaled_tail_counterexample,
)

DEFAULT_OUTPUT = ROOT / "outputs/nonpreemptive_oracle/r1_summary.json"


def _chains_to_dag(name: str, category: str, chains: tuple[Chain, ...]) -> BenchmarkDAG:
    structure = tuple((chain.comm, chain.delay) for chain in chains)
    return _chain_dag(name, category, structure, "R1 parallel-chain benchmark.")


def _waiting_counterexample(magnitude: int = 10) -> BenchmarkDAG:
    return BenchmarkDAG(
        "optional_wait_tight_two",
        "adversarial",
        (
            BenchTask("release_b", "compute", 1),
            BenchTask("A", "comm", magnitude),
            BenchTask("B", "comm", 1, ("release_b",)),
            BenchTask("tail_b", "compute", magnitude, ("B",)),
        ),
        "Waiting for B makes the work-conserving/optional-idle ratio tend to two.",
    )


def benchmark_cases(
    *,
    random_chains: int,
    random_joins: int,
    real_windows: int,
    real_max_flows: int,
    chain_seed: int,
    join_seed: int,
) -> list[BenchmarkDAG]:
    result = [
        _waiting_counterexample(),
        _chains_to_dag(
            "scaled_tail_counterexample_r1",
            "adversarial",
            scaled_tail_counterexample(4),
        ),
        _chains_to_dag(
            "scaled_five_four_counterexample_r1",
            "adversarial",
            scaled_five_four_counterexample(4),
        ),
        *adversarial_benchmarks(),
        *llm_motif_benchmarks(),
    ]

    chain_rng = random.Random(chain_seed)
    for index in range(random_chains):
        result.append(
            _chains_to_dag(
                f"random_chain_{index}",
                "random_chain",
                random_instance(chain_rng),
            )
        )

    join_rng = random.Random(join_seed)
    result.extend(random_join_dag(join_rng, index) for index in range(random_joins))

    effective_path = ROOT / "outputs/dag_audit/1f1b/effective_dag.json"
    if effective_path.exists():
        graph = json.loads(effective_path.read_text(encoding="utf-8"))
        for bucket_rank in range(real_windows):
            try:
                result.append(
                    reduce_effective_dag(
                        graph,
                        max_flows=real_max_flows,
                        quantum_us=1.0,
                        bucket_rank=bucket_rank,
                    )
                )
            except ValueError:
                break
    return result


def _action_names(actions: Iterable) -> list[str]:
    return ["WAIT" if action.kind == "wait" else str(action.task_id) for action in actions]


def evaluate(
    dags: Iterable[BenchmarkDAG],
    *,
    max_states: int,
    time_limit_s: float,
    progress: bool = False,
) -> dict:
    rows = []
    for index, dag in enumerate(dags):
        if progress:
            print(f"[{index + 1}] {dag.category}/{dag.name}", flush=True)
        values = {}
        for mode in ("optional_idle", "work_conserving"):
            dynamic_programming = exact_oracle(
                dag,
                mode=mode,
                max_states=max_states,
                time_limit_s=time_limit_s,
            )
            branch_and_bound = branch_and_bound_oracle(
                dag,
                mode=mode,
                max_states=max_states,
                time_limit_s=time_limit_s,
            )
            if dynamic_programming.makespan != branch_and_bound.makespan:
                raise AssertionError(
                    f"oracle disagreement for {dag.name}/{mode}: "
                    f"{dynamic_programming.makespan} != {branch_and_bound.makespan}"
                )
            values[mode] = {
                "makespan": dynamic_programming.makespan,
                "actions": _action_names(dynamic_programming.actions),
                "voluntary_waits": dynamic_programming.voluntary_waits,
                "forced_waits": dynamic_programming.forced_waits,
                "voluntary_wait_time": dynamic_programming.voluntary_wait_time,
                "forced_wait_time": dynamic_programming.forced_wait_time,
                "wait_time": dynamic_programming.wait_time,
                "lower_bounds": dynamic_programming.lower_bounds,
                "dp_states": dynamic_programming.explored_states,
                "dp_cache_hits": dynamic_programming.cache_hits,
                "dp_runtime_ms": dynamic_programming.runtime_ms,
                "bnb_states": branch_and_bound.explored_states,
                "bnb_runtime_ms": branch_and_bound.runtime_ms,
            }
        rows.append(
            {
                "name": dag.name,
                "category": dag.category,
                "tasks": len(dag.tasks),
                "flows": sum(task.kind == "comm" for task in dag.tasks),
                "idle_regret": (
                    values["work_conserving"]["makespan"]
                    - values["optional_idle"]["makespan"]
                ),
                **values,
            }
        )

    categories = sorted({row["category"] for row in rows})
    by_category = {}
    for category in categories:
        selected = [row for row in rows if row["category"] == category]
        by_category[category] = {
            "count": len(selected),
            "idle_helped": sum(row["idle_regret"] > 0 for row in selected),
            "max_idle_regret": max((row["idle_regret"] for row in selected), default=0),
            "mean_optional_dp_states": mean(
                row["optional_idle"]["dp_states"] for row in selected
            ),
            "mean_optional_bnb_states": mean(
                row["optional_idle"]["bnb_states"] for row in selected
            ),
        }
    return {
        "model": {
            "communication": "non-preemptive full-channel interval",
            "compute": "non-preemptive immediate-start",
            "idle": "optional WAIT to next compute completion",
        },
        "count": len(rows),
        "oracle_agreement": True,
        "idle_helped": sum(row["idle_regret"] > 0 for row in rows),
        "max_idle_regret": max((row["idle_regret"] for row in rows), default=0),
        "by_category": by_category,
        "rows": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--random-chains", type=int, default=100)
    parser.add_argument("--random-joins", type=int, default=50)
    parser.add_argument("--real-windows", type=int, default=8)
    parser.add_argument("--real-max-flows", type=int, default=1)
    parser.add_argument("--chain-seed", type=int, default=260813)
    parser.add_argument("--join-seed", type=int, default=260817)
    parser.add_argument("--max-states", type=int, default=2_000_000)
    parser.add_argument("--time-limit-s", type=float, default=30.0)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    dags = benchmark_cases(
        random_chains=args.random_chains,
        random_joins=args.random_joins,
        real_windows=args.real_windows,
        real_max_flows=args.real_max_flows,
        chain_seed=args.chain_seed,
        join_seed=args.join_seed,
    )
    report = evaluate(
        dags,
        max_states=args.max_states,
        time_limit_s=args.time_limit_s,
        progress=True,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    summary = {key: value for key, value in report.items() if key != "rows"}
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
