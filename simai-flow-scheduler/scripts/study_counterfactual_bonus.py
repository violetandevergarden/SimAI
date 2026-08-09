"""Stage-3.5 hard benchmarks and end-to-end counterfactual bonus ablations.

This study deliberately filters for DAGs on which residual Dynamic-tail is
non-optimal, then separates candidate generation (tail/join/hybrid) from the
counterfactual action horizon (tick/event/flow completion).  It also extracts
small windows at *policy disagreement states* from a real effective 1F1B DAG.
"""

from __future__ import annotations

import argparse
from collections import defaultdict, deque
from dataclasses import asdict
import json
from pathlib import Path
import random
from statistics import mean
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.benchmark_dag_oracle import (  # noqa: E402
    BenchTask,
    BenchmarkDAG,
    _Builder,
    exact_oracle,
)
from scripts.study_general_dag_heuristics import (  # noqa: E402
    ResidualDAG,
    random_join_dag,
    rollout_schedule,
    schedule_policy,
)


ABLATIONS = {
    "dt_tick": {"candidate_policy": "dynamic_tail", "horizon": "tick"},
    "dt_event": {"candidate_policy": "dynamic_tail", "horizon": "event"},
    "dt_flow": {"candidate_policy": "dynamic_tail", "horizon": "flow_complete"},
    "join_event": {"candidate_policy": "join", "horizon": "event"},
    "hybrid_tick": {"candidate_policy": "hybrid", "horizon": "tick"},
    "hybrid_event": {"candidate_policy": "hybrid", "horizon": "event"},
    "hybrid_flow": {"candidate_policy": "hybrid", "horizon": "flow_complete"},
}


def frontier_statistics(dag: BenchmarkDAG, decisions: list[str | None]) -> dict[str, int]:
    model = ResidualDAG(dag)
    by_name = {task_id: index for index, task_id in enumerate(model.order)}
    state = model.initial
    maximum = 0
    over_two = 0
    choice_ticks = 0
    for decision in decisions:
        ready = model.ready(state)
        maximum = max(maximum, len(ready))
        over_two += len(ready) > 2
        choice_ticks += len(ready) > 1
        selected = None if decision is None else by_name[decision]
        state = model.tick(state, selected)
    return {
        "max_ready": maximum,
        "ticks_ready_gt_1": choice_ticks,
        "ticks_ready_gt_2": over_two,
    }


def generate_hard_dags(
    target: int,
    seed: int,
    *,
    max_attempts: int | None = None,
) -> tuple[list[BenchmarkDAG], dict]:
    """Keep only exact-solvable DAGs where Dynamic-tail has positive regret."""

    rng = random.Random(seed)
    attempts_limit = max_attempts or max(500, target * 250)
    hard: list[BenchmarkDAG] = []
    exact_skips = 0
    for attempt in range(attempts_limit):
        dag = random_join_dag(rng, attempt)
        try:
            optimum = exact_oracle(dag, max_states=300_000).makespan
        except RuntimeError:
            exact_skips += 1
            continue
        baseline = schedule_policy(dag, "dynamic_tail").makespan
        if baseline <= optimum:
            continue
        hard.append(BenchmarkDAG(
            f"hard_join_{attempt}", "hard_general", dag.tasks,
            f"Dynamic-tail regret {baseline - optimum}; selected from seeded search.",
            (*dag.parameters, ("source_attempt", str(attempt))),
        ))
        if len(hard) == target:
            break
    return hard, {
        "requested": target,
        "found": len(hard),
        "attempts": attempt + 1 if attempts_limit else 0,
        "exact_state_limit_skips": exact_skips,
        "acceptance_rate": len(hard) / max(attempt + 1, 1),
    }


def evaluate_hard_suite(dags: list[BenchmarkDAG], *, top_k: int) -> dict:
    rows = []
    ratios: dict[str, list[float]] = defaultdict(list)
    repairs: dict[str, int] = defaultdict(int)
    gap_closed: dict[str, list[float]] = defaultdict(list)
    runtime: dict[str, list[float]] = defaultdict(list)
    for dag in dags:
        oracle = exact_oracle(dag, max_states=500_000)
        baseline = schedule_policy(dag, "dynamic_tail")
        gap = baseline.makespan - oracle.makespan
        if gap <= 0:
            raise AssertionError(f"hard suite contains an easy DAG: {dag.name}")
        methods = {}
        for name, arguments in ABLATIONS.items():
            result = rollout_schedule(dag, top_k=top_k, **arguments)
            ratio = result.makespan / oracle.makespan
            methods[name] = {
                "makespan": result.makespan,
                "ratio": ratio,
                "improvement": baseline.makespan - result.makespan,
                "gap_closed": (baseline.makespan - result.makespan) / gap,
                "runtime_ms": result.runtime_ms,
            }
            ratios[name].append(ratio)
            repairs[name] += result.makespan == oracle.makespan
            gap_closed[name].append(methods[name]["gap_closed"])
            runtime[name].append(result.runtime_ms)
        rows.append({
            "name": dag.name,
            "tasks": len(dag.tasks),
            "optimum": oracle.makespan,
            "baseline": baseline.makespan,
            "baseline_ratio": baseline.makespan / oracle.makespan,
            "frontier": frontier_statistics(dag, baseline.decisions),
            "methods": methods,
            "dag": [asdict(task) for task in dag.tasks],
        })

    summary = {
        name: {
            "mean_ratio": mean(ratios[name]),
            "max_ratio": max(ratios[name]),
            "exact_repairs": repairs[name],
            "repair_fraction": repairs[name] / len(dags),
            "mean_gap_closed": mean(gap_closed[name]),
            "mean_runtime_ms": mean(runtime[name]),
        }
        for name in ABLATIONS
    } if dags else {}
    return {"instances": len(dags), "top_k": top_k, "summary": summary, "rows": rows}


def effective_graph_to_benchmark(graph: dict, *, quantum_us: float) -> BenchmarkDAG:
    predecessors: dict[int, list[int]] = defaultdict(list)
    for edge in graph["edges"]:
        predecessors[edge["target"]].append(edge["source"])
    tasks = []
    for node in sorted(graph["nodes"], key=lambda item: item["id"]):
        is_compute = node["type"] == "compute"
        quantized = round(float(node["duration_us"]) / quantum_us)
        duration = max(0 if is_compute else 1, quantized)
        tasks.append(BenchTask(
            f"t{node['id']}", "compute" if is_compute else "comm", duration,
            tuple(f"t{item}" for item in sorted(predecessors[node["id"]])),
            f"{node.get('phase', '')}:{node.get('stage', '')}",
        ))
    dag = BenchmarkDAG(
        "full_quantized_real_1f1b", "real_full", tuple(tasks),
        "Full effective 1F1B DAG quantized for disagreement-state discovery.",
        (("quantum_us", str(quantum_us)),),
    )
    errors = dag.validate()
    if errors:
        raise ValueError(errors)
    return dag


def _decision_window(
    full: ResidualDAG,
    state: tuple[int, ...],
    seeds: list[int],
    window_index: int,
    *,
    depth: int = 2,
    boundary_cap: int = 30,
) -> BenchmarkDAG:
    """Extract a residual window around competing ready-flow decisions."""

    analysis = full.analyze(state)
    selected = set(seeds)
    queue = deque((seed, 0) for seed in seeds)
    while queue:
        node, level = queue.popleft()
        if level >= depth:
            continue
        for child in full.children[node]:
            if state[child] == 0:
                continue
            if child not in selected:
                selected.add(child)
                queue.append((child, level + 1))

    builder = _Builder(
        f"real_decision_window_{window_index}", "real_decision",
        "Residual window extracted where Dynamic-tail and another priority disagree.",
    )
    boundary: dict[int, str] = {}
    for index in sorted(selected):
        external = [
            parent for parent in full.deps[index]
            if parent not in selected and state[parent] != 0
        ]
        if external:
            release = min(
                boundary_cap,
                max(analysis.earliest_finish[parent] for parent in external),
            )
            boundary[index] = builder.add(
                f"release_{index}", "compute", release, role="boundary_release",
            )
    for index in range(len(full.tasks)):
        if index not in selected:
            continue
        task = full.tasks[index]
        deps = [
            f"n{parent}" for parent in full.deps[index]
            if parent in selected and state[parent] != 0
        ]
        if index in boundary:
            deps.append(boundary[index])
        builder.add(
            f"n{index}", task.kind, full.remaining(state, index), deps,
            role=task.role, cut=task.cut,
        )
    for index in sorted(selected):
        unfinished_internal = [
            child for child in full.children[index]
            if child in selected and state[child] != 0
        ]
        if not unfinished_internal and analysis.tail[index] > 0:
            builder.add(
                f"tail_{index}", "compute",
                min(boundary_cap, analysis.tail[index]), (f"n{index}",),
                role="boundary_tail",
            )
    return builder.finish(
        source_nodes=len(full.tasks), selected_nodes=len(selected), seed_flows=len(seeds),
    )


def extract_real_disagreement_windows(
    graph: dict,
    *,
    target: int,
    quantum_us: float = 10_000.0,
    max_ticks: int = 50_000,
) -> tuple[list[BenchmarkDAG], dict]:
    """Follow Dynamic-tail and snapshot states where another rule disagrees."""

    dag = effective_graph_to_benchmark(graph, quantum_us=quantum_us)
    model = ResidualDAG(dag)
    initial_tail = model.analyze(model.initial).tail
    state = model.initial
    windows = []
    disagreements = 0
    exact_skips = 0
    elapsed = 0
    last_snapshot = -100
    while not all(value == 0 for value in state) and elapsed < max_ticks:
        ready = model.ready(state)
        if not ready:
            state = model.tick(state, None)
            elapsed += 1
            continue
        dynamic = model.select_dynamic_tail(state, ready)
        static = max(
            ready,
            key=lambda index: (
                initial_tail[index], -model.remaining(state, index), -index,
            ),
        )
        gate = model.select_gate_tail(state, ready)
        spt = min(ready, key=lambda index: (model.remaining(state, index), index))
        lpt = max(ready, key=lambda index: (model.remaining(state, index), -index))
        alternatives = list(dict.fromkeys((static, gate, spt, lpt)))
        differing = [choice for choice in alternatives if choice != dynamic]
        if differing and len(ready) > 1:
            disagreements += 1
            if elapsed - last_snapshot >= 5 and len(windows) < target:
                analysis = model.analyze(state)
                ranked = sorted(
                    ready,
                    key=lambda index: (analysis.tail[index], -index),
                    reverse=True,
                )
                seeds = list(dict.fromkeys((dynamic, *differing, *ranked)))[:4]
                candidate = _decision_window(model, state, seeds, len(windows))
                if len(candidate.tasks) > 24:
                    exact_skips += 1
                    state = model.tick(state, dynamic)
                    elapsed += 1
                    continue
                try:
                    exact_oracle(candidate, max_states=50_000)
                except RuntimeError:
                    exact_skips += 1
                else:
                    windows.append(candidate)
                    last_snapshot = elapsed
        state = model.tick(state, dynamic)
        elapsed += 1
    return windows, {
        "quantum_us": quantum_us,
        "full_tasks": len(dag.tasks),
        "simulated_ticks": elapsed,
        "policy_disagreement_ticks": disagreements,
        "exact_state_limit_skips": exact_skips,
        "windows": len(windows),
    }


def evaluate_real_windows(windows: list[BenchmarkDAG], *, top_k: int) -> dict:
    rows = []
    for dag in windows:
        optimum = exact_oracle(dag, max_states=500_000).makespan
        dynamic = schedule_policy(dag, "dynamic_tail")
        static = schedule_policy(dag, "longest_tail")
        hybrid = rollout_schedule(
            dag, top_k=top_k, candidate_policy="hybrid", horizon="event",
        )
        rows.append({
            "name": dag.name,
            "tasks": len(dag.tasks),
            "optimum": optimum,
            "static_tail": static.makespan,
            "dynamic_tail": dynamic.makespan,
            "hybrid_event": hybrid.makespan,
            "distinguishing": len({static.makespan, dynamic.makespan, hybrid.makespan}) > 1,
        })
    return {
        "instances": len(rows),
        "distinguishing_windows": sum(row["distinguishing"] for row in rows),
        "rows": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hard-samples", type=int, default=20)
    parser.add_argument("--real-windows", type=int, default=8)
    parser.add_argument("--top-k", type=int, default=2)
    parser.add_argument("--seed", type=int, default=260818)
    parser.add_argument(
        "--effective-dag", type=Path,
        default=ROOT / "outputs" / "dag_audit" / "1f1b" / "effective_dag.json",
    )
    parser.add_argument(
        "--output", type=Path,
        default=ROOT / "outputs" / "counterfactual_bonus" / "report.json",
    )
    args = parser.parse_args()
    if args.hard_samples < 1 or args.real_windows < 0 or args.top_k < 1:
        parser.error("sample counts/top-k outside supported range")

    hard, hard_search = generate_hard_dags(args.hard_samples, args.seed)
    hard_evaluation = evaluate_hard_suite(hard, top_k=args.top_k)
    real_search = {"error": f"missing {args.effective_dag}"}
    real_evaluation = {"instances": 0, "distinguishing_windows": 0, "rows": []}
    if args.real_windows and args.effective_dag.exists():
        graph = json.loads(args.effective_dag.read_text(encoding="utf-8"))
        windows, real_search = extract_real_disagreement_windows(
            graph, target=args.real_windows,
        )
        real_evaluation = evaluate_real_windows(windows, top_k=args.top_k)

    report = {
        "model": "single preemptive communication bottleneck + parallel compute",
        "seed": args.seed,
        "hard_search": hard_search,
        "hard_evaluation": hard_evaluation,
        "real_window_search": real_search,
        "real_window_evaluation": real_evaluation,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({
        "hard_search": hard_search,
        "hard_summary": hard_evaluation["summary"],
        "real_window_search": real_search,
        "real_window_evaluation": real_evaluation,
        "output": str(args.output),
    }, indent=2))


if __name__ == "__main__":
    main()
