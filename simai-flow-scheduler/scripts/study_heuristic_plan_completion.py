"""Finish stages 4 and 5 of the heuristic research plan.

This module is deliberately isolated from the production executor.  It adds
auditable experiments for strict-series DAG regions, a research-only Zero
Bubble B/W decoupling, P/Q-stratified topology evaluation, feature ablation,
periodic-cache reuse and profile-scale sensitivity.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import asdict, replace
import json
from pathlib import Path
from statistics import mean
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.benchmark_dag_oracle import (  # noqa: E402
    BenchmarkDAG,
    reduce_effective_dag,
    topological_order,
)
from scripts.study_general_dag_heuristics import evaluate as evaluate_general  # noqa: E402
from scripts.study_llm_route_windows import (  # noqa: E402
    build_route_aware_aicb,
    build_route_aware_probe,
    evaluate_windows,
)
from scripts.study_llm_structured_candidates import (  # noqa: E402
    leave_one_feature_out,
    periodic_cached_rollout,
    structured_rollout,
)
from scripts.study_multiresource_dag import (  # noqa: E402
    MultiResourceDAG,
    MultiResourceInstance,
    exact_multiresource_oracle,
    multi_resource_lower_bounds,
    schedule_multiresource,
)
from scripts.study_parallel_chains import evaluate_random as evaluate_chains  # noqa: E402
from scripts.study_small_topology_sensitivity import (  # noqa: E402
    PLACEMENTS,
    four_rack_core_topology,
    single_switch_topology,
    two_rack_topology,
)


def strict_series_partition(dag: BenchmarkDAG) -> dict:
    """Partition only at nodes comparable with every node in the DAG.

    Such a global barrier is both on every source-to-sink progression across
    the cut and cannot overlap work on its two sides.  This is intentionally
    more conservative than arbitrary SESE detection.
    """

    order = topological_order(dag)
    tasks = dag.task_map()
    children: dict[str, list[str]] = defaultdict(list)
    ancestors: dict[str, set[str]] = {}
    for task_id in order:
        values: set[str] = set()
        for parent in tasks[task_id].deps:
            values.add(parent)
            values.update(ancestors[parent])
            children[parent].append(task_id)
        ancestors[task_id] = values
    descendants: dict[str, set[str]] = {}
    for task_id in reversed(order):
        values: set[str] = set()
        for child in children[task_id]:
            values.add(child)
            values.update(descendants[child])
        descendants[task_id] = values
    barriers = [
        task_id for task_id in order
        if len(ancestors[task_id] | descendants[task_id]) == len(order) - 1
    ]
    regions: list[list[str]] = []
    pending: list[str] = []
    barrier_set = set(barriers)
    for task_id in order:
        pending.append(task_id)
        if task_id in barrier_set:
            regions.append(pending)
            pending = []
    if pending:
        regions.append(pending)
    return {
        "barriers": barriers,
        "regions": regions,
        "region_sizes": [len(region) for region in regions],
        "exact_composition_safe": len(regions) > 1,
    }


def decouple_zero_bubble(
    instance: MultiResourceInstance,
    task_meta: dict[str, dict],
) -> tuple[MultiResourceInstance, dict]:
    """Research-only F->W, no same-layer B->W transformation."""

    forward: dict[tuple, str] = {}
    for task_id, meta in task_meta.items():
        if meta["type"] == "compute" and meta["phase"] == "forward":
            key = (meta["node"], meta["iteration"], meta["layer_id"], meta["item_id"])
            forward[key] = task_id
    changed = 0
    removed = 0
    added = 0
    tasks = []
    for task in instance.dag.tasks:
        meta = task_meta.get(task.task_id)
        if not meta or meta["type"] != "compute" or meta["phase"] != "backward_weight":
            tasks.append(task)
            continue
        key = (meta["node"], meta["iteration"], meta["layer_id"], meta["item_id"])
        fwd = forward.get(key)
        deps = []
        for dependency in task.deps:
            parent = task_meta.get(dependency)
            parent_endpoint = None if not parent else (
                parent["node"] if parent["type"] == "compute" else parent["dst"]
            )
            same_b = parent and parent["phase"] == "backward_input" and (
                parent_endpoint, parent["iteration"], parent["layer_id"], parent["item_id"]
            ) == key
            if same_b:
                removed += 1
            else:
                deps.append(dependency)
        if fwd is not None and fwd not in deps:
            deps.append(fwd)
            added += 1
        changed += tuple(deps) != task.deps
        tasks.append(replace(task, deps=tuple(sorted(deps))))
    dag = replace(
        instance.dag,
        name=f"{instance.dag.name}_bw_decoupled",
        tasks=tuple(tasks),
        description=f"{instance.dag.description} Research-only true B/W fork.",
    )
    transformed = MultiResourceInstance(dag, instance.resources)
    errors = transformed.validate()
    if errors:
        raise ValueError(errors)
    return transformed, {
        "changed_w_nodes": changed,
        "removed_b_to_w_edges": removed,
        "added_f_to_w_edges": added,
    }


def scale_instance(
    instance: MultiResourceInstance,
    *,
    compute_scale: float = 1.0,
    communication_scale: float = 1.0,
) -> MultiResourceInstance:
    tasks = tuple(
        replace(
            task,
            duration=max(0 if task.kind == "compute" else 1, round(
                task.duration * (compute_scale if task.kind == "compute" else communication_scale)
            )),
        )
        for task in instance.dag.tasks
    )
    return MultiResourceInstance(replace(instance.dag, tasks=tasks), instance.resources)


def compute_only_bound(model: MultiResourceDAG) -> int:
    state = model.initial
    earliest = [0] * len(model.tasks)
    for index, task in enumerate(model.tasks):
        predecessor = max((earliest[parent] for parent in model.deps[index]), default=0)
        earliest[index] = predecessor + (model.remaining(state, index) if task.kind == "compute" else 0)
    return max(earliest, default=0)


def pq_stratum(p: int, q: int) -> str:
    ratio = p / max(q, 1)
    if ratio < 0.5:
        return "compute_dominated"
    if ratio <= 2.0:
        return "balanced_P_approx_Q"
    return "communication_dominated"


def _fast_scenario(
    *,
    mode: str,
    topology_name: str,
    ga: int,
    layers: int,
    quantum_us: float,
    compute_scale: float = 1.0,
    communication_scale: float = 1.0,
) -> dict:
    topologies = {
        "single_switch": single_switch_topology,
        "two_rack_uplink": two_rack_topology,
        "four_rack_core": four_rack_core_topology,
    }
    instance, info, _graph = build_route_aware_probe(
        mode=mode,
        topology_path=Path("unused"),
        topology=topologies[topology_name](),
        quantum_us=quantum_us,
        assigned_nodes=PLACEMENTS["tp_cross"],
        ga=ga,
        layers=layers,
    )
    instance = scale_instance(
        instance,
        compute_scale=compute_scale,
        communication_scale=communication_scale,
    )
    model = MultiResourceDAG(instance)
    bounds = multi_resource_lower_bounds(model)
    p = bounds["max_resource_load"]
    q = compute_only_bound(model)
    methods = {
        policy: asdict(schedule_multiresource(instance, policy))
        for policy in ("dynamic_tail", "resource_tail", "bottleneck_first", "spt", "lpt")
    }
    return {
        "mode": mode,
        "topology": topology_name,
        "ga": ga,
        "layers": layers,
        "quantum_us": quantum_us,
        "compute_scale": compute_scale,
        "communication_scale": communication_scale,
        "tasks": len(instance.dag.tasks),
        "flows": len(info["flow_meta"]),
        "P": p,
        "Q": q,
        "P_over_Q": p / max(q, 1),
        "stratum": pq_stratum(p, q),
        "lower_bounds": bounds,
        "partition": strict_series_partition(instance.dag),
        "methods": {
            name: {
                "makespan": value["makespan"],
                "ratio_to_LB": value["makespan"] / max(bounds["combined"], 1),
                "network_idle_ticks": value["network_idle_ticks"],
                "runtime_ms": value["runtime_ms"],
            }
            for name, value in methods.items()
        },
    }


def run_completion(*, samples: int, seed: int, include_expensive: bool) -> dict:
    parameter_points = (
        (1, 2, 25.0), (2, 2, 25.0), (2, 3, 50.0), (4, 2, 50.0),
    )
    fast = [
        _fast_scenario(
            mode=mode,
            topology_name=topology,
            ga=ga,
            layers=layers,
            quantum_us=quantum,
        )
        for mode in ("1f1b", "zero_bubble", "interleaved_1f1b", "bidirectional", "dualpipe")
        for topology in ("single_switch", "four_rack_core")
        for ga, layers, quantum in parameter_points
        if (mode != "bidirectional" or ga % 2 == 0)
        and (mode != "dualpipe" or ga >= 4)
    ]
    fast.extend(
        _fast_scenario(
            mode=mode,
            topology_name="four_rack_core",
            ga=2,
            layers=2,
            quantum_us=25.0,
            communication_scale=scale,
        )
        for mode in ("1f1b", "bidirectional")
        for scale in (8.0, 16.0, 32.0)
    )

    high, info, _graph = build_route_aware_probe(
        mode="zero_bubble",
        topology_path=Path("unused"),
        topology=four_rack_core_topology(),
        quantum_us=25.0,
        assigned_nodes=PLACEMENTS["tp_cross"],
        ga=2,
        layers=2,
    )
    decoupled, transform = decouple_zero_bubble(high, info["task_meta"])
    zb_comparison = {
        "transform": transform,
        "original": {
            policy: schedule_multiresource(high, policy).makespan
            for policy in ("dynamic_tail", "bottleneck_first")
        },
        "decoupled": {
            policy: schedule_multiresource(decoupled, policy).makespan
            for policy in ("dynamic_tail", "bottleneck_first")
        },
        "original_bounds": multi_resource_lower_bounds(MultiResourceDAG(high)),
        "decoupled_bounds": multi_resource_lower_bounds(MultiResourceDAG(decoupled)),
    }

    structured_details = {}
    if include_expensive:
        for mode in ("1f1b", "bidirectional"):
            instance, mode_info, _ = build_route_aware_probe(
                mode=mode,
                topology_path=Path("unused"),
                topology=four_rack_core_topology(),
                quantum_us=25.0,
                assigned_nodes=PLACEMENTS["tp_cross"],
                ga=2,
                layers=2,
            )
            cached = periodic_cached_rollout(instance, mode_info["flow_meta"])
            coarse_cached = periodic_cached_rollout(
                instance, mode_info["flow_meta"], include_load=False,
            )
            structured_details[mode] = {
                "ablation": leave_one_feature_out(instance, mode_info["flow_meta"]),
                "periodic_cache": {
                    "makespan": cached.schedule.makespan,
                    "runtime_ms": cached.schedule.runtime_ms,
                    "hits": cached.cache_hits,
                    "misses": cached.cache_misses,
                    "fallback": cached.fallback,
                },
                "periodic_cache_without_load": {
                    "makespan": coarse_cached.schedule.makespan,
                    "runtime_ms": coarse_cached.schedule.runtime_ms,
                    "hits": coarse_cached.cache_hits,
                    "misses": coarse_cached.cache_misses,
                    "fallback": coarse_cached.fallback,
                },
            }

    by_stratum: dict[str, list[dict]] = defaultdict(list)
    for row in fast:
        by_stratum[row["stratum"]].append(row)
    stratified = {
        stratum: {
            "scenarios": len(rows),
            "mean_ratio_to_LB": {
                method: mean(row["methods"][method]["ratio_to_LB"] for row in rows)
                for method in rows[0]["methods"]
            },
        }
        for stratum, rows in by_stratum.items()
    }

    return {
        "scope": {
            "production_integration": False,
            "resource_model": "preemptive integral route-resource occupancy",
            "known_unavailable_metrics": [
                "physical GPU idle (compute is precedence-only)",
                "activation memory (not represented)",
                "bandwidth-sharing preemptions (unit-capacity abstraction)",
            ],
        },
        "single_channel_exact": {
            "parallel_chains": evaluate_chains(samples, seed),
            "general_dag": evaluate_general(samples, seed + 1),
        },
        "topology_full_dag": fast,
        "stratified_summary": stratified,
        "zero_bubble_isolated_semantics": zb_comparison,
        "structured_details": structured_details,
    }


def evaluate_real_aicb_reductions(
    *,
    aicb_path: Path,
    topology_path: Path,
    bucket_count: int = 3,
) -> dict:
    """Evaluate exact route-aware reductions from an unmodified GPT profile."""

    full, info, graph = build_route_aware_aicb(
        aicb_path=aicb_path,
        topology_path=topology_path,
        mode="1f1b",
        quantum_us=100_000.0,
    )
    windows = []
    skipped = []
    for bucket_rank in range(bucket_count):
        try:
            dag = reduce_effective_dag(
                graph,
                max_flows=2,
                quantum_us=100_000.0,
                bucket_rank=bucket_rank,
            )
            resources = {
                task.task_id: full.resources[task.task_id]
                for task in dag.tasks if task.kind == "comm"
            }
            windows.append(MultiResourceInstance(dag, resources))
        except (KeyError, ValueError) as error:
            skipped.append({"bucket_rank": bucket_rank, "reason": str(error)})
    exact_windows = []
    uncertified = []
    for window in windows:
        try:
            exact_multiresource_oracle(window, max_states=50_000)
        except RuntimeError as error:
            model = MultiResourceDAG(window)
            bounds = multi_resource_lower_bounds(model)
            heuristics = {
                policy: schedule_multiresource(window, policy).makespan
                for policy in ("dynamic_tail", "resource_tail", "bottleneck_first")
            }
            uncertified.append({
                "name": window.dag.name,
                "reason": str(error),
                "tasks": len(window.dag.tasks),
                "lower_bounds": bounds,
                "heuristics": heuristics,
                "optimality_certified_by_bound": (
                    min(heuristics.values()) == bounds["combined"]
                ),
            })
        else:
            exact_windows.append(window)
    evaluation = evaluate_windows(
        exact_windows,
        max_states=50_000,
        include_single_channel=False,
    )
    evaluation["state_limited_windows"] = uncertified
    evaluation["bound_certified_windows"] = sum(
        item["optimality_certified_by_bound"] for item in uncertified
    )
    info.pop("flow_meta", None)
    return {
        "profile": info,
        "reduction": {
            "bucket_count": bucket_count,
            "quantum_us": 100_000.0,
            "skipped": skipped,
        },
        "evaluation": evaluation,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=30)
    parser.add_argument("--seed", type=int, default=260809)
    parser.add_argument("--include-expensive", action="store_true")
    parser.add_argument("--include-real-aicb", action="store_true")
    parser.add_argument("--real-aicb-only", action="store_true")
    parser.add_argument(
        "--output", type=Path,
        default=ROOT / "outputs" / "heuristic_plan_completion" / "report.json",
    )
    args = parser.parse_args()
    report = {} if args.real_aicb_only else run_completion(
        samples=args.samples, seed=args.seed, include_expensive=args.include_expensive,
    )
    if args.include_real_aicb or args.real_aicb_only:
        report["real_gpt_aicb"] = evaluate_real_aicb_reductions(
            aicb_path=(
                ROOT / "inputs" / "aicb-workload"
                / "A100-gpt_13B_ws16_pp2-world_size16-tp8-pp2-ep1-"
                "gbs16-mbs2-seq4096-MOE-False-GEMM-False-flash_attn-True.txt"
            ),
            topology_path=(
                ROOT / "inputs" / "topologies"
                / "AlibabaHPN_16g_8gps_DualToR_DualPlane_200Gbps_A100"
            ),
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
