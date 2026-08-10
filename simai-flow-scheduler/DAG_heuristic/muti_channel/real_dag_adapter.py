"""Stage-4b: exact route-aware windows from an effective LLM training DAG.

The experiment uses the real workload builders, compute serializers, AlibabaHPN
topology loader and BFS routes.  Durations come from the repository's
homogeneous PP+TP+DP probe profile, so this is a structural/topological study,
not a claim about a measured GPT training trace.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict, deque
from dataclasses import asdict
import json
from pathlib import Path
from statistics import mean
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.audit_pipeline_dags import (  # noqa: E402
    analyze_effective_dag,
    build_hybrid_input,
    build_mode,
    effective_edges,
    stage_by_node,
)
from DAG_heuristic.common.benchmark import (  # noqa: E402
    BenchTask,
    BenchmarkDAG,
    _Builder,
    topological_order,
)
from DAG_heuristic.muti_channel.legacy_generator import (  # noqa: E402
    MultiResourceDAG,
    MultiResourceInstance,
    exact_multiresource_oracle,
    rollout_multiresource,
    route_resource_sets,
    schedule_multiresource,
)
from src.static_analysis.passes.routing import BfsStrategy  # noqa: E402
from src.static_analysis.passes.topology_loader import TopologyLoader  # noqa: E402
from src.static_analysis.passes.topology_loader import NetworkTopology  # noqa: E402
from src.static_analysis.passes.hermod_placement import assigned_nodes_for  # noqa: E402
from src.workload_format.schema import Job, ParallelismConfig  # noqa: E402
from src.workload_generator.aicb_parser import AicbParser  # noqa: E402


DEFAULT_TOPOLOGY = (
    ROOT / "inputs" / "topologies"
    / "AlibabaHPN_16g_8gps_DualToR_DualPlane_200Gbps_A100"
)


def _dimension(comm_type: str) -> str:
    for name in ("pp", "tp", "dp", "ep"):
        if comm_type.startswith(f"{name}_"):
            return name.upper()
    return "OTHER"


def build_route_aware_probe(
    *,
    mode: str,
    topology_path: Path,
    quantum_us: float,
    pp: int = 2,
    tp: int = 2,
    dp: int = 2,
    ga: int = 4,
    layers: int = 4,
    include_nic_resources: bool = True,
    assigned_nodes: list[int] | None = None,
    topology: NetworkTopology | None = None,
) -> tuple[MultiResourceInstance, dict, dict]:
    """Build an effective DAG and bind every flow to its real BFS route."""

    header, items, job = build_hybrid_input(
        pp=pp,
        tp=tp,
        dp=dp,
        ga=ga,
        layers=layers,
        pp_comm_size=1_048_576,
        tp_comm_size=524_288,
        dp_comm_size=2_097_152,
    )
    if assigned_nodes is not None:
        if len(assigned_nodes) != header.all_gpus:
            raise ValueError(
                f"assigned_nodes has {len(assigned_nodes)} entries; "
                f"expected {header.all_gpus}"
            )
        if len(set(assigned_nodes)) != len(assigned_nodes):
            raise ValueError("assigned_nodes must be unique")
        job.assigned_nodes = list(assigned_nodes)
    built = build_mode(
        mode,
        header,
        items,
        job,
        vpp=2,
        gradient_sync_bytes=2_097_152,
    )
    metrics, graph = analyze_effective_dag(
        built,
        bandwidth_bytes_per_us=25_000.0,
    )
    predecessors: dict[int, list[int]] = defaultdict(list)
    for edge in graph["edges"]:
        predecessors[edge["target"]].append(edge["source"])
    workload_tasks = {task.task_id: task for task in built.workload.tasks}
    tasks = []
    for node in sorted(graph["nodes"], key=lambda item: item["id"]):
        original = workload_tasks[node["id"]]
        is_compute = node["type"] == "compute"
        duration = round(float(node["duration_us"]) / quantum_us)
        duration = max(0 if is_compute else 1, duration)
        role = (
            f"{_dimension(original.comm_type.value)}:{node['phase']}:{node['stage']}"
            if not is_compute else f"COMPUTE:{node['phase']}:{node['stage']}"
        )
        tasks.append(BenchTask(
            f"t{node['id']}",
            "compute" if is_compute else "comm",
            duration,
            tuple(f"t{item}" for item in sorted(predecessors[node["id"]])),
            role,
        ))
    dag = BenchmarkDAG(
        f"{mode}_route_aware_probe",
        "llm_route_full",
        tuple(tasks),
        "Effective builder DAG bound to real AlibabaHPN BFS routes.",
        tuple(sorted({
            "mode": mode, "pp": pp, "tp": tp, "dp": dp,
            "ga": ga, "layers": layers, "quantum_us": quantum_us,
        }.items())),
    )
    errors = dag.validate()
    if errors:
        raise ValueError(errors)

    network = topology if topology is not None else TopologyLoader().load(topology_path)
    routes = BfsStrategy().compute_routes(built.workload, network)
    resources = route_resource_sets(
        built.workload,
        routes,
        directed=True,
        task_id_prefix="t",
        include_source_nic=include_nic_resources,
        include_destination_nic=include_nic_resources,
    )
    stage_width = dp * tp
    parallel_coords = {
        node: {
            "pp": position // stage_width,
            "dp": (position % stage_width) // tp,
            "tp": position % tp,
        }
        for position, node in enumerate(job.assigned_nodes)
    }
    flow_meta = {
        f"t{task.task_id}": {
            "dimension": _dimension(task.comm_type.value),
            "comm_type": task.comm_type.value,
            "phase": task.phase.value,
            "iteration": task.iteration,
            "layer_id": task.layer_id,
            "item_id": task.item_id,
            "chunk_id": task.chunk_id,
            "num_chunks": task.num_chunks,
            "stage": next(
                node["stage"] for node in graph["nodes"] if node["id"] == task.task_id
            ),
            "src": task.src,
            "dst": task.dst,
            "src_coord": parallel_coords[task.src],
            "dst_coord": parallel_coords[task.dst],
            "route": routes.get_path(task),
        }
        for task in built.workload.tasks if task.is_flow()
    }
    info = {
        "mode": mode,
        "effective_metrics": metrics,
        "tasks": len(tasks),
        "flows": len(resources),
        "route_resources": len(set().union(*resources.values())),
        "resource_model": "directed_links+nic_tx+nic_rx" if include_nic_resources else "directed_links",
        "route_hops": dict(Counter(
            len(item["route"]) - 1 for item in flow_meta.values()
        )),
        "resources_per_flow": dict(Counter(len(value) for value in resources.values())),
        "flow_dimensions": dict(Counter(
            item["dimension"] for item in flow_meta.values()
        )),
        "flow_meta": flow_meta,
        "task_meta": {
            f"t{task.task_id}": {
                "type": task.type.value,
                "phase": task.phase.value,
                "iteration": task.iteration,
                "layer_id": task.layer_id,
                "item_id": task.item_id,
                "node": task.node,
                "src": task.src,
                "dst": task.dst,
                "deps": [f"t{item}" for item in task.deps],
            }
            for task in built.workload.tasks
        },
    }
    return MultiResourceInstance(dag, resources), info, graph


def build_route_aware_aicb(
    *,
    aicb_path: Path,
    topology_path: Path,
    mode: str = "1f1b",
    quantum_us: float = 100.0,
    include_nic_resources: bool = True,
    dp_override: int | None = None,
    placement: str = "contiguous",
    gpus_per_server: int = 8,
) -> tuple[MultiResourceInstance, dict, dict]:
    """Bind an unmodified repository AICB profile to its physical BFS routes."""

    header, items = AicbParser().parse(str(aicb_path))
    topology = TopologyLoader().load(topology_path)
    if header.all_gpus > len(topology.gpu_nodes):
        raise ValueError(
            f"AICB needs {header.all_gpus} GPUs; topology has {len(topology.gpu_nodes)}"
        )
    header_dp = header.all_gpus // (header.tp * header.pp)
    dp = header_dp if dp_override is None else dp_override
    if dp < 1:
        raise ValueError("dp_override must be positive")
    if dp % header.ep:
        raise ValueError(f"DP={dp} must be divisible by EP={header.ep}")
    parallelism = ParallelismConfig(tp=header.tp, dp=dp, pp=header.pp, ep=header.ep)
    required_gpus = header.tp * dp * header.pp
    if required_gpus > len(topology.gpu_nodes):
        raise ValueError(
            f"expanded AICB needs {required_gpus} GPUs; "
            f"topology has {len(topology.gpu_nodes)}"
        )
    logical_nodes = assigned_nodes_for(parallelism, placement, gpus_per_server)
    if not set(logical_nodes) <= set(topology.gpu_nodes):
        raise ValueError(
            f"placement {placement!r} produced nodes outside the topology GPU set"
        )
    job = Job(
        job_id=0,
        name=f"aicb-{aicb_path.stem}",
        assigned_nodes=logical_nodes,
        parallelism=parallelism,
    )
    built = build_mode(
        mode, header, items, job,
        vpp=2,
        gradient_sync_bytes=max(
            (item.dp_comm_size for item in items if item.dp_comm_size > 0),
            default=2_097_152,
        ),
    )
    data_edges, serial_edges, all_edges = effective_edges(
        built.workload, built.plan,
    )
    stages = stage_by_node(job)
    graph = {
        "nodes": [
            {
                "id": task.task_id,
                "type": task.type.value,
                "duration_us": (
                    float(task.duration_us or 0) if task.is_compute()
                    else float(task.size_bytes or 0) / 25_000.0
                ),
                "phase": task.phase.value,
                "stage": stages.get(task.node if task.is_compute() else task.src, 0),
            }
            for task in built.workload.tasks
        ],
        "edges": [
            {"source": source, "target": target}
            for source, target in sorted(all_edges)
        ],
    }
    metrics = {
        "analysis": "linear effective-edge export; dominance metrics skipped",
        "nodes": len(graph["nodes"]),
        "data_edges": len(data_edges),
        "serializer_edges": len(serial_edges),
        "effective_edges": len(all_edges),
    }
    predecessors: dict[int, list[int]] = defaultdict(list)
    for edge in graph["edges"]:
        predecessors[edge["target"]].append(edge["source"])
    originals = {task.task_id: task for task in built.workload.tasks}
    tasks = []
    for node in sorted(graph["nodes"], key=lambda item: item["id"]):
        original = originals[node["id"]]
        is_compute = node["type"] == "compute"
        duration = round(float(node["duration_us"]) / quantum_us)
        duration = max(0 if is_compute else 1, duration)
        tasks.append(BenchTask(
            f"t{node['id']}",
            "compute" if is_compute else "comm",
            duration,
            tuple(f"t{item}" for item in sorted(predecessors[node["id"]])),
            (
                f"COMPUTE:{node['phase']}:{node['stage']}" if is_compute
                else f"{_dimension(original.comm_type.value)}:{node['phase']}:{node['stage']}"
            ),
        ))
    dag = BenchmarkDAG(
        f"{mode}_{aicb_path.stem}",
        "real_aicb_route_full",
        tuple(tasks),
        "Unmodified AICB timing profile with effective compute order and BFS routes.",
        tuple(sorted({
            "mode": mode, "tp": header.tp, "dp": dp, "pp": header.pp,
            "ga": header.ga, "quantum_us": quantum_us,
        }.items())),
    )
    errors = dag.validate()
    if errors:
        raise ValueError(errors)
    node_by_id = {node["id"]: node for node in graph["nodes"]}
    ordered_ids = [int(task_id[1:]) for task_id in topological_order(dag)]
    children: dict[int, list[int]] = defaultdict(list)
    earliest_finish: dict[int, float] = {}
    for task_id in ordered_ids:
        node = node_by_id[task_id]
        earliest_start = max(
            (earliest_finish[parent] for parent in predecessors[task_id]),
            default=0.0,
        )
        node["earliest_start_us"] = earliest_start
        node["earliest_finish_us"] = earliest_start + float(node["duration_us"])
        earliest_finish[task_id] = node["earliest_finish_us"]
        for parent in predecessors[task_id]:
            children[parent].append(task_id)
    downstream: dict[int, float] = {}
    for task_id in reversed(ordered_ids):
        downstream[task_id] = max(
            (
                float(node_by_id[child]["duration_us"]) + downstream[child]
                for child in children[task_id]
            ),
            default=0.0,
        )
    graph_makespan = max(earliest_finish.values(), default=0.0)
    for task_id in ordered_ids:
        node = node_by_id[task_id]
        latest_start = graph_makespan - float(node["duration_us"]) - downstream[task_id]
        node["slack_us"] = latest_start - float(node["earliest_start_us"])
    routes = BfsStrategy().compute_routes(built.workload, topology)
    resources = route_resource_sets(
        built.workload,
        routes,
        directed=True,
        task_id_prefix="t",
        include_source_nic=include_nic_resources,
        include_destination_nic=include_nic_resources,
    )
    stage_width = dp * header.tp
    coordinates = {
        node: {
            "pp": position // stage_width,
            "dp": (position % stage_width) // header.tp,
            "tp": position % header.tp,
        }
        for position, node in enumerate(job.assigned_nodes)
    }
    graph_nodes = {node["id"]: node for node in graph["nodes"]}
    flow_meta = {
        f"t{task.task_id}": {
            "dimension": _dimension(task.comm_type.value),
            "comm_type": task.comm_type.value,
            "phase": task.phase.value,
            "iteration": task.iteration,
            "layer_id": task.layer_id,
            "item_id": task.item_id,
            "chunk_id": task.chunk_id,
            "num_chunks": task.num_chunks,
            "stage": graph_nodes[task.task_id]["stage"],
            "src": task.src,
            "dst": task.dst,
            "src_coord": coordinates[task.src],
            "dst_coord": coordinates[task.dst],
            "route": routes.get_path(task),
        }
        for task in built.workload.tasks if task.is_flow()
    }
    return MultiResourceInstance(dag, resources), {
        "source": str(aicb_path),
        "header": {
            "tp": header.tp, "dp": dp, "pp": header.pp, "ep": header.ep,
            "ga": header.ga, "header_all_gpus": header.all_gpus,
            "expanded_all_gpus": required_gpus,
            "header_dp": header_dp,
            "dp_override": dp_override,
        },
        "placement": placement,
        "effective_metrics": metrics,
        "tasks": len(tasks),
        "flows": len(resources),
        "flow_dimensions": dict(Counter(item["dimension"] for item in flow_meta.values())),
        "route_hops": dict(Counter(len(item["route"]) - 1 for item in flow_meta.values())),
        "flow_meta": flow_meta,
    }, graph


def _route_decision_window(
    full: MultiResourceDAG,
    state: tuple[int, ...],
    seeds: list[int],
    window_index: int,
    *,
    depth: int,
    boundary_cap: int,
) -> MultiResourceInstance:
    analysis = full.residual.analyze(state)
    selected = set(seeds)
    queue = deque((seed, 0) for seed in seeds)
    while queue:
        node, level = queue.popleft()
        if level >= depth:
            continue
        for child in full.residual.children[node]:
            if state[child] != 0 and child not in selected:
                selected.add(child)
                queue.append((child, level + 1))

    builder = _Builder(
        f"route_decision_window_{window_index}",
        "llm_route_decision",
        "Residual LLM window where route-aware packed policies disagree.",
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

    window_resources: dict[str, frozenset] = {}
    for index in sorted(selected):
        task = full.tasks[index]
        deps = [
            f"n{parent}" for parent in full.deps[index]
            if parent in selected and state[parent] != 0
        ]
        if index in boundary:
            deps.append(boundary[index])
        name = builder.add(
            f"n{index}", task.kind, full.remaining(state, index), deps,
            role=task.role, cut=task.cut,
        )
        if task.kind == "comm":
            window_resources[name] = full.resources[index]

    for index in sorted(selected):
        internal = [
            child for child in full.residual.children[index]
            if child in selected and state[child] != 0
        ]
        if not internal and analysis.tail[index] > 0:
            builder.add(
                f"tail_{index}", "compute",
                min(boundary_cap, analysis.tail[index]), (f"n{index}",),
                role="boundary_tail",
            )
    return MultiResourceInstance(
        builder.finish(
            source_nodes=len(full.tasks),
            selected_nodes=len(selected),
            seed_flows=len(seeds),
        ),
        window_resources,
    )


def _ready_overlap(model: MultiResourceDAG, ready: list[int]) -> tuple[int, int]:
    conflicting = 0
    pairs = 0
    for position, left in enumerate(ready):
        for right in ready[position + 1:]:
            pairs += 1
            conflicting += bool(model.resources[left] & model.resources[right])
    return conflicting, pairs


def extract_route_windows(
    instance: MultiResourceInstance,
    *,
    target: int,
    depth: int = 0,
    max_tasks: int = 26,
    boundary_cap: int = 120,
    max_ticks: int = 20_000,
    exact_limit: int = 200_000,
) -> tuple[list[MultiResourceInstance], dict]:
    """Follow Dynamic-tail pack and retain exact-solvable disagreement windows."""

    model = MultiResourceDAG(instance)
    state = model.initial
    windows: list[MultiResourceInstance] = []
    elapsed = 0
    disagreements = 0
    candidate_windows = 0
    task_skips = 0
    exact_skips = 0
    last_snapshot = -100
    ready_widths: list[int] = []
    action_widths: list[int] = []
    overlap_conflicts = 0
    overlap_pairs = 0
    while not all(value == 0 for value in state) and elapsed < max_ticks:
        state = model.close(state)
        ready = model.ready(state)
        if not ready:
            state = model.tick(state, ())
            elapsed += 1
            continue
        dynamic = model.greedy_set(state, "dynamic_tail")
        resource = model.greedy_set(state, "resource_tail")
        bottleneck = model.greedy_set(state, "bottleneck_first")
        ready_widths.append(len(ready))
        action_widths.append(len(dynamic))
        conflicts, pairs = _ready_overlap(model, ready)
        overlap_conflicts += conflicts
        overlap_pairs += pairs
        alternatives = [
            resource,
            bottleneck,
            model.greedy_set(state, "spt"),
            model.greedy_set(state, "lpt"),
            model.greedy_set(state, "pp_first"),
            model.greedy_set(state, "dp_first"),
        ]
        differing = [choice for choice in alternatives if choice != dynamic]
        if differing:
            disagreements += 1
            if elapsed - last_snapshot >= 5 and len(windows) < target:
                candidate_windows += 1
                union = list(dict.fromkeys((*dynamic, *(x for item in differing for x in item))))
                analysis = model.residual.analyze(state)
                competitors = sorted(
                    (
                        index for index in ready
                        if any(model.resources[index] & model.resources[seed] for seed in union)
                    ),
                    key=lambda index: (analysis.tail[index], -index),
                    reverse=True,
                )
                seeds = list(dict.fromkeys((*union, *competitors)))[:6]
                candidate = _route_decision_window(
                    model, state, seeds, len(windows),
                    depth=depth, boundary_cap=boundary_cap,
                )
                if len(candidate.dag.tasks) > max_tasks:
                    task_skips += 1
                else:
                    try:
                        exact_multiresource_oracle(candidate, max_states=exact_limit)
                    except RuntimeError:
                        exact_skips += 1
                    else:
                        windows.append(candidate)
                        last_snapshot = elapsed
        state = model.tick(state, dynamic)
        elapsed += 1
    return windows, {
        "full_tasks": len(model.tasks),
        "simulated_ticks": elapsed,
        "policy_disagreement_ticks": disagreements,
        "candidate_windows": candidate_windows,
        "task_limit_skips": task_skips,
        "exact_state_limit_skips": exact_skips,
        "windows": len(windows),
        "mean_ready_width_when_nonempty": mean(ready_widths) if ready_widths else 0,
        "max_ready_width": max(ready_widths, default=0),
        "mean_parallel_action_width": mean(action_widths) if action_widths else 0,
        "max_parallel_action_width": max(action_widths, default=0),
        "ready_pair_conflict_fraction": overlap_conflicts / max(overlap_pairs, 1),
    }


def evaluate_windows(
    windows: list[MultiResourceInstance],
    *,
    max_states: int,
    include_single_channel: bool = True,
) -> dict:
    rows = []
    for instance in windows:
        oracle = exact_multiresource_oracle(instance, max_states=max_states)
        collapsed = MultiResourceInstance(
            instance.dag,
            {
                task.task_id: frozenset({"single-channel"})
                for task in instance.dag.tasks if task.kind == "comm"
            },
        )
        if include_single_channel:
            try:
                single = exact_multiresource_oracle(collapsed, max_states=max_states)
            except RuntimeError:
                single = None
        else:
            single = None
        results = {
            "dynamic_tail_pack": schedule_multiresource(instance, "dynamic_tail"),
            "resource_tail_pack": schedule_multiresource(instance, "resource_tail"),
            "bottleneck_first": schedule_multiresource(instance, "bottleneck_first"),
            "spt_pack": schedule_multiresource(instance, "spt"),
            "lpt_pack": schedule_multiresource(instance, "lpt"),
            "pp_first_pack": schedule_multiresource(instance, "pp_first"),
            "dp_first_pack": schedule_multiresource(instance, "dp_first"),
            "set_rollout_2": rollout_multiresource(instance, top_k=2),
        }
        dimensions = Counter(
            task.role.split(":", 1)[0]
            for task in instance.dag.tasks if task.kind == "comm"
        )
        rows.append({
            "name": instance.dag.name,
            "tasks": len(instance.dag.tasks),
            "flows": sum(task.kind == "comm" for task in instance.dag.tasks),
            "dimensions": dict(dimensions),
            "optimum": oracle.makespan,
            "single_channel_optimum": single.makespan if single else None,
            "single_channel_overestimate": (
                single.makespan / oracle.makespan if single else None
            ),
            "single_channel_oracle_status": (
                "exact" if single else "state_limit" if include_single_channel else "not_requested"
            ),
            "oracle_states": oracle.explored_states,
            "lower_bounds": oracle.lower_bounds,
            "methods": {
                name: {
                    "makespan": result.makespan,
                    "ratio": result.makespan / oracle.makespan,
                    "runtime_ms": result.runtime_ms,
                }
                for name, result in results.items()
            },
            "dag": [asdict(task) for task in instance.dag.tasks],
            "resources": {
                task_id: sorted(map(str, resources))
                for task_id, resources in instance.resources.items()
            },
        })

    method_names = next(iter(rows))["methods"] if rows else {}
    summary = {}
    for method in method_names:
        ratios = [row["methods"][method]["ratio"] for row in rows]
        summary[method] = {
            "mean_ratio": mean(ratios),
            "observed_max_ratio": max(ratios),
            "optimal_fraction": sum(value == 1 for value in ratios) / len(ratios),
            "mean_runtime_ms": mean(
                row["methods"][method]["runtime_ms"] for row in rows
            ),
        }
    distinguishing = sum(
        len({item["makespan"] for item in row["methods"].values()}) > 1
        for row in rows
    )
    return {
        "instances": len(rows),
        "distinguishing_windows": distinguishing,
        "single_channel_exact_windows": sum(
            row["single_channel_oracle_status"] == "exact" for row in rows
        ),
        "mean_single_channel_overestimate": mean([
            row["single_channel_overestimate"] for row in rows
            if row["single_channel_overestimate"] is not None
        ]) if any(
            row["single_channel_overestimate"] is not None for row in rows
        ) else None,
        "summary": summary,
        "rows": rows,
    }


def run_study(
    *,
    modes: list[str],
    topology_path: Path,
    target_per_mode: int,
    quantum_us: float,
    max_states: int,
    include_nic_resources: bool = True,
) -> dict:
    mode_reports = []
    all_windows: list[MultiResourceInstance] = []
    for mode in modes:
        instance, info, _graph = build_route_aware_probe(
            mode=mode,
            topology_path=topology_path,
            quantum_us=quantum_us,
            include_nic_resources=include_nic_resources,
        )
        windows, extraction = extract_route_windows(
            instance,
            target=target_per_mode,
            exact_limit=max_states,
        )
        for index, window in enumerate(windows):
            renamed = BenchmarkDAG(
                f"{mode}_{window.dag.name}_{index}",
                window.dag.category,
                window.dag.tasks,
                window.dag.description,
                window.dag.parameters,
            )
            all_windows.append(MultiResourceInstance(renamed, window.resources))
        info.pop("flow_meta")
        mode_reports.append({"mode": mode, "probe": info, "extraction": extraction})
    return {
        "config": {
            "modes": modes,
            "topology": str(topology_path),
            "target_per_mode": target_per_mode,
            "quantum_us": quantum_us,
            "max_states": max_states,
            "include_nic_resources": include_nic_resources,
        },
        "modes": mode_reports,
        "windows": evaluate_windows(all_windows, max_states=max_states),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--modes", nargs="+",
        default=[
            "1f1b", "interleaved_1f1b", "zero_bubble",
            "bidirectional", "dualpipe",
        ],
    )
    parser.add_argument("--topology", type=Path, default=DEFAULT_TOPOLOGY)
    parser.add_argument("--target-per-mode", type=int, default=6)
    parser.add_argument("--quantum-us", type=float, default=25.0)
    parser.add_argument("--max-states", type=int, default=200_000)
    parser.add_argument(
        "--link-only", action="store_true",
        help="Use only directed route links, without NIC TX/RX resources.",
    )
    parser.add_argument(
        "--output", type=Path,
        default=ROOT / "outputs" / "llm_route_windows" / "report.json",
    )
    args = parser.parse_args()
    report = run_study(
        modes=args.modes,
        topology_path=args.topology,
        target_per_mode=args.target_per_mode,
        quantum_us=args.quantum_us,
        max_states=args.max_states,
        include_nic_resources=not args.link_only,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({
        "modes": [item["extraction"] for item in report["modes"]],
        "windows": {
            key: value for key, value in report["windows"].items() if key != "rows"
        },
    }, indent=2))
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
