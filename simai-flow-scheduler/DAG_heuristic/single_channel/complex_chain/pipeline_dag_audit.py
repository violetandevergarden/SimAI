"""Stage-0 semantic audit and structural metrics for training pipeline DAGs.

Unlike ``pipeline_dag_export.py``, this tool materializes the *effective* DAG:
the workload data-dependency edges plus the per-device compute serialization
edges imposed by ``ExecutionPlan.compute_order``.  It validates that graph,
computes structure metrics, and optionally compares it task-by-task with an
``AnalyticalExecutor`` timeline.

The default audit uses a small hybrid PP+TP+DP workload so PP/TP/DP joins and
flow slack are all observable while the graph remains human-inspectable.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
import json
from pathlib import Path
import sys
from typing import Iterable

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from DAG_heuristic.single_channel.complex_chain.pipeline_dag_export import (
    MODES,
    MODE_LABEL,
    stage_by_node,
)
from src.executor.analytical import AnalyticalExecutor
from src.executor.policies.default_policy import DefaultSchedulingPolicy
from src.static_analysis.passes.pipeline_task_serializers import (
    BidirectionalPipelineSerializer,
    DualPipeSerializer,
    InterleavedOneFOneBSerializer,
    ZeroBubbleSerializer,
)
from src.static_analysis.passes.routing import BfsStrategy
from src.static_analysis.passes.task_serializer import ExecutionPlan, OneFOneBSerializer
from src.static_analysis.passes.topology_loader import TopologyLoader
from src.static_analysis.strategies.default_strategy import DefaultAnalysisResult
from src.workload_format.schema import (
    CommType,
    Job,
    ParallelismConfig,
    Phase,
    P2PWorkload,
    Task,
)
from src.workload_generator.aicb_parser import AicbHeader, AicbWorkItem
from src.workload_generator.builders.bidirectional_pipeline_builder import (
    BidirectionalPipelineWorkloadBuilder,
)
from src.workload_generator.builders.dualpipe_pipeline_builder import (
    DualPipePipelineWorkloadBuilder,
)
from src.workload_generator.builders.interleaved_pipeline_builder import (
    InterleavedPipelineWorkloadBuilder,
)
from src.workload_generator.builders.zero_bubble_pipeline_builder import (
    ZeroBubblePipelineWorkloadBuilder,
)
from src.workload_generator.workload_builder import WorkloadBuilder


@dataclass
class BuiltMode:
    mode: str
    workload: P2PWorkload
    plan: ExecutionPlan
    header: AicbHeader


def build_hybrid_input(
    *,
    pp: int,
    tp: int,
    dp: int,
    ga: int,
    layers: int,
    pp_comm_size: int,
    tp_comm_size: int,
    dp_comm_size: int,
) -> tuple[AicbHeader, list[AicbWorkItem], Job]:
    """Build a homogeneous-stage PP+TP+DP probe workload."""
    header = AicbHeader(
        tp=tp,
        ep=1,
        pp=pp,
        vpp=layers,
        ga=ga,
        all_gpus=pp * tp * dp,
        pp_comm_size=pp_comm_size,
    )

    def item(
        name: str,
        fwd_ns: int,
        bwd_ns: int,
        w_ns: int,
        *,
        with_collectives: bool,
    ) -> AicbWorkItem:
        return AicbWorkItem(
            name=name,
            forward_compute_time=fwd_ns,
            forward_comm="ALLREDUCE" if with_collectives and tp > 1 else "NONE",
            forward_comm_size=tp_comm_size if with_collectives and tp > 1 else 0,
            backward_compute_time=bwd_ns,
            backward_comm="ALLREDUCE" if with_collectives and tp > 1 else "NONE",
            backward_comm_size=tp_comm_size if with_collectives and tp > 1 else 0,
            dp_compute_time=w_ns,
            dp_comm="NONE",
            dp_comm_size=0,
            process_time=100,
        )

    # Keep DP synchronization on the canonical gradient bucket row.  Besides
    # matching current AICB traces, this is the only unambiguous representation
    # accepted by the replica-aware DualPipe builder.
    grad_item = item("grad_param_comm", 1_000, 1_000, 1_000, with_collectives=False)
    if dp > 1:
        grad_item.dp_comm = "ALLREDUCE"
        grad_item.dp_comm_size = dp_comm_size
    items = [grad_item]
    for _microbatch in range(ga):
        for layer in range(layers):
            items.append(item(
                f"layer{layer}",
                900_000 + 100_000 * layer,
                1_500_000 + 100_000 * layer,
                600_000 + 50_000 * layer,
                # Per-layer collectives here are TP-scoped; DP synchronization
                # is represented by the canonical gradient bucket above.
                with_collectives=True,
            ))
    items.append(item("optimizer1", 0, 0, 0, with_collectives=False))
    job = Job(
        job_id=0,
        name="stage0-hybrid-audit",
        assigned_nodes=list(range(header.all_gpus)),
        parallelism=ParallelismConfig(tp=tp, dp=dp, pp=pp),
    )
    return header, items, job


def build_mode(
    mode: str,
    header: AicbHeader,
    items: list[AicbWorkItem],
    job: Job,
    *,
    vpp: int,
    gradient_sync_bytes: int,
) -> BuiltMode:
    sidecar: dict[int, object] = {}
    if mode == "1f1b":
        builder = WorkloadBuilder()
    elif mode == "interleaved_1f1b":
        builder = InterleavedPipelineWorkloadBuilder(vpp, sidecar)
    elif mode == "zero_bubble":
        builder = ZeroBubblePipelineWorkloadBuilder(sidecar)
    elif mode == "bidirectional":
        builder = BidirectionalPipelineWorkloadBuilder(
            sidecar,
            gradient_sync_bytes=gradient_sync_bytes,
        )
    elif mode == "dualpipe":
        builder = DualPipePipelineWorkloadBuilder(
            sidecar,
            gradient_sync_bytes=gradient_sync_bytes,
        )
    else:
        raise ValueError(f"unknown mode: {mode}")

    workload = builder.build_from_aicb(header, items, job, comm_algo="ring")
    errors = workload.validate()
    if errors:
        raise ValueError(f"{mode} workload validation failed: {errors}")

    stages = stage_by_node(job)
    if mode == "1f1b":
        serializer = OneFOneBSerializer(pp=header.pp, node_to_stage=stages)
    elif mode == "interleaved_1f1b":
        serializer = InterleavedOneFOneBSerializer(
            virtual_pipeline_size=vpp,
            expansion_task_info=sidecar,
        )
    elif mode == "zero_bubble":
        serializer = ZeroBubbleSerializer(expansion_task_info=sidecar)
    elif mode == "bidirectional":
        serializer = BidirectionalPipelineSerializer(expansion_task_info=sidecar)
    else:
        serializer = DualPipeSerializer(expansion_task_info=sidecar)
    plan = serializer.serialize(workload)
    validation = serializer.validate(workload, plan.compute_order)
    if validation:
        raise ValueError(f"{mode} compute_order validation failed: {validation}")
    return BuiltMode(mode, workload, plan, header)


def effective_edges(
    workload: P2PWorkload,
    plan: ExecutionPlan,
) -> tuple[set[tuple[int, int]], set[tuple[int, int]], set[tuple[int, int]]]:
    data = {
        (dependency, task.task_id)
        for task in workload.tasks
        for dependency in task.deps
    }
    resource = {
        (task_ids[index], task_ids[index + 1])
        for task_ids in plan.compute_order.values()
        for index in range(len(task_ids) - 1)
    }
    return data, resource, data | resource


def _topological_order(
    task_ids: Iterable[int],
    edges: set[tuple[int, int]],
) -> tuple[list[int], dict[int, list[int]], dict[int, list[int]], int]:
    ids = list(task_ids)
    predecessors = {task_id: [] for task_id in ids}
    successors = {task_id: [] for task_id in ids}
    for source, target in edges:
        predecessors[target].append(source)
        successors[source].append(target)
    degree = {task_id: len(predecessors[task_id]) for task_id in ids}
    ready = sorted(task_id for task_id, value in degree.items() if value == 0)
    max_ready = len(ready)
    result: list[int] = []
    while ready:
        task_id = ready.pop(0)
        result.append(task_id)
        for child in successors[task_id]:
            degree[child] -= 1
            if degree[child] == 0:
                ready.append(child)
        ready.sort()
        max_ready = max(max_ready, len(ready))
    if len(result) != len(ids):
        cyclic = sorted(task_id for task_id, value in degree.items() if value > 0)
        raise ValueError(f"effective DAG has a cycle involving {cyclic[:20]}")
    return result, predecessors, successors, max_ready


def _task_weight(task: Task, bandwidth_bytes_per_us: float) -> float:
    if task.is_compute():
        return float(task.duration_us or 0)
    return float(task.size_bytes or 0) / bandwidth_bytes_per_us


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    return values[round((len(values) - 1) * quantile)]


def _exact_poset_width(
    order: list[int],
    successors: dict[int, list[int]],
    *,
    limit: int = 600,
) -> int | None:
    """Exact maximum antichain width via Dilworth and bipartite matching."""
    if len(order) > limit:
        return None
    reach: dict[int, set[int]] = {task_id: set() for task_id in order}
    for task_id in reversed(order):
        for child in successors[task_id]:
            reach[task_id].add(child)
            reach[task_id].update(reach[child])

    right_match: dict[int, int] = {}

    def augment(left: int, seen: set[int]) -> bool:
        for right in reach[left]:
            if right in seen:
                continue
            seen.add(right)
            if right not in right_match or augment(right_match[right], seen):
                right_match[right] = left
                return True
        return False

    matching = sum(augment(task_id, set()) for task_id in order)
    return len(order) - matching


def _dominance_metrics(
    order: list[int],
    predecessors: dict[int, list[int]],
    successors: dict[int, list[int]],
) -> dict:
    """Return global dominators and conservative SESE-region candidates."""
    source = -1
    sink = -2
    ids = set(order) | {source, sink}
    pred = {task_id: list(predecessors.get(task_id, [])) for task_id in order}
    succ = {task_id: list(successors.get(task_id, [])) for task_id in order}
    roots = [task_id for task_id in order if not pred[task_id]]
    leaves = [task_id for task_id in order if not succ[task_id]]
    pred[source] = []
    succ[source] = roots
    pred[sink] = leaves
    succ[sink] = []
    for task_id in roots:
        pred[task_id].append(source)
    for task_id in leaves:
        succ[task_id].append(sink)

    topo = [source, *order, sink]
    dom = {task_id: set(ids) for task_id in ids}
    dom[source] = {source}
    for task_id in topo[1:]:
        common = set.intersection(*(dom[p] for p in pred[task_id]))
        dom[task_id] = {task_id} | common

    postdom = {task_id: set(ids) for task_id in ids}
    postdom[sink] = {sink}
    for task_id in reversed(topo[:-1]):
        common = set.intersection(*(postdom[c] for c in succ[task_id]))
        postdom[task_id] = {task_id} | common

    mandatory = sorted((dom[sink] & postdom[source]) - {source, sink})

    # Conservative single-entry/single-exit candidates using the nearest
    # strict postdominator as exit, followed by explicit boundary checks.
    reach: dict[int, set[int]] = {task_id: set() for task_id in topo}
    for task_id in reversed(topo):
        for child in succ[task_id]:
            reach[task_id].add(child)
            reach[task_id].update(reach[child])
    regions: set[tuple[int, int, int]] = set()
    for entry in order:
        candidates = postdom[entry] - {entry, sink}
        if not candidates:
            continue
        exit_task = max(candidates, key=lambda item: len(postdom[item]))
        region = {
            task_id for task_id in order
            if (task_id == entry or task_id in reach[entry])
            and (task_id == exit_task or exit_task in reach[task_id])
        }
        if len(region) < 3:
            continue
        incoming_ok = all(
            target == entry
            for target in region
            for source_task in pred[target]
            if source_task not in region and source_task != source
        )
        outgoing_ok = all(
            source_task == exit_task
            for source_task in region
            for target in succ[source_task]
            if target not in region and target != sink
        )
        if incoming_ok and outgoing_ok:
            regions.add((entry, exit_task, len(region)))

    sizes = [size for _, _, size in regions]
    return {
        "global_mandatory_nodes": len(mandatory),
        "global_mandatory_task_ids": mandatory,
        "sese_regions": len(regions),
        "largest_sese_region_nodes": max(sizes, default=0),
    }


def _flow_dimension(task: Task) -> str:
    value = task.comm_type.value
    if value.startswith("pp_"):
        return "PP"
    if value.startswith("dp_"):
        return "DP"
    if value.startswith("tp_"):
        return "TP"
    if value.startswith("ep_"):
        return "EP"
    return "OTHER"


def _operation(task: Task) -> str:
    if task.phase is Phase.FORWARD:
        return "F"
    if task.phase is Phase.BACKWARD_INPUT:
        return "B"
    if task.phase is Phase.BACKWARD_WEIGHT:
        return "W"
    return task.phase.value


def _periodicity(
    workload: P2PWorkload,
    plan: ExecutionPlan,
    stage_map: dict[int, int],
    ga: int,
) -> dict:
    tasks = {task.task_id: task for task in workload.tasks}
    by_stage: dict[int, list[dict]] = defaultdict(list)
    for node, task_ids in sorted(plan.compute_order.items()):
        blocks: list[tuple[str, int]] = []
        for task_id in task_ids:
            task = tasks[task_id]
            if not 0 <= task.iteration < ga:
                continue
            token = (_operation(task), task.iteration)
            if not blocks or blocks[-1] != token:
                blocks.append(token)
        labels = [operation for operation, _ in blocks]
        first_backward = next((i for i, label in enumerate(labels) if label == "B"), len(labels))
        last_forward = max((i for i, label in enumerate(labels) if label == "F"), default=-1)
        steady = labels[first_backward:last_forward + 1] if last_forward >= first_backward else []
        period = 0
        for candidate in range(1, len(steady) + 1):
            if all(label == steady[index % candidate] for index, label in enumerate(steady)):
                period = candidate
                break
        by_stage[stage_map[node]].append({
            "node": node,
            "tokens": " ".join(f"{op}{mb}" for op, mb in blocks),
            "warmup_blocks": first_backward,
            "steady_blocks": len(steady),
            "steady_label_period": period,
            "cooldown_blocks": max(0, len(labels) - last_forward - 1),
        })
    return {str(stage): entries for stage, entries in sorted(by_stage.items())}


def analyze_effective_dag(
    built: BuiltMode,
    *,
    bandwidth_bytes_per_us: float,
) -> tuple[dict, dict]:
    workload = built.workload
    tasks = {task.task_id: task for task in workload.tasks}
    stage_map = stage_by_node(workload.jobs[0])
    data, resource, edges = effective_edges(workload, built.plan)
    order, predecessors, successors, max_ready = _topological_order(tasks, edges)
    weights = {
        task_id: _task_weight(task, bandwidth_bytes_per_us)
        for task_id, task in tasks.items()
    }

    earliest_start: dict[int, float] = {}
    earliest_finish: dict[int, float] = {}
    critical_pred: dict[int, int | None] = {}
    depth: dict[int, int] = {}
    for task_id in order:
        if predecessors[task_id]:
            pred = max(predecessors[task_id], key=lambda item: earliest_finish[item])
            earliest_start[task_id] = earliest_finish[pred]
            critical_pred[task_id] = pred
            depth[task_id] = 1 + max(depth[item] for item in predecessors[task_id])
        else:
            earliest_start[task_id] = 0.0
            critical_pred[task_id] = None
            depth[task_id] = 0
        earliest_finish[task_id] = earliest_start[task_id] + weights[task_id]
    makespan = max(earliest_finish.values(), default=0.0)

    latest_finish: dict[int, float] = {}
    latest_start: dict[int, float] = {}
    for task_id in reversed(order):
        latest_finish[task_id] = (
            min(latest_start[child] for child in successors[task_id])
            if successors[task_id]
            else makespan
        )
        latest_start[task_id] = latest_finish[task_id] - weights[task_id]
    slack = {
        task_id: max(0.0, latest_start[task_id] - earliest_start[task_id])
        for task_id in order
    }

    critical_end = max(order, key=lambda item: earliest_finish[item])
    critical_path: list[int] = []
    cursor: int | None = critical_end
    while cursor is not None:
        critical_path.append(cursor)
        cursor = critical_pred[cursor]
    critical_path.reverse()

    events: list[tuple[float, int]] = []
    for task_id in order:
        if weights[task_id] <= 0:
            continue
        events.append((earliest_start[task_id], 1))
        events.append((earliest_finish[task_id], -1))
    active = 0
    max_active = 0
    for _time, delta in sorted(events, key=lambda item: (item[0], item[1])):
        active += delta
        max_active = max(max_active, active)

    in_degrees = {task_id: len(predecessors[task_id]) for task_id in order}
    out_degrees = {task_id: len(successors[task_id]) for task_id in order}
    joins = [task_id for task_id in order if in_degrees[task_id] > 1]
    forks = [task_id for task_id in order if out_degrees[task_id] > 1]

    flow_slack: dict[str, dict] = {}
    for dimension in ("PP", "TP", "DP", "EP", "OTHER"):
        values = [
            slack[task_id]
            for task_id, task in tasks.items()
            if task.is_flow() and _flow_dimension(task) == dimension
        ]
        if values:
            flow_slack[dimension] = {
                "count": len(values),
                "zero_slack": sum(value <= 1e-9 for value in values),
                "min_us": min(values),
                "p50_us": _percentile(values, 0.50),
                "p95_us": _percentile(values, 0.95),
                "max_us": max(values),
            }

    flow_ids = {task_id for task_id, task in tasks.items() if task.is_flow()}
    join_blocking_flows: set[int] = set()
    unique_join_blocking_flows: set[int] = set()
    for child in joins:
        pred_flows = [item for item in predecessors[child] if item in flow_ids]
        if not pred_flows:
            continue
        latest = max(earliest_finish[item] for item in predecessors[child])
        blockers = [item for item in pred_flows if abs(earliest_finish[item] - latest) <= 1e-9]
        join_blocking_flows.update(blockers)
        if len(blockers) == 1:
            unique_join_blocking_flows.add(blockers[0])

    templates = Counter()
    for task_id, task in tasks.items():
        source_stage = stage_map.get(task.src) if task.is_flow() else stage_map.get(task.node)
        target_stage = stage_map.get(task.dst) if task.is_flow() else source_stage
        templates[(
            task.type.value,
            source_stage,
            target_stage,
            task.phase.value,
            task.layer_id,
            task.comm_type.value if task.is_flow() else "compute",
            in_degrees[task_id],
            out_degrees[task_id],
        )] += 1
    repeated_tasks = sum(count for count in templates.values() if count > 1)

    path_tasks = [tasks[task_id] for task_id in critical_path]
    backbone_switches = {
        "stage": sum(
            stage_map.get(a.node if a.is_compute() else a.src)
            != stage_map.get(b.node if b.is_compute() else b.src)
            for a, b in zip(path_tasks, path_tasks[1:])
        ),
        "microbatch": sum(a.iteration != b.iteration for a, b in zip(path_tasks, path_tasks[1:])),
        "phase": sum(a.phase != b.phase for a, b in zip(path_tasks, path_tasks[1:])),
    }

    optimizer_like = [
        task for task in tasks.values()
        if task.iteration == built.header.ga and task.is_compute() and task.phase is Phase.FORWARD
    ]
    optimizer_direct_flow_deps = sum(
        tasks[dependency].is_flow()
        for task in optimizer_like
        for dependency in task.deps
    )
    optimizer_dependency_kinds = Counter(
        (
            tasks[dependency].type.value,
            tasks[dependency].phase.value,
            tasks[dependency].comm_type.value if tasks[dependency].is_flow() else "compute",
        )
        for task in optimizer_like
        for dependency in task.deps
    )
    optimizer_ancestors: set[int] = set()
    ancestor_queue = deque(
        dependency
        for task in optimizer_like
        for dependency in predecessors[task.task_id]
    )
    while ancestor_queue:
        dependency = ancestor_queue.popleft()
        if dependency in optimizer_ancestors:
            continue
        optimizer_ancestors.add(dependency)
        ancestor_queue.extend(predecessors[dependency])
    model_w = {
        task.task_id for task in tasks.values()
        if task.is_compute()
        and task.phase is Phase.BACKWARD_WEIGHT
        and 0 <= task.iteration < built.header.ga
    }
    dp_flows = {
        task.task_id for task in tasks.values()
        if task.is_flow() and _flow_dimension(task) == "DP"
    }

    metrics = {
        "mode": built.mode,
        "label": MODE_LABEL[built.mode],
        "nodes": {
            "total": len(tasks),
            "compute": sum(task.is_compute() for task in tasks.values()),
            "flow": sum(task.is_flow() for task in tasks.values()),
        },
        "edges": {
            "data": len(data),
            "compute_resource": len(resource),
            "compute_resource_new": len(resource - data),
            "effective": len(edges),
        },
        "sources": sum(not predecessors[task_id] for task_id in order),
        "sinks": sum(not successors[task_id] for task_id in order),
        "critical_path": {
            "makespan_us": makespan,
            "nodes": len(critical_path),
            "compute_nodes": sum(tasks[item].is_compute() for item in critical_path),
            "flow_nodes": sum(tasks[item].is_flow() for item in critical_path),
            "task_ids": critical_path,
            "switches": backbone_switches,
        },
        "width": {
            "exact_antichain": _exact_poset_width(order, successors),
            "max_topological_ready_frontier": max_ready,
            "max_asap_active_tasks": max_active,
        },
        "fork_join": {
            "fork_nodes": len(forks),
            "join_nodes": len(joins),
            "max_out_degree": max(out_degrees.values(), default=0),
            "max_in_degree": max(in_degrees.values(), default=0),
        },
        "dominance": _dominance_metrics(order, predecessors, successors),
        "flow_slack": flow_slack,
        "join_gating": {
            "flows": len(flow_ids),
            "last_blocker_flows": len(join_blocking_flows),
            "unique_last_blocker_flows": len(unique_join_blocking_flows),
            "last_blocker_ratio": len(join_blocking_flows) / max(len(flow_ids), 1),
        },
        "template_repetition": {
            "unique_templates_without_microbatch": len(templates),
            "repeated_task_fraction": repeated_tasks / max(len(tasks), 1),
            "max_template_repetitions": max(templates.values(), default=0),
        },
        "periodicity": _periodicity(workload, built.plan, stage_map, built.header.ga),
        "optimizer_audit": {
            "phase_optimizer_nodes": sum(task.phase is Phase.OPTIMIZER for task in tasks.values()),
            "post_forward_entry_nodes": len(optimizer_like),
            "post_forward_direct_flow_dependencies": optimizer_direct_flow_deps,
            "post_entry_direct_dependency_kinds": {
                "/".join(kind): count
                for kind, count in sorted(optimizer_dependency_kinds.items())
            },
            "model_backward_weight_ancestors": len(model_w & optimizer_ancestors),
            "model_backward_weight_total": len(model_w),
            "dp_flow_ancestors": len(dp_flows & optimizer_ancestors),
            "dp_flow_total": len(dp_flows),
        },
        "activation_memory": {
            "modeled_in_task_ir": False,
            "note": "No activation-size/lifetime field or memory dependency exists in Task IR.",
        },
    }
    graph = {
        "nodes": [
            {
                "id": task_id,
                "type": task.type.value,
                "rank": task.node if task.is_compute() else None,
                "src": task.src if task.is_flow() else None,
                "dst": task.dst if task.is_flow() else None,
                "stage": stage_map.get(task.node if task.is_compute() else task.src),
                "phase": task.phase.value,
                "iteration": task.iteration,
                "layer_id": task.layer_id,
                "duration_us": weights[task_id],
                "earliest_start_us": earliest_start[task_id],
                "earliest_finish_us": earliest_finish[task_id],
                "slack_us": slack[task_id],
                "critical": task_id in set(critical_path),
            }
            for task_id, task in sorted(tasks.items())
        ],
        "edges": [
            {
                "source": source,
                "target": target,
                "kinds": sorted(
                    kind for kind, members in (("data", data), ("compute_resource", resource))
                    if (source, target) in members
                ),
            }
            for source, target in sorted(edges)
        ],
        "compute_order": {str(node): task_ids for node, task_ids in built.plan.compute_order.items()},
    }
    return metrics, graph


def validate_timeline(
    built: BuiltMode,
    graph: dict,
    topology_path: Path,
) -> dict:
    topology = TopologyLoader().load(topology_path)
    routes = BfsStrategy().compute_routes(built.workload, topology)
    result = AnalyticalExecutor(
        topology,
        DefaultSchedulingPolicy(DefaultAnalysisResult(routes, built.plan)),
    ).execute(built.workload)
    timings = result.per_task
    violations = []
    for edge in graph["edges"]:
        source = edge["source"]
        target = edge["target"]
        if timings[source].end_time_us > timings[target].start_time_us:
            violations.append({
                "source": source,
                "target": target,
                "kinds": edge["kinds"],
                "source_end_us": timings[source].end_time_us,
                "target_start_us": timings[target].start_time_us,
            })

    task_by_id = {task.task_id: task for task in built.workload.tasks}
    rows = []
    for task_id, timing in sorted(
        timings.items(), key=lambda item: (item[1].start_time_us, item[1].end_time_us, item[0])
    ):
        task = task_by_id[task_id]
        rows.append({
            "task_id": task_id,
            "type": task.type.value,
            "node": task.node,
            "src": task.src,
            "dst": task.dst,
            "phase": task.phase.value,
            "iteration": task.iteration,
            "layer_id": task.layer_id,
            "start_us": timing.start_time_us,
            "end_us": timing.end_time_us,
        })
    return {
        "mode": built.mode,
        "topology": str(topology_path),
        "tasks_expected": len(built.workload.tasks),
        "tasks_completed": len(timings),
        "makespan_us": result.makespan_us,
        "effective_edge_violations": violations,
        "validated": len(timings) == len(built.workload.tasks) and not violations,
        "timeline": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--modes", nargs="+", choices=list(MODES), default=list(MODES))
    parser.add_argument("--pp", type=int, default=2)
    parser.add_argument("--tp", type=int, default=2)
    parser.add_argument("--dp", type=int, default=2)
    parser.add_argument("--ga", type=int, default=4)
    parser.add_argument("--layers", type=int, default=4)
    parser.add_argument("--vpp", type=int, default=2)
    parser.add_argument("--pp-comm", type=int, default=1_048_576)
    parser.add_argument("--tp-comm", type=int, default=524_288)
    parser.add_argument("--dp-comm", type=int, default=2_097_152)
    parser.add_argument("--gradient-sync", type=int, default=2_097_152)
    parser.add_argument("--bandwidth-gbps", type=float, default=200.0)
    parser.add_argument("--out", default="DAG_heuristic/outputs/dag_audit")
    parser.add_argument(
        "--timeline-mode",
        choices=list(MODES),
        default="1f1b",
        help="Mode checked task-by-task with AnalyticalExecutor.",
    )
    parser.add_argument(
        "--topology",
        default="inputs/topologies/AlibabaHPN_16g_8gps_DualToR_DualPlane_200Gbps_A100",
    )
    args = parser.parse_args()

    header, items, job = build_hybrid_input(
        pp=args.pp,
        tp=args.tp,
        dp=args.dp,
        ga=args.ga,
        layers=args.layers,
        pp_comm_size=args.pp_comm,
        tp_comm_size=args.tp_comm,
        dp_comm_size=args.dp_comm,
    )
    output = ROOT / args.out
    output.mkdir(parents=True, exist_ok=True)
    bandwidth = args.bandwidth_gbps * 1e9 / 8 / 1e6
    summaries = []
    timeline_result = None
    for mode in args.modes:
        built = build_mode(
            mode,
            header,
            items,
            job,
            vpp=args.vpp,
            gradient_sync_bytes=args.gradient_sync,
        )
        metrics, graph = analyze_effective_dag(
            built,
            bandwidth_bytes_per_us=bandwidth,
        )
        mode_dir = output / mode
        mode_dir.mkdir(parents=True, exist_ok=True)
        (mode_dir / "effective_dag.json").write_text(
            json.dumps(graph, indent=2), encoding="utf-8"
        )
        (mode_dir / "audit_summary.json").write_text(
            json.dumps(metrics, indent=2), encoding="utf-8"
        )
        summaries.append(metrics)
        print(
            f"[{mode}] nodes={metrics['nodes']['total']} "
            f"effective_edges={metrics['edges']['effective']} "
            f"width={metrics['width']['exact_antichain']} "
            f"cp={metrics['critical_path']['makespan_us']:.1f}us"
        )
        if mode == args.timeline_mode:
            timeline_result = validate_timeline(built, graph, ROOT / args.topology)
            (mode_dir / "timeline_validation.json").write_text(
                json.dumps(timeline_result, indent=2), encoding="utf-8"
            )

    aggregate = {
        "configuration": {
            "pp": args.pp,
            "tp": args.tp,
            "dp": args.dp,
            "ga": args.ga,
            "layers_per_stage": args.layers,
            "vpp": args.vpp,
            "bandwidth_gbps": args.bandwidth_gbps,
        },
        "modes": summaries,
        "timeline_validation": (
            None if timeline_result is None else {
                key: value for key, value in timeline_result.items() if key != "timeline"
            }
        ),
    }
    (output / "audit_report.json").write_text(
        json.dumps(aggregate, indent=2), encoding="utf-8"
    )
    print(f"Audit report: {output / 'audit_report.json'}")


if __name__ == "__main__":
    main()
