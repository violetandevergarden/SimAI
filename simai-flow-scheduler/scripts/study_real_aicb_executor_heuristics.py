"""Real-AICB heuristic replay on the analytical max-min executor.

The module is an isolated research path.  It reuses the real workload
builders, pipeline serializers, BFS routes and executor, while replacing only
the bandwidth allocator with strict priority tiers derived from effective-DAG
tail and residual route load.  No production policy is registered.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
import json
from pathlib import Path
from statistics import mean
import sys
from time import perf_counter


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.audit_pipeline_dags import build_mode, effective_edges  # noqa: E402
from src.executor.analytical import AnalyticalExecutor  # noqa: E402
from src.executor.bandwidth_allocators.hermod_allocator import HermodAllocator  # noqa: E402
from src.executor.policies.default_policy import DefaultSchedulingPolicy  # noqa: E402
from src.static_analysis.passes.hermod_placement import assigned_nodes_for  # noqa: E402
from src.static_analysis.passes.routing import BfsStrategy  # noqa: E402
from src.static_analysis.passes.topology_loader import TopologyLoader  # noqa: E402
from src.static_analysis.strategies.default_strategy import DefaultAnalysisResult  # noqa: E402
from src.workload_format.schema import Job, ParallelismConfig  # noqa: E402
from src.workload_generator.aicb_parser import AicbParser  # noqa: E402


def _dimension(comm_type: str) -> str:
    for value in ("pp", "tp", "dp", "ep"):
        if comm_type.startswith(f"{value}_"):
            return value.upper()
    return "OTHER"


@dataclass
class AllocatorTelemetry:
    calls: int = 0
    conflict_calls: int = 0
    paused_assignments: int = 0
    bandwidth_preemptions: int = 0
    runtime_ms: float = 0.0


class ResidualPriorityContext:
    """Static effective tails plus incrementally updated future route loads."""

    def __init__(
        self, workload, plan, route_table, topology, job: Job,
        gpus_per_server: int = 8,
        compute_estimate_scale: float = 1.0,
        communication_estimate_scale: float = 1.0,
        estimate_noise: float = 0.0,
        noise_seed: int = 0,
        tail_bucket_us: float = 0.0,
    ):
        self.tasks = {task.task_id: task for task in workload.tasks}
        _data, _serial, edges = effective_edges(workload, plan)
        children: dict[int, list[int]] = defaultdict(list)
        indegree = {task_id: 0 for task_id in self.tasks}
        for source, target in edges:
            children[source].append(target)
            indegree[target] += 1
        ready = deque(sorted(task_id for task_id, degree in indegree.items() if degree == 0))
        order = []
        while ready:
            task_id = ready.popleft()
            order.append(task_id)
            for child in children[task_id]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    ready.append(child)
        if len(order) != len(self.tasks):
            raise ValueError("effective DAG is cyclic")

        self.paths = {
            task.task_id: route_table.get_path(task)
            for task in workload.tasks if task.is_flow()
        }
        self.links = {
            task_id: tuple(zip(path, path[1:]))
            for task_id, path in self.paths.items()
        }
        duration = {}
        for task_id, task in self.tasks.items():
            hashed = (task_id ^ (noise_seed * 0x9E3779B9)) & 0xFFFFFFFF
            hashed ^= hashed >> 16
            hashed = (hashed * 0x7FEB352D) & 0xFFFFFFFF
            hashed ^= hashed >> 15
            hashed = (hashed * 0x846CA68B) & 0xFFFFFFFF
            hashed ^= hashed >> 16
            noise_multiplier = 1.0 + estimate_noise * (
                2.0 * hashed / 0xFFFFFFFF - 1.0
            )
            if task.is_compute():
                duration[task_id] = (
                    float(task.duration_us or 0) * compute_estimate_scale
                    * noise_multiplier
                )
            else:
                path = self.paths[task_id]
                capacities = [
                    topology.get_link(src, dst).bandwidth_gbps
                    for src, dst in zip(path, path[1:])
                    if topology.get_link(src, dst) is not None
                ]
                bandwidth = min(capacities, default=1.0)
                duration[task_id] = (
                    float(task.size_bytes or 0) * 8 / (bandwidth * 1e3)
                    * communication_estimate_scale
                    * noise_multiplier
                )
        self.tail = {}
        for task_id in reversed(order):
            self.tail[task_id] = max(
                (duration[child] + self.tail[child] for child in children[task_id]),
                default=0.0,
            )

        self.remaining_link_bytes: dict[tuple[int, int], int] = defaultdict(int)
        for task in workload.tasks:
            if task.is_flow():
                for link in self.links[task.task_id]:
                    self.remaining_link_bytes[link] += int(task.size_bytes or 0)
        self.completed_flows: set[int] = set()
        tp = job.parallelism.tp
        stage_width = job.parallelism.dp * tp
        self.coords = {
            node: (
                position // stage_width,
                (position % stage_width) // tp,
                position % tp,
            )
            for position, node in enumerate(job.assigned_nodes)
        }
        self.replica_server_order = []
        for pp_index in range(job.parallelism.pp):
            servers = []
            for dp_index in range(job.parallelism.dp):
                logical = (pp_index * job.parallelism.dp + dp_index) * tp
                servers.append(job.assigned_nodes[logical] // gpus_per_server)
            self.replica_server_order.append(servers)
        self.replica_server_monotone = all(
            servers == sorted(servers) for servers in self.replica_server_order
        )
        self.tail_bucket_us = tail_bucket_us

    def complete(self, task_id: int) -> None:
        task = self.tasks[task_id]
        if not task.is_flow() or task_id in self.completed_flows:
            return
        self.completed_flows.add(task_id)
        for link in self.links[task_id]:
            self.remaining_link_bytes[link] -= int(task.size_bytes or 0)

    def bottleneck_load_us(self, task_id: int, topology) -> float:
        values = []
        for link in self.links[task_id]:
            obj = topology.get_link(*link)
            if obj is not None:
                values.append(self.remaining_link_bytes[link] * 8 / (obj.bandwidth_gbps * 1e3))
        return max(values, default=0.0)

    def priority(self, task_id: int, policy: str, topology) -> tuple:
        task = self.tasks[task_id]
        tail = round(self.tail[task_id], 6)
        if self.tail_bucket_us > 0:
            tail = (tail // self.tail_bucket_us) * self.tail_bucket_us
        load = round(self.bottleneck_load_us(task_id, topology), 6)
        if policy == "dynamic_tail":
            return (tail,)
        if policy == "resource_tail":
            return (tail, load)
        if policy == "bottleneck_first":
            return (load, tail)
        if policy in {
            "dimension_tie", "replica_tie", "chunk_tie", "route_tie",
            "replica_route_tie", "guarded_replica_tie", "llm_tie",
        }:
            endpoint = task.src
            _pp, dp, tp = self.coords.get(endpoint, (0, 0, 0))
            dimension = _dimension(task.comm_type.value)
            backbone = dimension in {"PP", "TP"} and task.phase.value in {
                "forward", "backward_input",
            }
            dimension_order = {"PP": 4, "TP": 3, "DP": 2, "EP": 1}.get(dimension, 0)
            if policy == "dimension_tie":
                return (tail, backbone, dimension_order)
            if policy == "replica_tie":
                return (tail, -dp, -tp)
            if policy == "guarded_replica_tie":
                return (
                    (tail, -dp, -tp)
                    if self.replica_server_monotone else (tail,)
                )
            if policy == "chunk_tie":
                return (tail, -(task.chunk_id or 0))
            if policy == "route_tie":
                return (tail, -len(self.links[task_id]), -load)
            if policy == "replica_route_tie":
                return (tail, -len(self.links[task_id]), -load, -dp, -tp)
            return (
                tail,
                backbone,
                dimension_order,
                -dp,
                -tp,
                -(task.chunk_id or 0),
            )
        raise ValueError(f"unknown executor heuristic: {policy}")


class ResidualPriorityAllocator:
    """Strict priority across scores, progressive max-min inside one score."""

    def __init__(self, context: ResidualPriorityContext, policy: str):
        self.context = context
        self.policy = policy
        self.telemetry = AllocatorTelemetry()
        self._last_positive: set[int] = set()
        self._last_signature: tuple[int, tuple[int, ...]] | None = None

    def allocate(self, active_flows, topology, current_time=0):
        started = perf_counter()
        self.telemetry.calls += 1
        link_rem = {}
        link_users: Counter[tuple[int, int]] = Counter()
        for flow in active_flows:
            for link in zip(flow.path, flow.path[1:]):
                link_users[link] += 1
                if link not in link_rem:
                    obj = topology.get_link(*link)
                    link_rem[link] = obj.bandwidth_gbps if obj else 0.0
        signature = (current_time, tuple(sorted(flow.task_id for flow in active_flows)))
        if signature != self._last_signature and any(value > 1 for value in link_users.values()):
            self.telemetry.conflict_calls += 1
        self._last_signature = signature

        tiers: dict[tuple, list] = defaultdict(list)
        for flow in active_flows:
            score = self.context.priority(flow.task_id, self.policy, topology)
            tiers[score].append(flow)
        result = {}
        for score in sorted(tiers, reverse=True):
            HermodAllocator._allocate_tier(tiers[score], link_rem, result)
        positive = {task_id for task_id, bandwidth in result.items() if bandwidth > 0}
        active_ids = {flow.task_id for flow in active_flows}
        self.telemetry.bandwidth_preemptions += len(
            (self._last_positive & active_ids) - positive
        )
        self.telemetry.paused_assignments += sum(
            bandwidth <= 0 for bandwidth in result.values()
        )
        self._last_positive = positive
        self.telemetry.runtime_ms += (perf_counter() - started) * 1_000
        return {flow.task_id: result.get(flow.task_id, 0.0) for flow in active_flows}


class ResidualPriorityPolicy(DefaultSchedulingPolicy):
    def __init__(self, analysis, context, policy: str):
        super().__init__(analysis)
        self.context = context
        self.allocator = ResidualPriorityAllocator(context, policy)

    def on_task_completed(self, current_time, task) -> None:
        super().on_task_completed(current_time, task)
        self.context.complete(task.task_id)


def build_real_experiment(
    *,
    aicb_path: Path,
    topology_path: Path,
    dp: int,
    placement: str,
    mode: str,
):
    header, items = AicbParser().parse(str(aicb_path))
    topology = TopologyLoader().load(topology_path)
    parallelism = ParallelismConfig(tp=header.tp, dp=dp, pp=header.pp, ep=header.ep)
    nodes = assigned_nodes_for(parallelism, placement, 8)
    if not set(nodes) <= set(topology.gpu_nodes):
        raise ValueError("placement needs GPU ids outside topology")
    job = Job(
        job_id=0,
        name=aicb_path.stem,
        assigned_nodes=nodes,
        parallelism=parallelism,
    )
    gradient_bytes = max(
        (item.dp_comm_size for item in items if item.dp_comm_size > 0),
        default=2_097_152,
    )
    built = build_mode(
        mode, header, items, job,
        vpp=2,
        gradient_sync_bytes=gradient_bytes,
    )
    routes = BfsStrategy().compute_routes(built.workload, topology)
    analysis = DefaultAnalysisResult(routes, built.plan)
    return header, job, built.workload, topology, analysis


def _unlock_delay(result, workload) -> dict:
    timings = result.per_task
    children: dict[int, list[int]] = defaultdict(list)
    tasks = {task.task_id: task for task in workload.tasks}
    for task in workload.tasks:
        for parent in task.deps:
            children[parent].append(task.task_id)
    values = []
    for task in workload.tasks:
        if not task.is_flow() or _dimension(task.comm_type.value) != "PP":
            continue
        for child in children[task.task_id]:
            if tasks[child].is_compute() and task.task_id in timings and child in timings:
                values.append(max(
                    0,
                    timings[child].start_time_us - timings[task.task_id].end_time_us,
                ))
    return {
        "mean_pp_compute_unlock_delay_us": mean(values) if values else 0,
        "max_pp_compute_unlock_delay_us": max(values, default=0),
    }


def run_case(
    *,
    aicb_path: Path,
    topology_path: Path,
    dp: int,
    placement: str,
    mode: str,
    policies: tuple[str, ...],
) -> dict:
    header, job, workload, topology, analysis = build_real_experiment(
        aicb_path=aicb_path,
        topology_path=topology_path,
        dp=dp,
        placement=placement,
        mode=mode,
    )
    methods = {}
    placement_features = None
    estimate_profiles = {
        "dynamic_compute_80": ("dynamic_tail", 0.8, 1.0, 0.0, 0, 0.0),
        "dynamic_compute_120": ("dynamic_tail", 1.2, 1.0, 0.0, 0, 0.0),
        "dynamic_comm_80": ("dynamic_tail", 1.0, 0.8, 0.0, 0, 0.0),
        "dynamic_comm_120": ("dynamic_tail", 1.0, 1.2, 0.0, 0, 0.0),
        "dynamic_bucket_1ms": ("dynamic_tail", 1.0, 1.0, 0.0, 0, 1_000.0),
        "dynamic_bucket_100ms": ("dynamic_tail", 1.0, 1.0, 0.0, 0, 100_000.0),
        "dynamic_noise_20_s1": ("dynamic_tail", 1.0, 1.0, 0.2, 1, 0.0),
        "dynamic_noise_20_s2": ("dynamic_tail", 1.0, 1.0, 0.2, 2, 0.0),
        "dynamic_noise_20_s3": ("dynamic_tail", 1.0, 1.0, 0.2, 3, 0.0),
        "dynamic_noise_20_bucket_1ms_s1": (
            "dynamic_tail", 1.0, 1.0, 0.2, 1, 1_000.0,
        ),
        "dynamic_noise_20_bucket_1ms_s2": (
            "dynamic_tail", 1.0, 1.0, 0.2, 2, 1_000.0,
        ),
        "dynamic_noise_20_bucket_1ms_s3": (
            "dynamic_tail", 1.0, 1.0, 0.2, 3, 1_000.0,
        ),
        "dynamic_noise_20_bucket_100ms_s1": (
            "dynamic_tail", 1.0, 1.0, 0.2, 1, 100_000.0,
        ),
    }
    for policy_name in policies:
        if policy_name == "default":
            policy = DefaultSchedulingPolicy(analysis)
            started = perf_counter()
            result = AnalyticalExecutor(topology, policy).execute(workload)
            runtime_ms = (perf_counter() - started) * 1_000
            telemetry = None
        else:
            (
                allocator_policy, compute_scale, communication_scale,
                estimate_noise, noise_seed, tail_bucket_us,
            ) = estimate_profiles.get(
                policy_name, (policy_name, 1.0, 1.0, 0.0, 0, 0.0),
            )
            context = ResidualPriorityContext(
                workload, analysis.execution_plan, analysis.route_table, topology, job,
                compute_estimate_scale=compute_scale,
                communication_estimate_scale=communication_scale,
                estimate_noise=estimate_noise,
                noise_seed=noise_seed,
                tail_bucket_us=tail_bucket_us,
            )
            if placement_features is None:
                placement_features = {
                    "replica_server_order": context.replica_server_order,
                    "replica_server_monotone": context.replica_server_monotone,
                }
            policy = ResidualPriorityPolicy(analysis, context, allocator_policy)
            started = perf_counter()
            result = AnalyticalExecutor(topology, policy).execute(workload)
            runtime_ms = (perf_counter() - started) * 1_000
            telemetry = policy.allocator.telemetry
        methods[policy_name] = {
            "makespan_us": result.makespan_us,
            "executor_runtime_ms": runtime_ms,
            **_unlock_delay(result, workload),
            **({} if telemetry is None else {
                "allocator": {
                    "calls": telemetry.calls,
                    "conflict_calls": telemetry.conflict_calls,
                    "paused_assignments": telemetry.paused_assignments,
                    "bandwidth_preemptions": telemetry.bandwidth_preemptions,
                    "runtime_ms": telemetry.runtime_ms,
                },
            }),
        }
    baseline = methods["default"]["makespan_us"]
    for value in methods.values():
        value["improvement_vs_default"] = (
            (baseline - value["makespan_us"]) / baseline if baseline else 0
        )
    return {
        "aicb": str(aicb_path),
        "model": aicb_path.name.split("_ws", 1)[0],
        "mode": mode,
        "parallelism": {
            "tp": header.tp, "dp": dp, "pp": header.pp, "ep": header.ep,
            "ga": header.ga,
        },
        "placement": placement,
        "placement_features": placement_features,
        "topology": str(topology_path),
        "tasks": len(workload.tasks),
        "flows": len(workload.get_flow_tasks()),
        "flow_dimensions": dict(Counter(
            _dimension(task.comm_type.value) for task in workload.get_flow_tasks()
        )),
        "methods": methods,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--aicb", type=Path)
    inputs.add_argument("--aicbs", nargs="+", type=Path)
    parser.add_argument("--topology", type=Path, required=True)
    parser.add_argument("--dp", type=int, required=True)
    parser.add_argument("--placement", choices=["contiguous", "cyclic_pp_dp"], default="contiguous")
    parser.add_argument(
        "--placements", nargs="+", choices=["contiguous", "cyclic_pp_dp"],
    )
    parser.add_argument("--mode", choices=["1f1b", "bidirectional"], default="1f1b")
    parser.add_argument("--modes", nargs="+", choices=["1f1b", "bidirectional"])
    parser.add_argument(
        "--policies", nargs="+",
        choices=[
            "default", "dynamic_tail", "resource_tail", "bottleneck_first",
            "dimension_tie", "replica_tie", "chunk_tie", "route_tie",
            "replica_route_tie", "guarded_replica_tie", "llm_tie",
            "dynamic_compute_80", "dynamic_compute_120",
            "dynamic_comm_80", "dynamic_comm_120",
            "dynamic_noise_20_s1", "dynamic_noise_20_s2", "dynamic_noise_20_s3",
            "dynamic_bucket_1ms",
            "dynamic_bucket_100ms",
            "dynamic_noise_20_bucket_1ms_s1",
            "dynamic_noise_20_bucket_1ms_s2",
            "dynamic_noise_20_bucket_1ms_s3",
            "dynamic_noise_20_bucket_100ms_s1",
        ],
        default=["default", "dynamic_tail", "resource_tail", "llm_tie"],
    )
    parser.add_argument(
        "--output", type=Path,
        default=ROOT / "outputs" / "real_aicb_executor_heuristics" / "case.json",
    )
    args = parser.parse_args()
    policies = tuple(dict.fromkeys(("default", *args.policies)))
    aicbs = args.aicbs or [args.aicb]
    placements = args.placements or [args.placement]
    modes = args.modes or [args.mode]
    cases = [
        run_case(
            aicb_path=aicb,
            topology_path=args.topology,
            dp=args.dp,
            placement=placement,
            mode=mode,
            policies=policies,
        )
        for aicb in aicbs
        for placement in placements
        for mode in modes
    ]
    report = cases[0] if len(cases) == 1 else {
        "config": {
            "aicbs": [str(path) for path in aicbs],
            "topology": str(args.topology),
            "dp": args.dp,
            "placements": placements,
            "modes": modes,
            "policies": policies,
        },
        "cases": cases,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
