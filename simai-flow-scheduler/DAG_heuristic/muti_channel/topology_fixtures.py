"""Stage-4c: LLM scheduling sensitivity on hand-built small topologies.

The DAG is still produced by the real pipeline builders and serializers.  The
network is deliberately reduced to three transparent 8-GPU graphs so that PP,
DP or TP traffic can be placed across known bottlenecks.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from statistics import mean
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from DAG_heuristic.muti_channel.real_dag_adapter import (  # noqa: E402
    build_route_aware_probe,
    evaluate_windows,
    extract_route_windows,
)
from DAG_heuristic.muti_channel.legacy_generator import (  # noqa: E402
    MultiResourceDAG,
    MultiResourceInstance,
    multi_resource_lower_bounds,
    rollout_multiresource,
    schedule_multiresource,
)
from src.static_analysis.passes.topology_loader import (  # noqa: E402
    Link,
    NetworkTopology,
    NodeType,
)


def _add_bidirectional(
    topology: NetworkTopology,
    src: int,
    dst: int,
    *,
    bandwidth_gbps: float = 200.0,
    latency_us: float = 1.0,
) -> None:
    topology.add_link(Link(src, dst, bandwidth_gbps, latency_us, 0.0))
    topology.add_link(Link(dst, src, bandwidth_gbps, latency_us, 0.0))


def _finish_topology(
    topology: NetworkTopology,
    switches: list[int],
    name: str,
) -> NetworkTopology:
    topology.total_nodes = 8 + len(switches)
    topology.gpu_count = 8
    topology.switch_count = len(switches)
    topology.gpu_nodes = list(range(8))
    topology.switch_nodes = switches
    topology.gpu_type = name
    topology.node_types.update({gpu: NodeType.GPU for gpu in range(8)})
    topology.node_types.update({switch: NodeType.ASW_SWITCH for switch in switches})
    return topology


def single_switch_topology() -> NetworkTopology:
    topology = NetworkTopology()
    for gpu in range(8):
        _add_bidirectional(topology, gpu, 8)
    return _finish_topology(topology, [8], "manual_single_switch")


def two_rack_topology() -> NetworkTopology:
    """GPUs 0--3 and 4--7 share one inter-rack directed uplink pair."""

    topology = NetworkTopology()
    for gpu in range(4):
        _add_bidirectional(topology, gpu, 8)
    for gpu in range(4, 8):
        _add_bidirectional(topology, gpu, 9)
    _add_bidirectional(topology, 8, 9, bandwidth_gbps=100.0, latency_us=2.0)
    return _finish_topology(topology, [8, 9], "manual_two_rack")


def four_rack_core_topology() -> NetworkTopology:
    """Two GPUs per rack; all rack switches attach to one shared core."""

    topology = NetworkTopology()
    rack_switches = [8, 9, 10, 11]
    for rack, switch in enumerate(rack_switches):
        for gpu in (2 * rack, 2 * rack + 1):
            _add_bidirectional(topology, gpu, switch)
        _add_bidirectional(topology, switch, 12, bandwidth_gbps=100.0, latency_us=2.0)
    return _finish_topology(topology, [*rack_switches, 12], "manual_four_rack_core")


TOPOLOGIES = {
    "single_switch": single_switch_topology,
    "two_rack": two_rack_topology,
    "four_rack_core": four_rack_core_topology,
}

# Logical rank order is [PP][DP][TP].
PLACEMENTS = {
    # Stage 0 occupies rack 0, stage 1 rack 1: PP crosses the uplink.
    "pp_cross": [0, 1, 2, 3, 4, 5, 6, 7],
    # Each stage has one DP replica in each rack: DP crosses, PP stays local.
    "dp_cross": [0, 1, 4, 5, 2, 3, 6, 7],
    # Every TP pair is split across racks; deliberately contention-heavy.
    "tp_cross": [0, 4, 1, 5, 2, 6, 3, 7],
}


def _full_schedule_metrics(instance: MultiResourceInstance) -> dict:
    model = MultiResourceDAG(instance)
    results = {
        policy: schedule_multiresource(instance, policy)
        for policy in (
            "dynamic_tail", "resource_tail", "bottleneck_first",
            "lpt",
        )
    }
    best = min(result.makespan for result in results.values())
    initial_loads: dict[str, int] = {}
    for index, task in enumerate(model.tasks):
        if task.kind != "comm":
            continue
        for resource in model.resources[index]:
            name = str(resource)
            initial_loads[name] = initial_loads.get(name, 0) + task.duration
    return {
        "best_observed": best,
        "max_initial_resource_load": max(initial_loads.values(), default=0),
        "methods": {
            policy: {
                "makespan": result.makespan,
                "ratio_to_best_observed": result.makespan / best,
                "runtime_ms": result.runtime_ms,
            }
            for policy, result in results.items()
        },
    }


def run_sensitivity(
    *,
    modes: list[str],
    target_per_scenario: int,
    quantum_us: float,
    max_states: int,
) -> dict:
    scenarios = []
    all_windows: list[MultiResourceInstance] = []
    for topology_name, topology_factory in TOPOLOGIES.items():
        for placement_name, assigned_nodes in PLACEMENTS.items():
            for mode in modes:
                instance, info, _graph = build_route_aware_probe(
                    mode=mode,
                    topology_path=Path("unused-when-topology-is-supplied"),
                    topology=topology_factory(),
                    quantum_us=quantum_us,
                    assigned_nodes=assigned_nodes,
                    ga=2,
                    layers=2,
                )
                windows, extraction = extract_route_windows(
                    instance,
                    target=target_per_scenario,
                    depth=0,
                    max_tasks=30,
                    exact_limit=max_states,
                )
                prefix = f"{topology_name}_{placement_name}_{mode}"
                for index, window in enumerate(windows):
                    renamed = type(window.dag)(
                        f"{prefix}_window_{index}",
                        window.dag.category,
                        window.dag.tasks,
                        window.dag.description,
                        window.dag.parameters,
                    )
                    all_windows.append(MultiResourceInstance(renamed, window.resources))
                scenarios.append({
                    "topology": topology_name,
                    "placement": placement_name,
                    "mode": mode,
                    "probe": {
                        key: value for key, value in info.items()
                        if key not in {"effective_metrics", "flow_meta"}
                    },
                    "extraction": extraction,
                    "full_schedule": _full_schedule_metrics(instance),
                })
    window_report = evaluate_windows(
        all_windows, max_states=max_states, include_single_channel=False,
    )
    return {
        "config": {
            "modes": modes,
            "target_per_scenario": target_per_scenario,
            "quantum_us": quantum_us,
            "max_states": max_states,
        },
        "topologies": {
            name: {
                "nodes": topology.total_nodes,
                "directed_links": len(topology.links),
                "gpu_nodes": topology.gpu_nodes,
                "switch_nodes": topology.switch_nodes,
            }
            for name, factory in TOPOLOGIES.items()
            for topology in [factory()]
        },
        "placements": PLACEMENTS,
        "scenario_summary": {
            "count": len(scenarios),
            "mean_conflict_fraction": mean(
                item["extraction"]["ready_pair_conflict_fraction"]
                for item in scenarios
            ),
            "scenarios_with_disagreements": sum(
                item["extraction"]["policy_disagreement_ticks"] > 0
                for item in scenarios
            ),
            "scenarios_where_dynamic_not_best_observed": sum(
                item["full_schedule"]["methods"]["dynamic_tail"]["makespan"]
                > item["full_schedule"]["best_observed"]
                for item in scenarios
            ),
        },
        "scenarios": scenarios,
        "windows": window_report,
    }


def run_focused_rollout(*, modes: list[str], quantum_us: float) -> dict:
    """Expensive full-DAG rollout on the strongest observed conflict scenario."""

    rows = []
    for mode in modes:
        instance, _info, _graph = build_route_aware_probe(
            mode=mode,
            topology_path=Path("unused-when-topology-is-supplied"),
            topology=four_rack_core_topology(),
            quantum_us=quantum_us,
            assigned_nodes=PLACEMENTS["tp_cross"],
            ga=2,
            layers=2,
        )
        model = MultiResourceDAG(instance)
        methods = {
            policy: schedule_multiresource(instance, policy)
            for policy in ("dynamic_tail", "resource_tail", "bottleneck_first", "lpt")
        }
        methods["set_rollout_2"] = rollout_multiresource(instance, top_k=2)
        methods["set_rollout_4"] = rollout_multiresource(instance, top_k=4)
        rows.append({
            "mode": mode,
            "topology": "four_rack_core",
            "placement": "tp_cross",
            "tasks": len(instance.dag.tasks),
            "lower_bounds": multi_resource_lower_bounds(model),
            "methods": {
                name: {
                    "makespan": result.makespan,
                    "runtime_ms": result.runtime_ms,
                }
                for name, result in methods.items()
            },
        })
    return {"quantum_us": quantum_us, "rows": rows}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--modes", nargs="+", default=["1f1b", "bidirectional"])
    parser.add_argument("--target-per-scenario", type=int, default=3)
    parser.add_argument("--quantum-us", type=float, default=25.0)
    parser.add_argument("--max-states", type=int, default=100_000)
    parser.add_argument(
        "--focused-only", action="store_true",
        help="Only run expensive Rollout-2/4 on four_rack_core + tp_cross.",
    )
    parser.add_argument(
        "--output", type=Path,
        default=ROOT / "outputs" / "small_topology_sensitivity" / "report.json",
    )
    args = parser.parse_args()
    if args.focused_only:
        report = {"focused_rollout": run_focused_rollout(
            modes=args.modes, quantum_us=args.quantum_us,
        )}
    else:
        report = run_sensitivity(
            modes=args.modes,
            target_per_scenario=args.target_per_scenario,
            quantum_us=args.quantum_us,
            max_states=args.max_states,
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    if args.focused_only:
        print(json.dumps(report, indent=2))
    else:
        print(json.dumps({
            "scenario_summary": report["scenario_summary"],
            "windows": {
                key: value for key, value in report["windows"].items() if key != "rows"
            },
        }, indent=2))
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
