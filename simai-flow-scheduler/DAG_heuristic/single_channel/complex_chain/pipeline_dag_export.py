"""Export the workload DAG produced by each PP pipeline strategy.

This is a research/diagnostic tool for the DAG-scheduling heuristics line of
work (see ``DAG_heuristic/docs/260804组会.md``).  For every PP pipeline strategy implemented in
the simulator it materializes the task DAG — compute nodes (duration_us) and
flow nodes (src/dst/size_bytes) with dependency edges — and writes a compact
``dag.json``, a graphviz ``dag.dot``, a human-readable ``chain_view.txt``, and
a structural ``summary.json``.

By default it builds a **small synthetic AICB workload** with tp=1, dp=1 so
the DAG is pure PP (no TP/DP collectives), which is the shape the research is
interested in.  Pass ``--aicb`` to export from a real AICB file instead.

Usage:
    python -m DAG_heuristic.single_channel.complex_chain.pipeline_dag_export
    python -m DAG_heuristic.single_channel.complex_chain.pipeline_dag_export --modes 1f1b zero_bubble
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from src.workload_format.schema import (
    CommType,
    Job,
    ParallelismConfig,
    Phase,
    P2PWorkload,
    Task,
)
from src.workload_generator.aicb_parser import AicbHeader, AicbWorkItem, AicbParser
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
from src.workload_generator.rank_grouper import MegatronRankGrouper
from src.workload_generator.workload_builder import WorkloadBuilder

# ---------------------------------------------------------------------------
# Mode registry
# ---------------------------------------------------------------------------

MODES = ("1f1b", "interleaved_1f1b", "zero_bubble", "bidirectional", "dualpipe")

MODE_LABEL = {
    "1f1b": "1F1B / GPipe-style (base WorkloadBuilder DAG)",
    "interleaved_1f1b": "Interleaved 1F1B (VPP)",
    "zero_bubble": "Zero Bubble (ZB1P)",
    "bidirectional": "Bidirectional / basic Chimera",
    "dualpipe": "DeepSeek-style DualPipe",
}


def make_builder(mode: str, vpp: int, gradient_sync_bytes: int | None):
    """Return the strategy-specific workload builder for *mode*."""
    if mode == "1f1b":
        return WorkloadBuilder()
    if mode == "interleaved_1f1b":
        return InterleavedPipelineWorkloadBuilder(vpp)
    if mode == "zero_bubble":
        return ZeroBubblePipelineWorkloadBuilder()
    if mode == "bidirectional":
        return BidirectionalPipelineWorkloadBuilder(
            gradient_sync_bytes=gradient_sync_bytes,
        )
    if mode == "dualpipe":
        return DualPipePipelineWorkloadBuilder()
    raise ValueError(f"Unknown mode: {mode}")


# ---------------------------------------------------------------------------
# Synthetic AICB workload (pure PP, no TP/DP collectives)
# ---------------------------------------------------------------------------

def build_synthetic_aicb(
    pp: int,
    ga: int,
    layers_per_mb: int,
    pp_comm_size: int,
    grad_sync_bytes: int = 4096,
) -> tuple[AicbHeader, list[AicbWorkItem]]:
    """Build a tiny AICB workload with tp=1, dp=1, ep=1.

    Shape:
      - one ``grad_param_comm`` pre item (carries the per-stage gradient
        sync bytes used by the Chimera / DualPipe gradient sync);
      - ``ga * layers_per_mb`` layer items (NONE comm → pure PP DAG);
      - one ``optimizer1`` post item (NONE comm so DualPipe accepts it).

    Compute times are small on purpose so the exported DAG is easy to inspect.
    """
    tp = 1
    dp = 1
    ep = 1
    all_gpus = tp * dp * pp
    header = AicbHeader(
        tp=tp,
        ep=ep,
        pp=pp,
        vpp=layers_per_mb,
        ga=ga,
        all_gpus=all_gpus,
        pp_comm_size=pp_comm_size,
    )

    def item(
        name: str,
        fwd: int,
        bwd: int,
        dp_c: int = 0,
        dp_comm: str = "NONE",
        dp_size: int = 0,
    ) -> AicbWorkItem:
        return AicbWorkItem(
            name=name,
            forward_compute_time=fwd,
            forward_comm="NONE",
            forward_comm_size=0,
            backward_compute_time=bwd,
            backward_comm="NONE",
            backward_comm_size=0,
            dp_compute_time=dp_c,
            dp_comm=dp_comm,
            dp_comm_size=dp_size,
            process_time=100,
        )

    items: list[AicbWorkItem] = [
        item("grad_param_comm", 1000, 1000, 1000, "ALLREDUCE", grad_sync_bytes),
    ]
    # Compute times in ns (simulator converts to us by //1000).  Use
    # realistic magnitudes so the compute/communication ratio resembles a
    # real Transformer layer, not a degenerate zero-compute DAG.
    for _ in range(ga):
        for l in range(layers_per_mb):
            items.append(item(f"layer{l}", 1_000_000 + l * 200_000, 2_000_000 + l * 200_000))
    items.append(item("optimizer1", 0, 0))
    return header, items


# ---------------------------------------------------------------------------
# Job / stage helpers
# ---------------------------------------------------------------------------

def make_job(header: AicbHeader) -> Job:
    dp = header.all_gpus // (header.tp * header.pp)
    return Job(
        job_id=0,
        name="synthetic-pp",
        assigned_nodes=list(range(header.all_gpus)),
        parallelism=ParallelismConfig(
            tp=header.tp,
            dp=dp,
            pp=header.pp,
            ep=header.ep,
        ),
    )


def stage_by_node(job: Job) -> dict[int, int]:
    """Map each GPU rank to its physical PP stage (layout [PP][DP][TP])."""
    grouper = MegatronRankGrouper(job.assigned_nodes, job.parallelism)
    stage_width = grouper.dp * grouper.tp
    mapping: dict[int, int] = {}
    for stage in range(grouper.pp):
        start = stage * stage_width
        for node in grouper.nodes[start:start + stage_width]:
            mapping[node] = stage
    return mapping


def node_stage(task: Task, stage_map: dict[int, int]) -> int | None:
    """PP stage of a task's owning rank (compute node / flow src)."""
    rank = task.node if task.is_compute() else task.src
    return stage_map.get(rank) if rank is not None else None


# ---------------------------------------------------------------------------
# DAG analysis
# ---------------------------------------------------------------------------

def analyze_dag(
    workload: P2PWorkload,
    bandwidth_bytes_per_us: float,
) -> dict:
    """Compute depth (longest edge path) and weighted critical path.

    Weight model follows docs/260804组会.md: compute node weight = duration_us,
    flow node weight = size_bytes / C (single bottleneck channel).  The
    critical-path makespan is therefore a lower bound under the single-channel
    model, not the simulator's makespan (which uses real topology + policy).
    """
    tasks = {t.task_id: t for t in workload.tasks}
    deps: dict[int, list[int]] = {tid: list(t.deps) for tid, t in tasks.items()}
    children: dict[int, list[int]] = defaultdict(list)
    for tid, preds in deps.items():
        for p in preds:
            children[p].append(tid)

    def weight(t: Task) -> float:
        if t.is_compute():
            return float(t.duration_us or 0)
        return float(t.size_bytes or 0) / max(bandwidth_bytes_per_us, 1e-9)

    # Topological order via Kahn (deps are predecessors).
    in_deg = {tid: len(preds) for tid, preds in deps.items()}
    queue = [tid for tid, d in in_deg.items() if d == 0]
    order: list[int] = []
    while queue:
        u = queue.pop()
        order.append(u)
        for v in children[u]:
            in_deg[v] -= 1
            if in_deg[v] == 0:
                queue.append(v)
    if len(order) != len(tasks):
        raise ValueError(
            f"DAG has a cycle (topological order covers {len(order)}/{len(tasks)})"
        )

    depth: dict[int, int] = {tid: 0 for tid in tasks}
    cp_full: dict[int, float] = {}
    cp_compute: dict[int, float] = {}
    pred_full: dict[int, int | None] = {tid: None for tid in tasks}
    for u in order:
        w = weight(tasks[u])
        d = 0
        best_full = 0.0
        best_comp = 0.0
        best_pred = None
        for p in deps[u]:
            d = max(d, depth[p] + 1)
            if cp_full[p] > best_full:
                best_full = cp_full[p]
                best_pred = p
            best_comp = max(best_comp, cp_compute[p])
        depth[u] = d
        cp_full[u] = best_full + w
        cp_compute[u] = best_comp + (w if tasks[u].is_compute() else 0.0)
        pred_full[u] = best_pred

    end = max(cp_full, key=cp_full.get)
    crit: list[int] = []
    node = end
    while node is not None:
        crit.append(node)
        node = pred_full[node]
    crit.reverse()

    crit_bytes = sum(
        tasks[t].size_bytes or 0 for t in crit if tasks[t].is_flow()
    )
    crit_flows = sum(1 for t in crit if tasks[t].is_flow())
    crit_compute_us = sum(
        tasks[t].duration_us or 0 for t in crit if tasks[t].is_compute()
    )

    return {
        "depth": depth,
        "critical_path_full_us": cp_full[end],
        "critical_path_compute_us": cp_compute[end],
        "critical_path_nodes": crit,
        "critical_path_flow_count": crit_flows,
        "critical_path_flow_bytes": crit_bytes,
        "critical_path_compute_us": crit_compute_us,
    }


def compute_stats(
    workload: P2PWorkload,
    dag: dict,
    bandwidth_gbps: float,
    stage_map: dict[int, int],
) -> dict:
    tasks = workload.tasks

    flow_types = Counter(t.comm_type.value for t in tasks if t.is_flow())
    phases = Counter(t.phase.value for t in tasks if t.is_compute())

    in_deg = Counter()
    out_deg: dict[int, int] = defaultdict(int)
    for t in tasks:
        out_deg[t.task_id] = 0
    for t in tasks:
        for p in t.deps:
            in_deg[t.task_id] += 1
            out_deg[p] += 1

    pp_flows = [t for t in tasks if t.comm_type in (CommType.PP_SEND, CommType.PP_RECV)]
    fwd_pp = [t for t in pp_flows if t.phase is Phase.FORWARD]
    bwd_pp = [t for t in pp_flows if t.phase is not Phase.FORWARD]

    # Coflow-like grouping: flows sharing (iteration, direction) — the PP
    # traffic of one microbatch going one way.
    coflows: dict[tuple[int, str], list[Task]] = defaultdict(list)
    for t in pp_flows:
        coflows[(t.iteration, "fwd" if t.phase is Phase.FORWARD else "bwd")].append(t)

    stage_counts = Counter(
        node_stage(t, stage_map) for t in tasks
    )

    return {
        "bandwidth_gbps": bandwidth_gbps,
        "nodes": {"total": len(tasks),
                  "compute": sum(1 for t in tasks if t.is_compute()),
                  "flow": sum(1 for t in tasks if t.is_flow())},
        "edges": sum(len(t.deps) for t in tasks),
        "sources": sum(1 for t in tasks if not t.deps),
        "sinks": sum(1 for v in out_deg.values() if v == 0),
        "flow_types": dict(sorted(flow_types.items())),
        "compute_phases": dict(sorted(phases.items())),
        "pp_flows": {
            "total": len(pp_flows),
            "forward": len(fwd_pp),
            "backward": len(bwd_pp),
            "bytes": sum(t.size_bytes or 0 for t in pp_flows),
            "coflow_groups": len(coflows),
            "max_coflow_size": max((len(v) for v in coflows.values()), default=0),
        },
        "total_flow_bytes": sum(t.size_bytes or 0 for t in tasks if t.is_flow()),
        "total_compute_us": sum(t.duration_us or 0 for t in tasks if t.is_compute()),
        "degree": {
            "max_in": max(in_deg.values(), default=0),
            "max_out": max(out_deg.values(), default=0),
            "mean_in": sum(in_deg.values()) / max(len(tasks), 1),
        },
        "critical_path": {
            "makespan_us": dag["critical_path_full_us"],
            "compute_only_us": dag["critical_path_compute_us"],
            "flow_count": dag["critical_path_flow_count"],
            "flow_bytes": dag["critical_path_flow_bytes"],
            "compute_us": dag["critical_path_compute_us"],
        },
        "stages": {str(k): v for k, v in sorted(stage_counts.items())},
    }


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

def export_json(workload: P2PWorkload, dag: dict, stage_map: dict[int, int], path: Path) -> None:
    tasks = workload.tasks
    nodes = []
    for t in tasks:
        base = {
            "id": t.task_id,
            "type": t.type.value,
            "stage": node_stage(t, stage_map),
            "phase": t.phase.value,
            "iteration": t.iteration,
            "layer_id": t.layer_id,
            "depth": dag["depth"][t.task_id],
        }
        if t.is_compute():
            base.update({"rank": t.node, "duration_us": t.duration_us})
        else:
            base.update({
                "src": t.src,
                "dst": t.dst,
                "size_bytes": t.size_bytes,
                "comm_type": t.comm_type.value,
            })
        nodes.append(base)

    edges = [
        [pred, t.task_id]
        for t in tasks
        for pred in t.deps
    ]
    path.write_text(
        json.dumps({"nodes": nodes, "edges": edges}, indent=1),
        encoding="utf-8",
    )


def export_dot(workload: P2PWorkload, stage_map: dict[int, int], path: Path) -> None:
    """Graphviz DOT, left-to-right, one cluster per PP stage."""
    phase_color = {
        Phase.FORWARD: "#a7d8a0",
        Phase.BACKWARD_INPUT: "#a8c8f0",
        Phase.BACKWARD_WEIGHT: "#f5d79a",
        Phase.OPTIMIZER: "#e0e0e0",
    }
    lines = [
        "digraph dag {",
        '  rankdir=LR;',
        '  node [shape=box, style=filled, fontsize=10];',
        '  edge [fontsize=8];',
    ]

    stage_cluster: dict[int, list[str]] = defaultdict(list)
    for t in workload.tasks:
        st = node_stage(t, stage_map)
        sid = f"n{t.task_id}"
        if t.is_compute():
            color = phase_color.get(t.phase, "#f0f0f0")
            label = f"R{t.node}\\ni{t.iteration} L{t.layer_id}\\n{t.phase.value}\\n{t.duration_us}us"
        else:
            if t.comm_type in (CommType.PP_SEND, CommType.PP_RECV):
                color = "#f9a8a8" if t.phase is Phase.FORWARD else "#c9a8f0"
            else:
                color = "#d0d0d0"
            label = f"R{t.src}->R{t.dst}\\ni{t.iteration}\\n{t.size_bytes}B"
        lines.append(
            f'  {sid} [label="{label}", fillcolor="{color}"];'
        )
        if st is not None:
            stage_cluster[st].append(sid)

    for st, sids in sorted(stage_cluster.items()):
        lines.append(f'  subgraph cluster_stage{st} {{')
        lines.append(f'    label="stage {st}";')
        lines.append("    " + "; ".join(sids) + ";")
        lines.append("  }")

    # Edges from all tasks (deduplicated, forward deps)
    seen = set()
    for t in workload.tasks:
        for p in t.deps:
            key = (p, t.task_id)
            if key in seen:
                continue
            seen.add(key)
            lines.append(f"  n{p} -> n{t.task_id};")
    lines.append("}")
    path.write_text("\n".join(lines), encoding="utf-8")


def export_chain_view(
    workload: P2PWorkload,
    path: Path,
) -> None:
    """Textual per-microbatch view: rank compute chains with PP flows between."""
    tasks = workload.tasks
    flows = [t for t in tasks if t.is_flow()]

    # PP flow list grouped by iteration + direction.
    pp = sorted(
        [t for t in flows if t.comm_type in (CommType.PP_SEND, CommType.PP_RECV)],
        key=lambda t: (t.iteration, t.phase.value, t.src, t.dst),
    )
    lines = ["# PP flows (src rank -> dst rank), per microbatch / direction", ""]
    by_iter = defaultdict(list)
    for t in pp:
        by_iter[t.iteration].append(t)
    for it in sorted(by_iter):
        fwd = [t for t in by_iter[it] if t.phase is Phase.FORWARD]
        bwd = [t for t in by_iter[it] if t.phase is not Phase.FORWARD]
        lines.append(f"## microbatch {it}")
        if fwd:
            lines.append(f"  forward:  " + "  ".join(
                f"R{t.src}(L{t.layer_id})->R{t.dst}(L{t.layer_id}) {t.size_bytes}B" for t in fwd))
        if bwd:
            lines.append(f"  backward: " + "  ".join(
                f"R{t.src}(L{t.layer_id})->R{t.dst}(L{t.layer_id}) {t.size_bytes}B" for t in bwd))
        lines.append("")

    # Non-PP flows (TP/DP/EP collectives), grouped by type.
    others = [t for t in flows if t.comm_type not in (CommType.PP_SEND, CommType.PP_RECV)]
    if others:
        lines.append("# Other collective flows")
        for t in sorted(others, key=lambda t: (t.comm_type.value, t.iteration, t.src, t.dst)):
            lines.append(f"  {t.comm_type.value} R{t.src}->R{t.dst} i{t.iteration} {t.size_bytes}B")
        lines.append("")

    # Per-rank compute task census.
    lines.append("# Compute tasks per rank / microbatch / phase")
    comp = workload.get_compute_tasks()
    census: dict[tuple[int, int, str], int] = Counter(
        (t.node, t.iteration, t.phase.value) for t in comp
    )
    for key in sorted(census):
        lines.append(f"  R{key[0]} i{key[1]} {key[2]}: {census[key]}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def write_report(
    report_path: Path,
    results: list[dict],
    aicb_path: str,
    params: dict,
) -> None:
    """Append/refresh the 'DAG 导出' section of the heuristic progress doc."""
    report_path.parent.mkdir(parents=True, exist_ok=True)
    header = "## DAG 导出记录（不同 PP 流水线方式的 DAG 结构）"
    text = [header, ""]
    text.append(
        "- 生成：`python -m DAG_heuristic.single_channel.complex_chain.pipeline_dag_export`"
        "（默认合成小 workload；`--aicb <路径>` 可换成真实 AICB，"
        "`--pp/--ga/--layers/--pp-comm/--bandwidth-gbps` 可调参数，"
        "`--modes` 可选子集）"
    )
    text.append(f"- 日期：2026-08-06")
    text.append(f"- 来源 workload：`{aicb_path}`")
    text.append(
        f"- 并行度参数：tp={params['tp']}, dp={params['dp']}, pp={params['pp']}, "
        f"ga={params['ga']}, layers_per_mb={params['layers_per_mb']}, "
        f"pp_comm={params['pp_comm_size']} B, vpp={params['vpp']}"
    )
    text.append(
        f"- 带宽模型（关键路径）：单瓶颈 channel，容量 "
        f"{params['bandwidth_gbps']} Gbps（= {params['bandwidth_gbps'] * 1e9 / 8 / 1e6:.1f} B/us）"
    )
    text.append("")
    text.append("| mode | nodes | compute | flow | edges | pp_flows (f/b) | 关键路径 makespan (us) | 关键路径 compute-only (us) |")
    text.append("|---|---|---|---|---|---|---|---|")
    for r in results:
        s = r["stats"]
        cp = s["critical_path"]
        ppf = s["pp_flows"]
        text.append(
            f"| {r['mode']} | {s['nodes']['total']} | {s['nodes']['compute']} | "
            f"{s['nodes']['flow']} | {s['edges']} | {ppf['total']} "
            f"({ppf['forward']}/{ppf['backward']}) | "
            f"{cp['makespan_us']:.1f} | {cp['compute_only_us']:.1f} |"
        )
    text.append("")

    text.append("### 结构观察")
    for r in results:
        s = r["stats"]
        text.append(f"**{r['mode']}** — {r['label']}")
        text.append(f"- 节点 {s['nodes']['total']} = {s['nodes']['compute']} 计算 + {s['nodes']['flow']} 流；边 {s['edges']}；源点 {s['sources']} / 汇点 {s['sinks']}")
        text.append(f"- flow types: {s['flow_types']}")
        text.append(f"- PP 流 {s['pp_flows']['total']} 条（前向 {s['pp_flows']['forward']} / 后向 {s['pp_flows']['backward']}），"
                    f"总字节 {s['pp_flows']['bytes']}，coflow 组数 {s['pp_flows']['coflow_groups']}，最大组 {s['pp_flows']['max_coflow_size']} 条")
        text.append(f"- 关键路径：makespan(含流带宽) {s['critical_path']['makespan_us']:.1f} us，"
                    f"其中纯计算 {s['critical_path']['compute_only_us']:.1f} us，"
                    f"路径上流 {s['critical_path']['flow_count']} 条 / {s['critical_path']['flow_bytes']} B")
        text.append(f"- 每 stage 节点数：{s['stages']}")
        note = r.get("note")
        if note:
            text.append(f"- 注：{note}")
        text.append("")

    text.append("### 导出产物")
    for r in results:
        text.append(
            f"- `{r['out_dir']}` : " + " / ".join(r["artifacts"])
        )
    text.append("")

    text.append("### 观察记录（供 heuristic 研究）")
    text.append(
        "1. **所有 builder 都会在全部 rank 上物化每个 layer 的 compute 任务**："
        "DAG 的 stage 归属只体现在 PP 流的位置和跨 stage 依赖边上，"
        "而不是 compute 任务本身只出现在所属 stage 的 rank 上。"
        "因此导出 DAG 不是“真实”的 stage 分片 PP DAG，而是每个 rank 都走完整 "
        "forward/backward 链、由 PP 流串联的“全模型流水”模型。"
        "做人工 DAG 观察时需注意这一点；若需要 stage 分片语义，需在 builder 层按层归属裁剪。"
    )
    text.append(
        "2. PP 维度结构差异主要体现为 PP 流（coflow）的数量与接线位置："
        "1F1B 每 microbatch 每 stage 边界 1 条 fwd + 1 条 bwd；"
        "VPP 将其乘以 (vpp 逻辑层数)；Chimera/DualPipe 额外增加副本间的梯度同步 "
        "(dp_allreduce)。"
    )
    text.append(
        "3. Zero Bubble 的 DAG 与 1F1B 几乎一致（仅增加 W/DP → optimizer 的 gating），"
        "其收益主要在执行顺序（schedule），而不是依赖图拓扑。"
    )
    text.append("")

    content = report_path.read_text(encoding="utf-8") if report_path.exists() else ""
    # Replace an existing section with the same header, else append.
    if header in content:
        head, _, _tail = content.partition(header)
        _next_header = _tail.find("\n## ")
        if _next_header != -1:
            tail = _tail[_next_header:]
        else:
            tail = ""
        content = head + tail
    content = (content.rstrip() + "\n\n" if content.strip() else "") + "\n".join(text)
    report_path.write_text(content, encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export workload DAGs from each PP pipeline strategy.",
    )
    parser.add_argument(
        "--modes", nargs="+", choices=list(MODES), default=list(MODES),
        help="Which PP strategies to export (default: all).",
    )
    parser.add_argument(
        "--aicb", default=None,
        help="Real AICB file to export instead of the synthetic workload.",
    )
    parser.add_argument("--pp", type=int, default=2)
    parser.add_argument("--ga", type=int, default=4,
                        help="Gradient accumulation steps (= microbatches).")
    parser.add_argument("--layers", dest="layers_per_mb", type=int, default=4,
                        help="Layers per microbatch (synthetic workload only).")
    parser.add_argument("--pp-comm", dest="pp_comm_size", type=int, default=8_388_608,
                        help="PP activation size in bytes (synthetic).")
    parser.add_argument("--vpp", type=int, default=2,
                        help="Virtual pipeline size for interleaved_1f1b.")
    parser.add_argument("--grad-sync-bytes", type=int, default=None,
                        help="Per-stage gradient sync bytes for Chimera/DualPipe.")
    parser.add_argument("--bandwidth-gbps", type=float, default=200.0,
                        help="Bottleneck capacity used for flow weights.")
    parser.add_argument("--out", default="DAG_heuristic/outputs/dag_export")
    parser.add_argument("--report", default="DAG_heuristic/docs/heuristic进度.md")
    args = parser.parse_args()

    out_root = ROOT / args.out
    out_root.mkdir(parents=True, exist_ok=True)

    # Build or parse the workload inputs.
    if args.aicb:
        aicb_path = args.aicb
        header, items = AicbParser().parse(aicb_path)
        pp, ga, layers_per_mb = header.pp, header.ga, header.vpp
        pp_comm_size = header.pp_comm_size
        params = {
            "tp": header.tp, "dp": header.all_gpus // (header.tp * header.pp),
            "pp": header.pp, "ga": header.ga, "layers_per_mb": header.vpp,
            "pp_comm_size": header.pp_comm_size, "vpp": args.vpp,
            "bandwidth_gbps": args.bandwidth_gbps,
        }
    else:
        aicb_path = "<synthetic>"
        header, items = build_synthetic_aicb(
            pp=args.pp,
            ga=args.ga,
            layers_per_mb=args.layers_per_mb,
            pp_comm_size=args.pp_comm_size,
        )
        pp, ga, layers_per_mb = args.pp, args.ga, args.layers_per_mb
        pp_comm_size = args.pp_comm_size
        params = {
            "tp": header.tp, "dp": header.all_gpus // (header.tp * header.pp),
            "pp": header.pp, "ga": header.ga, "layers_per_mb": header.vpp,
            "pp_comm_size": header.pp_comm_size, "vpp": args.vpp,
            "bandwidth_gbps": args.bandwidth_gbps,
        }

    job = make_job(header)
    stage_map = stage_by_node(job)
    bandwidth_bytes_per_us = args.bandwidth_gbps * 1e9 / 8 / 1e6

    print(f"AICB: {aicb_path}")
    print(f"Header: tp={header.tp} dp={params['dp']} pp={pp} ga={ga} "
          f"layers/mb={layers_per_mb} pp_comm={pp_comm_size} all_gpus={header.all_gpus}")

    results = []
    for mode in args.modes:
        builder = make_builder(mode, args.vpp, args.grad_sync_bytes)
        try:
            workload = builder.build_from_aicb(header, items, job, comm_algo="ring")
        except Exception as exc:  # surface per-mode failures, continue others
            print(f"[{mode}] FAILED: {type(exc).__name__}: {exc}")
            continue
        # schema.validate() is O(V^2) for large DAGs (linear task_id scan per
        # node); rely on analyze_dag's topological sort for cycle detection and
        # only run the full validator on small DAGs.
        if len(workload.tasks) <= 5000:
            errors = workload.validate()
            if errors:
                print(f"[{mode}] validation failed: {errors}")
                continue
        else:
            print(f"[{mode}] {len(workload.tasks)} tasks: skipping O(V^2) validate()")

        dag = analyze_dag(workload, bandwidth_bytes_per_us)
        stats = compute_stats(workload, dag, args.bandwidth_gbps, stage_map)

        out_dir = out_root / mode
        out_dir.mkdir(parents=True, exist_ok=True)
        export_json(workload, dag, stage_map, out_dir / "dag.json")
        artifacts = ["dag.json", "summary.json"]
        # DOT / chain view are for manual observation; skip them on large DAGs.
        if len(workload.tasks) <= 2000:
            export_dot(workload, stage_map, out_dir / "dag.dot")
            export_chain_view(workload, out_dir / "chain_view.txt")
            artifacts += ["dag.dot", "chain_view.txt"]
        else:
            print(f"[{mode}] >2000 tasks: skipping dag.dot / chain_view.txt")
        (out_dir / "summary.json").write_text(
            json.dumps(stats, indent=2), encoding="utf-8",
        )

        note = None
        if mode == "zero_bubble":
            note = ("DAG 与 1f1b 的 PP 流完全相同；额外把各 W/DP 终端接到 "
                    "optimizer 形成 join（汇点数由 36 降到 4），"
                    "主要收益在执行顺序而非依赖拓扑。")
        if mode == "interleaved_1f1b":
            note = (f"PP 流 = 2·(pp·vpp−1)·ga = {stats['pp_flows']['total']}；"
                    "VPP 使每 microbatch 的 stage 边界数从 pp−1 增到 pp·vpp−1。")
        if mode == "bidirectional":
            note = ("额外 dp_allreduce 为镜像副本间的梯度同步（Chimera）；"
                    "前后半 microbatch 沿相反方向流过 stage。")
        if mode == "dualpipe":
            note = ("额外 dp_allreduce 为双 replica 的权重同步；"
                    "前后半 microbatch 分别在两个反向 replica 上处理。")
        results.append({
            "mode": mode,
            "label": MODE_LABEL[mode],
            "stats": stats,
            "out_dir": str(out_dir),
            "note": note,
            "artifacts": artifacts,
        })
        print(f"[{mode}] tasks={stats['nodes']['total']} "
              f"edges={stats['edges']} pp_flows={stats['pp_flows']['total']} "
              f"cp={stats['critical_path']['makespan_us']:.1f}us")

    report_path = ROOT / args.report
    if results:
        write_report(report_path, results, aicb_path, params)
        print(f"\nReport written to: {report_path}")
    else:
        print("\nNo mode exported successfully; report not updated.")


if __name__ == "__main__":
    main()
