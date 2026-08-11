## DAG 导出记录（不同 PP 流水线方式的 DAG 结构）

- 生成：`python scripts/export_pipeline_dags.py`（默认合成小 workload；`--aicb <路径>` 可换成真实 AICB，`--pp/--ga/--layers/--pp-comm/--bandwidth-gbps` 可调参数，`--modes` 可选子集）
- 日期：2026-08-06
- 来源 workload：`<synthetic>`
- 并行度参数：tp=1, dp=1, pp=2, ga=4, layers_per_mb=4, pp_comm=8388608 B, vpp=2
- 带宽模型（关键路径）：单瓶颈 channel，容量 200.0 Gbps（= 25000.0 B/us）

| mode | nodes | compute | flow | edges | pp_flows (f/b) | 关键路径 makespan (us) | 关键路径 compute-only (us) |
|---|---|---|---|---|---|---|---|
| 1f1b | 116 | 108 | 8 | 134 | 8 (4/4) | 29474.1 | 28803.0 |
| interleaved_1f1b | 132 | 108 | 24 | 158 | 24 (12/12) | 30816.3 | 28803.0 |
| zero_bubble | 116 | 108 | 8 | 166 | 8 (4/4) | 29474.1 | 28803.0 |
| bidirectional | 124 | 108 | 16 | 194 | 8 (4/4) | 29474.1 | 28803.0 |
| dualpipe | 124 | 108 | 16 | 194 | 8 (4/4) | 29474.1 | 28803.0 |

### 结构观察
**1f1b** — 1F1B / GPipe-style (base WorkloadBuilder DAG)
- 节点 116 = 108 计算 + 8 流；边 134；源点 2 / 汇点 36
- flow types: {'pp_send': 8}
- PP 流 8 条（前向 4 / 后向 4），总字节 67108864，coflow 组数 8，最大组 1 条
- 关键路径：makespan(含流带宽) 29474.1 us，其中纯计算 28803.0 us，路径上流 2 条 / 16777216 B
- 每 stage 节点数：{'0': 58, '1': 58}

**interleaved_1f1b** — Interleaved 1F1B (VPP)
- 节点 132 = 108 计算 + 24 流；边 158；源点 2 / 汇点 36
- flow types: {'pp_send': 24}
- PP 流 24 条（前向 12 / 后向 12），总字节 201326592，coflow 组数 8，最大组 3 条
- 关键路径：makespan(含流带宽) 30816.3 us，其中纯计算 28803.0 us，路径上流 6 条 / 50331648 B
- 每 stage 节点数：{'0': 66, '1': 66}
- 注：PP 流 = 2·(pp·vpp−1)·ga = 24；VPP 使每 microbatch 的 stage 边界数从 pp−1 增到 pp·vpp−1。

**zero_bubble** — Zero Bubble (ZB1P)
- 节点 116 = 108 计算 + 8 流；边 166；源点 2 / 汇点 4
- flow types: {'pp_send': 8}
- PP 流 8 条（前向 4 / 后向 4），总字节 67108864，coflow 组数 8，最大组 1 条
- 关键路径：makespan(含流带宽) 29474.1 us，其中纯计算 28803.0 us，路径上流 2 条 / 16777216 B
- 每 stage 节点数：{'0': 58, '1': 58}
- 注：DAG 与 1f1b 的 PP 流完全相同；额外把各 W/DP 终端接到 optimizer 形成 join（汇点数由 36 降到 4），主要收益在执行顺序而非依赖拓扑。

**bidirectional** — Bidirectional / basic Chimera
- 节点 124 = 108 计算 + 16 流；边 194；源点 2 / 汇点 4
- flow types: {'dp_allreduce': 8, 'pp_send': 8}
- PP 流 8 条（前向 4 / 后向 4），总字节 67108864，coflow 组数 8，最大组 1 条
- 关键路径：makespan(含流带宽) 29474.1 us，其中纯计算 28803.0 us，路径上流 2 条 / 16777216 B
- 每 stage 节点数：{'0': 62, '1': 62}
- 注：额外 dp_allreduce 为镜像副本间的梯度同步（Chimera）；前后半 microbatch 沿相反方向流过 stage。

**dualpipe** — DeepSeek-style DualPipe
- 节点 124 = 108 计算 + 16 流；边 194；源点 2 / 汇点 4
- flow types: {'dp_allreduce': 8, 'pp_send': 8}
- PP 流 8 条（前向 4 / 后向 4），总字节 67108864，coflow 组数 8，最大组 1 条
- 关键路径：makespan(含流带宽) 29474.1 us，其中纯计算 28803.0 us，路径上流 2 条 / 16777216 B
- 每 stage 节点数：{'0': 62, '1': 62}
- 注：额外 dp_allreduce 为双 replica 的权重同步；前后半 microbatch 分别在两个反向 replica 上处理。

### 导出产物
- `D:\Code\SimAI\simai-flow-scheduler\outputs\dag_export\1f1b` : dag.json / summary.json / dag.dot / chain_view.txt
- `D:\Code\SimAI\simai-flow-scheduler\outputs\dag_export\interleaved_1f1b` : dag.json / summary.json / dag.dot / chain_view.txt
- `D:\Code\SimAI\simai-flow-scheduler\outputs\dag_export\zero_bubble` : dag.json / summary.json / dag.dot / chain_view.txt
- `D:\Code\SimAI\simai-flow-scheduler\outputs\dag_export\bidirectional` : dag.json / summary.json / dag.dot / chain_view.txt
- `D:\Code\SimAI\simai-flow-scheduler\outputs\dag_export\dualpipe` : dag.json / summary.json / dag.dot / chain_view.txt

### 观察记录（供 heuristic 研究）
1. **更正：AICB 的 `vpp`/layer 行是每个 PP stage 的局部层模板，不是全局模型层表。** builder 在每个物理 stage 上复制相同的局部层编号是当前 homogeneous-stage 抽象的预期行为；全局模型可理解为 `pp × local_layers`。因此不能据此断言“每个 rank 执行完整全局模型”。真正的限制是目前不能直接表达各 stage 层数或 profile 不同的异构分片，且导出中的 `layer_id` 是 stage-local id。
2. PP 维度结构差异主要体现为 PP 流（coflow）的数量与接线位置：1F1B 每 microbatch 每 stage 边界 1 条 fwd + 1 条 bwd；VPP 将其乘以 (vpp 逻辑层数)；Chimera/DualPipe 额外增加副本间的梯度同步 (dp_allreduce)。
3. Zero Bubble 的 workload 数据图与 1F1B 接近（增加 W/DP → post-step gating），但 effective DAG 还必须计入不同的 `compute_order`。当前 W 仍依赖同层 B，因此它不是理论上 B/W 完全解耦的 ZB 图；已有收益主要来自 serializer 给出的执行顺序。

## 现有 heuristic 的表现与常数近似比

### 1. 结论先行

在本文当前采用的模型中：

- 所有通信共享一个容量为 $C$ 的瓶颈 channel；
- 通信允许抢占；
- GPU 上的 compute order 已经作为依赖边写入 DAG，计算任务 ready 后立即执行；
- 所有任务在时刻 0 已经包含在一个有限 DAG 中；

**任意 work-conserving 的通信 list scheduling 都是 2-approximation。**

这里 work-conserving 指：只要至少存在一个 ready communication，channel 就以总速率 $C$
传输某个或某些 ready communication，不人为留空。选择哪个 ready communication 不影响这个
2-近似保证。因此 FIFO、SPT、LPT、longest-delay、longest-tail、LRPT、TicTac 风格优先级，
只要执行层不会因为队首任务尚未 ready 而阻塞其它 ready flow，都自动具有同一个最坏情况上界。

这个结论回答了“是否可能有常数近似比”：**可以，而且不需要依赖某个特别复杂的 priority score；
priority score 的作用是改善平均表现和实际 workload 上的 gap，而不是建立最基础的常数保证。**

### 2. 2-approximation 定理

记：

$$
P=\sum_{i\in V_{comm}}\frac{b_i}{C}
$$

为全部通信独占瓶颈链路时的总处理时间；记：

$$
Q=\max_{\pi\text{ 是 DAG 中的一条路径}}
\sum_{v\in\pi\cap V_{comp}}p_v
$$

为任意依赖路径上的最大纯计算时间。显然对于最优 makespan $OPT$：

$$
OPT\ge P,\qquad OPT\ge Q.
$$

> **定理4：** 对任意 work-conserving 调度 $H$，其 makespan 满足
> $$
> T_H\le P+Q\le 2OPT.
> $$

证明思路：从调度中最后完成的节点开始，反向选择使当前节点最晚 ready 的直接前驱，得到一条
blocking path。路径上的计算任务 ready 后立即运行，其总执行时间不超过 $Q$。路径上的通信任务
从 ready 到完成之间，如果自身没有传输，由于调度是 work-conserving，此时 channel 必定正在传输
其它通信；这些等待区间沿 blocking path 在时间上互不重叠，可以全部计入总 network busy time
$P$。因此整个时间轴最多由 $P$ 的 network busy time 和 $Q$ 的因果计算时间构成，即
$T_H\le P+Q$。再结合两个 lower bound 得到：

$$
\frac{T_H}{OPT}\le
\frac{P+Q}{\max(P,Q)}\le 2.
$$

该证明与经典 List Scheduling 的“总工作量 + blocking chain”证明结构相同。Graham-style
List Scheduling 在更传统的 precedence-constrained machine scheduling 中同样给出 2-近似；
[相关综述和定理](https://link.springer.com/article/10.1007/s10951-021-00687-6)也使用从末任务反向追踪
blocking/minimal chain 的分析方法。本文的 $P+Q$ 证明则针对“一个通信资源 + 已固化计算链”的
特殊结构进行了简化。

这个上界还给出一个有用的 instance-dependent 估计：

$$
\rho_H\le\frac{P+Q}{\max(P,Q)}.
$$

当训练 workload 明显 compute-bound（$Q\gg P$）或 communication-bound（$P\gg Q$）时，
所有 work-conserving 策略的理论比值都会接近 1；最值得做复杂 heuristic 的区域反而是
$P\approx Q$，因为此时通信/计算 overlap 对 makespan 最敏感。

### 3. 上界为什么不能代替 priority heuristic

2 这个通用分析基本是 tight 的。考虑两条链，令 $\varepsilon\to0$：

```text
链 A: comm(ε) -> compute(L) -> comm(ε)
链 B: comm(L)
```

如果某个 work-conserving 策略先传 B，则：

$$
T_H=2L+2\varepsilon.
$$

如果先传 A 的第一个小 flow，再用 B 覆盖 A 的计算，则：

$$
OPT=L+2\varepsilon.
$$

比值趋近 2。这说明“链路始终忙”只能提供安全网，真正获得接近最优的结果仍然依赖于尽早启动
具有长计算尾部的通信。这也是 longest-tail 在本场景中最直接的直觉。

### 4. 现有 heuristic 与本问题的对应关系

| heuristic | 当前模型中的 priority | 直觉 | 理论判断 |
|---|---|---|---|
| FIFO / arbitrary list | ready 顺序或 task id | 最低实现成本 | work-conserving 时 2-approx，但存在趋近 2 的实例 |
| SPT | 最小剩余通信量优先 | 尽快完成更多 flow | 可快速释放短分支，但可能推迟长计算尾部；只有通用 2-bound |
| LPT | 最大剩余通信量优先 | 先处理网络大项 | 容易错过启动 compute overlap 的机会；只有通用 2-bound |
| Longest delay first | 下一段计算 lag 最大者优先 | 尽早启动长 compute | 只看一步，忽略更深的 DAG tail；只有通用 2-bound |
| Longest-tail first | $q(v)$ 最大者优先 | 尽早启动最长下游因果链 | 当前最推荐 baseline；通用 2-bound，尚无更小 worst-case 证明 |
| LRPT / critical-path first | $b_v^{rem}/C+q(v)$ 最大者优先 | 同时考虑当前 flow 和下游 tail | 容易被“大 flow 本身”吸引；通用 2-bound |
| TicTac TIC/TAC | 基于通信依赖、时间预测和两两 comparator 离线定优先级 | 减少通信阻塞计算关键链 | 原论文明确将 comparator 称为 approximate induction，并未给原问题近似保证；在本文简化模型中只要执行时 work-conserving，仍继承 2-bound |

[TicTac](https://proceedings.mlsys.org/paper_files/paper/2019/file/94cb28874a503f34b3c4a41bddcea2bd-Paper.pdf)
与本文目标最接近：它同样从 DAG 和 time oracle 生成通信 priority，目标是提高通信/计算 overlap；
其 priority 只在 ready queue 的候选任务间生效，因此不会破坏 DAG。不同之处是 TicTac 面向
parameter-server worker 上主要作为根节点的 recv，而本文还包含运行中动态释放的 PP/TP/DP flow，
所以不能直接照搬 TAC comparator，但可以把它作为 pairwise lookahead 的设计参考。

带 time lag 的 single-machine/coupled-task 文献同样表明，即使每个 job 只有两段操作，问题也常为
NP-hard；但其中很多结果使用 **exact lag**，本文 compute 产生的是 **minimum lag**，因此这些文献中的
特殊近似比不能直接移植。[Single machine scheduling subject to precedence delays](https://doi.org/10.1016/0166-218X(96)00110-2)
同时覆盖了 minimum precedence delay 与抢占/非抢占复杂性；
[coupled-task survey](https://www.numdam.org/article/RO_2012__46_4_335_0.pdf)给出了 exact-gap、minimum-gap、
chain precedence 等变体的边界。

### 5. 小规模精确对比

新增研究脚本：

```bash
python scripts/evaluate_chain_heuristics.py --samples 2000 --seed 260804
```

脚本对离散时间的多平行链实例使用动态规划求精确最优解，并比较六个 work-conserving heuristic。
实验配置为 2000 个固定随机种子实例、2--4 条链、每条链 1--3 个通信任务、通信量 1--3、计算
lag 0--4；通信可以在整数时间点抢占。结果如下：

| policy | mean ratio | p95 ratio | observed max |
|---|---:|---:|---:|
| FIFO | 1.1653 | 1.4286 | 1.7500 |
| SPT | 1.1297 | 1.3529 | 1.6667 |
| LPT | 1.1739 | 1.4000 | 1.7500 |
| Longest delay | 1.0795 | 1.2727 | 1.5000 |
| Longest-tail | **1.0019** | **1.0000** | **1.1667** |
| LRPT | 1.0039 | 1.0476 | 1.2308 |

这些数字不是近似比证明，只描述该实例分布；但它们支持三个判断：

1. 仅看 flow size 的 SPT/LPT 明显不够，调度的主要信号来自下游计算结构；
2. 只看下一段 compute lag 已经有改善，但不如完整 DAG tail；
3. 在这组实例上，**不包含当前 flow 大小的 longest-tail 比 LRPT 更稳**，说明当前 flow 大小已经通过
   占用 channel 体现，再把它加入 priority 可能产生重复计权。

原 motivating example 的 exact optimum 为 8；FIFO、LPT、longest-delay、longest-tail 和 LRPT
均得到 9，SPT 得到 8。这也再次说明 longest-tail 虽然在随机总体上很好，却不是逐实例最优规则。

### 6. 面向 SimAI training DAG 的推荐路线

第一版 policy 建议采用 **Dynamic Longest-Tail List Scheduling**：

1. 对 residual DAG 反向计算每个 ready flow 的
   $$
   q(v)=\max_{\pi:v\leadsto sink}\sum_{u\in\pi,u\ne v}d(u),
   $$
   其中通信 duration 使用剩余字节数除以 $C$，计算 duration 使用 profile；
2. channel 空闲或有新 flow ready 时，选择 $q(v)$ 最大者；
3. 新任务释放、当前 flow 完成，或 residual DAG/profile 明显变化时重新计算；
4. tie-break 依次考虑：能立即解锁的计算量、剩余 flow 较小者、稳定 task id；
5. 始终只在 ready set 中选择并保持 work-conserving，从而保留 2-approximation 安全网。

随后可以加入一个不会破坏保证的 **top-$k$ one-step rollout**：只对 $q(v)$ 最高的少量候选分别模拟到
下一个 release/completion event，用新的 lower bound

$$
LB_{rem}=\max(P_{rem},Q_{rem},L_{rem})
$$

其中 $L_{rem}$ 是 residual DAG 在忽略资源争用时的 weighted critical-path lower bound。
评估选择后果，再执行预测 makespan 最小的候选。只要 rollout 最终仍选择某个 ready flow 且不主动
idle，最坏情况 2-bound 保持不变；它主要用于修复 motivating example 这类 longest-tail 的局部反例。

评测时应分三层：

1. 小 DAG：动态规划/枚举最优解，报告 ratio distribution 与 worst discovered instance；
2. 中 DAG：用 CP-SAT/MILP 给 lower bound 和限时 incumbent，报告 optimality gap；
3. 真实 1F1B/ZB/DualPipe DAG：报告 makespan、network idle、compute idle、overlap coefficient，以及
   $T_H/\max(P,Q,L)$。这里必须导出包含 `compute_order` resource edges 的 effective DAG，而不仅是
   workload data-dependency DAG。

### 7. 结论的适用边界

上述 2-approximation **只保证本文当前的单瓶颈抽象**。以下扩展不能直接沿用证明：

- 多链路拓扑、不同 flow 占用不同且可能重叠的路径；
- collective 同时占用多个网络资源；
- GPU compute order 未固化、通信调度会反过来决定多个 GPU queue 的顺序；
- 为公平性、deadline 或多租户隔离而主动让瓶颈链路空闲；
- 在线到达且未来 job 不在当前 DAG 中。

在这些场景里，等待时间无法再全部 charge 到同一个 $P$，因此 2-bound 可能失效。合理路线是先把
单瓶颈版本作为具有理论保证的 baseline，再研究按 bottleneck decomposition、每链路 shadow price
或 primal-dual score 扩展到真实拓扑，而不要提前声称仍有常数近似比。

## 阶段 0：DAG 语义审计与特征提取（2026-08-06）

### 0. 阶段结论

阶段 0 已完成，退出条件已满足。新增 `scripts/audit_pipeline_dags.py`，它导出并检查：

```text
effective DAG
  = workload 中的显式依赖边
  ∪ 每个设备 compute_order 中相邻计算任务的串行边
```

collective completion/join 和显式 optimizer gating 已经包含在 workload 依赖中，不需要再凭名称补边。脚本对 effective DAG 做无环检查、结构指标提取，并用 `AnalyticalExecutor` 的逐任务时间线检查每条 effective edge 的 `end(source) <= start(target)`。

主要判断如下：

1. **后续 heuristic 必须基于 effective DAG，不能基于 workload DAG。** 在默认混合探针中，五种模式都新增了 392 条原数据图没有的设备串行边；忽略它们会制造 GPU 上实际不存在的并行性。
2. **图具有强模板重复性和小 frontier，但不是可直接独立求解的小块之和。** 混合图的任务模板重复率为 100%，拓扑 ready frontier 仅 10--18；与此同时 join 很密集，且没有跨全图的 mandatory dominator。适合做压缩 frontier-state、周期策略或短窗口 rollout，不支持按 microbatch/维度独立切开后直接拼接。
3. **当前 ZB 不是 B/W 解耦图。** 所有 model W 的直接前驱仍是同层 B compute 或 B 的 TP collective flow；ZB 主要通过 `compute_order` 把 W 延迟填入气泡，并额外把 model W gate 到 post-step。
4. **optimizer 与 activation memory 语义不完整。** 普通 AICB 的 `optimizer1` 被编码成 `iteration=ga, phase=forward` 的 post-step compute，而不是 `Phase.OPTIMIZER`。普通 1F1B/Interleaved 的 DP flow 不是 post-step 的祖先；Task IR 也没有 activation 大小、生命周期或显存依赖。因此当前结果适用于固定单迭代、忽略 activation 容量的调度，不能直接外推到跨迭代 optimizer overlap 或显存受限调度。

### 1. 审计 workload 与口径

使用两组确定性合成 workload：

- **混合并行探针**：`pp=2, tp=2, dp=2, ga=4, local_layers=4, vpp=2`，共 8 ranks；每层有 F/B/W compute 与 F/B 的 TP all-reduce，另含规范的 `grad_param_comm` DP bucket 和 PP activation/gradient。
- **纯 PP 小图**：`pp=2, tp=1, dp=1, ga=4, local_layers=4, vpp=2`；用于在 116--132 个节点上精确计算 maximum antichain width。

通信 duration 在结构分析中统一按 200 Gbps 单瓶颈折算。这一 critical-path 数值是“忽略通信资源争用”的 DAG lower bound，不是 simulator makespan，也不用于宣称某种 PP 策略实际更快。混合探针的生成命令为：

```bash
python scripts/audit_pipeline_dags.py
python scripts/audit_pipeline_dags.py --tp 1 --dp 1 --ga 4 --layers 4 --out outputs/dag_audit_pp
```

### 2. DAG 语义审计

#### 2.1 PP stage 与 local layer

- rank 到 `(dp replica, pp stage, tp lane)` 的映射由 `MegatronRankGrouper`/job parallelism 决定。
- AICB 的 layer 行在这里表示**每 stage 的局部层模板**；每个物理 stage 都拥有 layer 0--3 并不表示每个 stage 执行完整全局模型。
- 当前抽象只支持 homogeneous stages。若真实切分的各 stage 层数、计算时间或通信量不同，需要扩展输入/构图语义；不能仅凭相同 `layer_id` 把跨 stage 的任务视为同一模型层。

#### 2.2 B/W、PP、TP、DP

- F 沿本 stage 的局部层正向串联；B 沿局部层反向串联。
- PP forward flow 从前一 stage 的末层 F/TP completion 到后一 stage 的首层 F；PP backward flow方向相反，从后一 stage 的首层 B/TP completion 到前一 stage 的末层 B。
- TP collective 被展开为多条 ring flow；下游 compute 依赖该 collective 的全部终端 flow，因而 join completion 已显式进入 workload DAG。
- DP bucket 同样是 flow 子图。DualPipe 拒绝 layer 行中含义不明确的 `dp_comm`，只接受 `grad_param_comm`/`grad_gather`；审计探针因此把 DP 同步放在规范 gradient bucket 上。
- 当前基础 builder 中，W 依赖同层 B（有 TP 时通常依赖 B collective 的终端 flow），所以 B/W 并未并行分叉。ZeroBubble builder 没有移除此边。

#### 2.3 compute order

`ExecutionPlan.compute_order` 在 executor 中由 per-node cursor 强制执行，serializer 的 `validate()` 也按相邻任务补边做环检测。因此 effective exporter 只需加入每个 rank 上相邻 compute 的边；无需构造传递闭包。默认混合图中每种策略都有 424 条相邻 compute-order 边，其中 392 条不在 workload 数据依赖里。

#### 2.4 optimizer barrier

所有模式的 `Phase.OPTIMIZER` 节点数均为 0；这里审计的是 `iteration=ga` 的 post-step forward entry：

| mode | model W 成为 post-step 祖先 | DP flow 成为 post-step 祖先 | 结论 |
|---|---:|---:|---|
| 1F1B | 128/128 | 0/16 | compute order 使 W 在前，但不等待异步 DP 完成 |
| Interleaved | 128/128 | 0/16 | 同上 |
| ZeroBubble | 128/128 | 0/16 | 显式 gate 全部 model W；规范 DP bucket 未 gate |
| Bidirectional | 128/128 | 96/112 | 等待其新增的 replica gradient sync；仍有 16 条规范 bucket flow 不在 barrier 中 |
| DualPipe | 128/128 | 96/96 | 新增的 replica gradient sync 全部进入 barrier |

这说明后续若研究 optimizer deadline，必须先把“哪些 DP collective 属于本迭代 optimizer barrier”做成明确 IR 语义，不能靠 `optimizer1` 名称或所有 DP flow 的集合猜测。

#### 2.5 activation memory

`Task` 没有 activation bytes、张量身份、产生/释放时间或 memory dependency。serializer 的 inflight 限制只是顺序规则，不等价于按字节建模显存。因此当前 frontier-state 中的 `M_activation` 只能暂时省略；若比较会改变 activation 驻留峰值的 schedule，必须补模型后再下结论。

### 3. 结构特征统计

#### 3.1 混合 PP×TP×DP effective DAG

| mode | nodes | data edges | effective edges | CP lower bound (us) | ready frontier | ASAP active max | max join indegree |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1F1B | 992 | 1776 | 2168 | 67909.6 | 10 | 12 | 9 |
| Interleaved | 1056 | 1904 | 2296 | 61517.7 | 14 | 12 | 9 |
| ZeroBubble | 992 | 1904 | 2296 | 65662.8 | 10 | 12 | 25 |
| Bidirectional | 1088 | 2272 | 2664 | 65786.7 | 18 | 16 | 21 |
| DualPipe | 1072 | 2248 | 2640 | 54756.0 | 18 | 16 | 21 |

大图的 exact antichain 计算默认跳过（超过 600 节点），以免审计脚本在大实例上失控；`ready frontier` 和 `ASAP active` 是另外两个可扩展的并行度指标，不能当作 exact width。

#### 3.2 纯 PP 小图的精确宽度

| mode | nodes | effective edges | exact width | ready frontier | SESE regions |
|---|---:|---:|---:|---:|---:|
| 1F1B | 116 | 172 | 3 | 3 | 24 |
| Interleaved | 132 | 204 | 6 | 4 | 16 |
| ZeroBubble | 116 | 215 | 4 | 3 | 0 |
| Bidirectional | 124 | 235 | 6 | 5 | 0 |
| DualPipe | 124 | 238 | 6 | 5 | 0 |

SESE 使用保守的 dominator/post-dominator 条件。混合图有多源、多汇和大量 collective fan-out/fan-in，未找到非平凡全局 SESE；纯 PP 中只在 1F1B/Interleaved 找到很小的 3-node region。这进一步否定了“大量独立区域精确求解后直接组合”作为主路线。

#### 3.3 flow slack（混合图）

表中为 `count / zero-slack / p50 / max`，时间单位 us：

| mode | PP | TP | DP |
|---|---|---|---|
| 1F1B | 32 / 8 / 516.1 / 9404.9 | 512 / 320 / 0.0 / 9404.9 | 16 / 8 / 8825.8 / 8825.8 |
| Interleaved | 96 / 16 / 4558.1 / 10425.8 | 512 / 272 / 0.0 / 10425.8 | 16 / 8 / 4333.9 / 4333.9 |
| ZeroBubble | 32 / 8 / 2574.2 / 6683.9 | 512 / 264 / 0.0 / 10604.9 | 16 / 8 / 6725.8 / 6725.8 |
| Bidirectional | 32 / 12 / 558.1 / 20267.8 | 512 / 280 / 0.0 / 20267.8 | 112 / 24 / 9425.8 / 11546.8 |
| DualPipe | 32 / 8 / 4821.0 / 9321.0 | 512 / 448 / 0.0 / 9362.9 | 96 / 48 / 9362.9 / 9362.9 |

这里的 slack 来自无资源争用的 weighted effective DAG。它适合做 priority feature 和候选筛选，但网络排队发生后必须在 residual DAG 上动态重算。TP 中大量零 slack flow 说明“只区分 PP/DP，忽略 TP join”会漏掉关键阻塞；DP 则普遍具有较大可延迟空间，支持把它当作 deadline side work，但必须保留 optimizer barrier。

#### 3.4 join、重复与周期

- 混合图的 flow 成为某个 join 的 ASAP 最后阻塞者比例为 96%--98%。该数值偏高是因为 ring collective 的每个中间/终端 hop 都形成局部 join，不能直接解释成“96% flow 都应该最高优先级”。实用的 gate score 应聚合到 collective/coflow，并只看面向关键 compute 的最终 completion join。
- 去掉 microbatch 后，混合图只有 76--114 种局部模板，所有任务都属于重复模板，最大重复次数 16。模板/周期压缩具有实际价值。
- `compute_order` 可稳定分成 warmup、steady、cooldown；脚本输出每 rank 的 `F/B/W + microbatch` token 序列和 steady label period。不同策略的 steady 区间不一定长，不能把一次小 `ga=4` 的检测周期当成通用定理，后续应扩大 GA 后验证周期稳定性。
- critical backbone 的 `(stage, microbatch, phase)` 切换次数分别为：1F1B `(2,5,43)`、Interleaved `(4,11,41)`、ZB `(2,11,13)`、Bidirectional `(6,8,39)`、DualPipe `(4,8,27)`。关键链会跨 microbatch/phase 切换，因此永久 chain decomposition 不可靠；动态 tail 更符合图结构。

### 4. Timeline 逐任务验证

对默认混合 1F1B 的 992 个任务使用现有 16-GPU 200 Gbps topology 运行 `AnalyticalExecutor`：

- expected/completed：`992/992`；
- simulator makespan：`66911 us`；
- 检查的 effective edges：2168；
- `source.end > target.start` 的违例：0。

这验证了 workload dependency 与相邻 compute-order edge 都被 executor 实际遵守。结构分析的 200 Gbps 单瓶颈 duration 与真实 topology 的路由/链路计时口径不同，因此这里只验证逐边先后关系，不要求静态 CP 数值等于 simulator makespan。

### 5. 对后续 restricted problem 的选择依据

阶段 0 支持把下一阶段的主问题收敛为：

> 在 compute order 已固化、activation memory 暂不约束、collective 以 completion join 聚合的 residual effective DAG 上，为共享瓶颈的 ready flow/coflow 做 work-conserving 动态优先级调度。

具体设计建议：

1. 以 **Dynamic Longest-Tail** 为 baseline，但 tail 必须穿过 TP/PP/DP join，并在事件后重算。
2. gate-aware 特征从“裸 flow 是否最后阻塞”改为 **collective completion 是否解锁关键 compute**，避免 ring hop 重复计权。
3. 利用 `(stage, phase, local_layer, microbatch offset, virtual chunk)` 模板和小 frontier 做 top-k rollout/beam；不要先静态切成独立小 DAG。
4. 将 W/DP 视为带 post-step deadline 的 side work，而不是删除其与 backbone 的连接；当前 optimizer barrier 缺口补齐前，只在 DualPipe 或显式构造 barrier 的 motif 上评价 deadline 策略。
5. 理论上的单瓶颈并行链可作为局部决策与 2-approx 安全底座；真实 topology 的多资源 flow/collective 需要另做 bottleneck decomposition，阶段 0 的统计不能证明其仍有常数近似比。

### 6. 产物与复现

- 审计/导出器：`scripts/audit_pipeline_dags.py`
- 自动化回归：`tests/test_audit_pipeline_dags.py`
- 混合图报告：`outputs/dag_audit/audit_report.json`
- 每种模式：`outputs/dag_audit/<mode>/effective_dag.json` 与 `audit_summary.json`
- 逐任务时间线：`outputs/dag_audit/1f1b/timeline_validation.json`
- 纯 PP 精确宽度报告：`outputs/dag_audit_pp/audit_report.json`

验证命令与结果：

```text
python -m pytest tests/test_audit_pipeline_dags.py -q
3 passed
```

全量 `python -m pytest -q` 的结果为 `794 passed, 3 skipped, 18 errors`；18 个 error 均发生在既有 Spectrum-X topology fixture 的 setup 阶段，原因是工作区缺少仓库外部文件 `astra-sim-alibabacloud/inputs/topo/Spectrum-X_8g_8gps_400Gbps_H100`，不是本次 DAG 审计产生的测试失败。
