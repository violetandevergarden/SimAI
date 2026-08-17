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

## 阶段 1：Benchmark 与精确 Oracle（2026-08-11）

### 0. 阶段结论

阶段 1 的四项产出已经完成：

- 可复现 benchmark generator；
- 两个相互校验的 exact oracle；
- 固定随机种子的 worst-instance 搜索器；
- 统一 lower bound 和 heuristic 评价接口。

实现位于 `scripts/benchmark_dag_oracle.py`，测试位于 `tests/test_benchmark_dag_oracle.py`。它不是 topology-aware simulator，而是专门服务于理论和小实例验证的单瓶颈模型。

本阶段最重要的实验判断是：

1. **评价系统已经能识别错误 heuristic。** 例如 LPT 在“大 flow 与长 compute tail”反例上的比值达到 1.7；不能再凭少数正常 LLM 样例判断策略好坏。
2. **Longest-tail 是当前最可靠的 baseline，但不是 exact rule。** 在 200 个混合随机实例中 mean ratio 为 1.0035、observed max 为 1.1176；已有确定性反例为 9/8。
3. **简单把 immediate unlock 加进分数并不一定更好。** 当前简单 gate-aware score 的 mean ratio 为 1.0144、observed max 为 1.2，说明 gate 必须判断“是否成为 join 的最后阻塞者”，不能只累计直接后继 compute。
4. **统一 lower bound 在小 motif 上很强，但不能代替 oracle。** 14 个合成图上 combined LB 等于 OPT；真实缩减窗口中 LB=144、OPT=145，仍存在非零 gap。
5. **许多基础 LLM motif 对通信顺序并不敏感。** 单独的 PP wave、optimizer DP burst 等图上所有 work-conserving 策略都达到最优。这不是 heuristic 已经解决问题，而是说明有区分力的 benchmark 必须组合多个 motif，让 backbone、join 和 deferred work 真正竞争。

### 1. 精确模型与语义

小 DAG 使用以下离散模型：

- 所有 `comm` 共享一个容量为 1 的瓶颈 channel；
- communication 可以在整数时间边界抢占；
- 每个时刻只传输一个 ready communication 的一个单位；
- `compute` ready 后立即开始，所有互不依赖的 compute 可以并行推进；
- effective DAG 中已经包含固定的 compute-order edge；
- 所有 duration 使用正整数时间量子，允许零时长边界 compute。

精确状态记录每个节点是 `pending`、`completed`，还是剩余多少执行时间。每个时间步执行：

1. 闭包启动所有 newly-ready compute；
2. 枚举一个 ready communication，若不存在则 channel idle；
3. 所有 active compute 与所选 communication 同时推进一个时间单位；
4. 重复到所有节点完成。

因此 oracle 枚举的是实际需要 heuristic 决定的 ready-flow 序列，而不是枚举 compute 排列。

### 2. Benchmark generator

#### 2.1 七类对抗图

| 名称 | 攻击目标 | 节点/通信 | OPT | combined LB |
|---|---|---:|---:|---:|
| `large_flow_vs_long_tail` | 大 flow 与长 compute tail | 6/3 | 10 | 10 |
| `longest_tail_counterexample` | static longest-tail 反例 | 8/4 | 8 | 8 |
| `lrpt_double_count` | LRPT 对当前 flow size 重复计权 | 10/5 | 10 | 10 |
| `join_false_critical` | join 假关键分支 | 5/2 | 10 | 10 |
| `fork_multi_unlock` | 小 flow 解锁多个 compute 分支 | 6/2 | 8 | 8 |
| `deferred_w_competition` | W/DP 与 PP backbone 竞争 | 9/4 | 9 | 9 |
| `optimizer_dp_burst` | optimizer 前集中 DP | 9/4 | 11 | 11 |

这些图是确定性的，每个图单独输出完整节点、依赖、最优决策序列、lower bound 和各 policy 结果。

#### 2.2 七类参数化 LLM motif

| 名称 | 表示内容 | 默认节点/通信 | OPT |
|---|---|---:|---:|
| `pp_forward_wave` | 多 stage、多 microbatch forward wave | 10/4 | 9 |
| `pp_backward_wave` | 反向 PP wave | 10/4 | 9 |
| `one_f_one_b` | 小型 1F1B 与固定 stage compute order | 24/6 | 23 |
| `zb_bw_fork` | 理想化的 B/W 共同依赖 grad-output、相互独立 | 13/3 | 13 |
| `w_dp_optimizer_join` | W→DP→optimizer 汇聚 | 7/3 | 9 |
| `tp_collective_plus_pp` | TP fan-out/fan-in 后接 PP | 7/4 | 11 |
| `warmup_steady_cooldown` | 单 rank 的 warmup/steady/cooldown 顺序 | 16/4 | 24 |

这里的 ZB motif 有意使用真实 B/W 分叉语义，而不是复刻当前 builder 中的 `B→W`，方便分别评价“当前实现”与“理论 ZB”。

#### 2.3 真实 effective DAG 缩减

缩减器读取阶段 0 的 `effective_dag.json`，执行：

1. 按 ASAP earliest-start 时间量子寻找 flow 最密集的冲突窗口；
2. 选择低 slack seed flows；
3. 保留直接前驱、直接后继以及 retained join 的所有 companion inputs；
4. 将窗口外前驱折算为并行 boundary-release compute；
5. 将窗口外下游折算为 boundary-tail compute；
6. 只在“左 compute 唯一后继是右 compute，且右 compute 唯一前驱是左 compute”时合并串行 compute，绝不跨越 flow、fork 或 join。

从默认 1F1B 混合 effective DAG 得到：

```text
原图：992 nodes
缩减图：54 nodes，其中 22 communications
安全合并：4 compute nodes
DP explored states：694
B&B explored states：111
combined LB：144
exact OPT：145
```

六种当前 policy 在这个窗口上都得到 145。它可以验证 oracle 和缩减边界，但不是一个能区分 heuristic 的真实窗口。阶段 2 应继续扫描多个 conflict windows，并优先保留至少存在两种 heuristic 结果差异的窗口。

### 3. Exact oracle

实现了两个相互独立的求解器：

#### 3.1 Memoized discrete-time DP

- 对每个 canonical residual state 枚举所有 ready communication；
- 使用缓存消除到达同一 residual state 的重复决策前缀；
- 返回 exact makespan、一个最优决策序列和 explored-state 数量；
- `max_states` 防止误把中型 DAG 送入指数级精确搜索。

#### 3.2 Branch-and-bound

- 初始 incumbent 取六个 heuristic 中的最好结果；
- DFS 枚举 ready communication；
- 使用 residual communication work 与 active compute 的 lower bound 剪枝；
- 对同一 residual state 保留最早到达时间；
- 与 DP 逐 benchmark 比较 exact makespan，不一致立即报错。

15 个正式 benchmark 上两个 oracle 全部一致。项目目前没有 OR-Tools/PuLP 依赖，因此没有为了阶段 1 强行引入 CP-SAT/MILP。对于当前几十节点的小图，DP 与 B&B 已经提供独立 exactness check；将来扩展到中型图时再增加可选 CP-SAT backend，而不是把它变成运行 benchmark 的强依赖。

### 4. 统一 lower bound

实现：

$$
LB=\max(P,Q,L,LB_{window},LB_{cut}).
$$

- `P`：全部未争用 communication work；
- `Q`：任意 precedence path 上的最大纯 compute work；
- `L`：忽略 channel contention 的 weighted critical path；
- `LB_window`：由 precedence-only earliest release 和 downstream tail 推导每个 flow 的必要执行窗口，然后检查所有 release/deadline 区间的 preemptive demand；取第一个满足所有必要条件的 horizon；
- `LB_cut`：带相同显式 cut label 的 communication 总工作量最大值。

在当前单 channel 模型中，`LB_cut <= P`，因此它数值上是冗余的；保留这一项是为了将同一 benchmark 提升到多链路/多瓶颈模型后按 cut 或 resource 分组。不能把单 channel 下 cut bound 很强的假象外推到真实 topology。

所有正式 benchmark 都自动检查 `combined LB <= exact OPT`。14 个合成图的 LB gap 为 0，真实缩减图的 gap 为 1。

### 5. 统一评价接口

`evaluate(dag)` 对任意 `BenchmarkDAG` 统一输出：

- 节点数、通信数、参数与类别；
- DP/B&B explored states；
- exact optimum 与 optimal ready-flow decisions；
- 五个 lower bound 和 combined LB；
- FIFO、SPT、LPT、Longest-tail、LRPT、Gate-aware 的 makespan 与 `makespan/OPT`。

benchmark 使用稳定 JSON schema 导出，并提供 `dag_to_json()`/`dag_from_json()` round trip，便于保存反例和后续回归。

### 6. Worst-instance 搜索结果

运行：

```bash
python scripts/benchmark_dag_oracle.py --search-samples 200 --seed 260811
```

实例组成：100 个随机 parallel-chain DAG，加 100 个随机 fork/join layered DAG；全部 200 个完成精确求解，0 个触发 300000-state 上限。

| policy | mean ratio | observed max | worst H/OPT |
|---|---:|---:|---:|
| FIFO | 1.1626 | 1.6875 | 27/16 |
| SPT | 1.1428 | 1.5455 | 17/11 |
| LPT | 1.1840 | 1.6875 | 27/16 |
| Longest-tail | **1.0035** | **1.1176** | 19/17 |
| LRPT | **1.0035** | **1.1176** | 19/17 |
| 简单 Gate-aware | 1.0144 | 1.2000 | 12/10 |

这些只是固定分布上的 observed ratio，不是近似比证明。尤其不能因为 200 个样本中 longest-tail 很好就断言它优于某个常数；阶段 2 仍需系统扩大参数范围并进行反例族构造。

确定性 benchmark 中具有区分力的结果包括：

- `large_flow_vs_long_tail`：LPT=17/10，其余主要策略达到 10；
- `longest_tail_counterexample`：SPT=8，Longest-tail/LRPT/Gate-aware=9；
- `lrpt_double_count`：SPT=13/10，LPT/Gate-aware=11/10；
- `fork_multi_unlock`：LPT=12/8，其余为 8。

这说明没有一个简单静态分数逐实例支配其它策略，top-k rollout 的目标应是识别这些局部反例，而不是替换 work-conserving 基线。

### 7. 对阶段 2/3 的直接指导

1. 将 Longest-tail 设为主要 baseline，同时保留 SPT 作为反例修复的重要候选；某些 longest-tail 反例恰好由 SPT 得到最优。
2. rollout 评价必须调用 residual `combined LB`，而不是只看当前 flow 的 tail。
3. gate score 应计算“完成该 flow 后，某个 join 是否真正被解锁”，并聚合 collective completion；直接后继 compute duration 求和的简单版本已出现 1.2 反例。
4. benchmark 应从单 motif 发展到 motif composition：PP/TP backbone + deferred W/DP + optimizer join，只有竞争发生时 heuristic 才有评价意义。
5. 所有新 heuristic 必须同时跑确定性反例、随机 chain、随机 fork/join 和真实缩减窗口，并保存 worst JSON 作为永久回归。

### 8. 产物与验证

- generator/oracle/evaluator：`scripts/benchmark_dag_oracle.py`
- 单元测试：`tests/test_benchmark_dag_oracle.py`
- 汇总报告：`outputs/benchmark_oracle/report.json`
- 每个 benchmark：`outputs/benchmark_oracle/<name>.json`

阶段相关回归：

```text
python -m pytest tests/test_benchmark_dag_oracle.py \
  tests/test_evaluate_chain_heuristics.py \
  tests/test_audit_pipeline_dags.py -q

11 passed
```

`python -m py_compile` 通过。当前环境未安装 `ruff`，因此 `python -m ruff check ...` 无法运行；这是工具缺失，不是 lint error。

加入阶段 1 测试后的全量回归为 `799 passed, 3 skipped, 18 errors`。18 个 error 仍全部是缺少外部 Spectrum-X topology fixture 导致的既有 setup error；没有新增功能测试失败。

## 阶段 2：并行链问题（2026-08-13）

### 0. 阶段结论

阶段 2 没有局限于多项式贪心算法，而是形成了按计算预算逐级增强的算法族：

```text
Longest-tail
  → top-k one-step rollout
  → Monte Carlo schedule sampling
  → bounded beam search
  → pseudo-polynomial exact DP / binary feasibility DP
```

主要结论如下：

1. 一般单瓶颈并行链中，任意 work-conserving 策略保留严格的 2-近似安全网；已有实例族使错误优先级的比值趋近 2。
2. Longest-tail/LRPT 也不是逐实例最优：构造了可任意同比放大的 `9/8` 反例族。因此当前不能声称它们具有小于 `9/8` 的一般近似比。
3. 每条链只有一个 flow 时，按下游 compute tail 非增排序是最优算法；所有 compute lag 为 0 时，任意 work-conserving 策略最优。这是两个严格 restricted results。
4. 100 个精确可解随机实例上，Longest-tail mean ratio 为 1.0037；top-4 rollout 为 1.0008；Monte Carlo-64 为 1.0004；beam-32 在这批实例上全部最优。
5. 非多项式搜索确实有实用价值，但 frontier 小不是唯一条件：5 条链也可能让 exact DP 的状态数超过 20 万。必须同时设置 state budget 和 wall-clock budget。
6. 推荐实际策略不是固定选择某一个算法，而是采用 anytime cascade：有结果时立即可用 Longest-tail，在剩余预算内逐步用 rollout、Monte Carlo、beam 或 exact search 改善 incumbent。

实现位于 `scripts/study_parallel_chains.py`，结果位于 `outputs/parallel_chain_study/report.json`。

### 1. 问题形式化

有 `h` 条相互独立的链。链 `i` 为：

$$
C_{i,1}\to L_{i,1}\to C_{i,2}\to L_{i,2}\to\cdots
\to C_{i,m_i}\to L_{i,m_i},
$$

其中：

- `C` 是共享同一个容量为 1 的 channel 的 communication；
- `L` 是 communication 完成后自动开始的 compute lag；
- 不同链的 compute 可以并行；
- communication 可以在整数时间点抢占和恢复；
- 调度器只在当前 ready communications 中选择；
- workload 静态且有限；
- GPU 上其它固定 compute order 已经写成 DAG/链依赖。

目标是最小化所有链完成的 makespan。令：

$$
P=\sum_{i,j}C_{i,j},\qquad
Q=\max_i\sum_j L_{i,j}.
$$

### 2. Work-conserving 的 2-近似

#### 2.1 定理

对任意 work-conserving schedule `H`：

$$
T_H\le P+Q\le 2OPT.
$$

这里 work-conserving 表示：只要至少有一条 ready communication，channel 就传输某条 ready communication，不主动 idle。

#### 2.2 证明要点

从最后完成的链段反向构造 blocking chain。时间轴上的每个区间只可能属于两类：

1. channel busy：所有这类区间总长不超过全部通信工作量 `P`；
2. channel idle：因为 schedule work-conserving，此时没有 communication ready，blocking chain 必然正在等待本链 compute lag；这些不重叠 idle 区间可依次 charge 到同一条因果链，其总长不超过 `Q`。

所以 `T_H <= P+Q`。同时：

$$
OPT\ge P,\qquad OPT\ge Q,
$$

从而：

$$
\frac{T_H}{OPT}
\le\frac{P+Q}{\max(P,Q)}
\le2.
$$

证明真正需要的条件是单瓶颈、work-conserving、compute ready 后自动执行、固定静态依赖。整数时间和抢占是本阶段 exact algorithm 的条件，不是 `P+Q` charging argument 的核心；非抢占但仍 work-conserving 的版本也能使用同样上界。多链路 topology、collective 多资源占用和主动 idle 不能直接沿用。

#### 2.3 趋近 2 的实例族

令：

```text
链 A：comm(M) → compute(0)
链 B：comm(1) → compute(M) → comm(1) → compute(0)
```

如果错误策略先完整传输 A，例如 LPT，则：

$$
T_H=2M+2,\qquad OPT=M+2,
$$

比值随 `M→∞` 趋近 2。实验中：

| M | OPT | LPT | ratio |
|---:|---:|---:|---:|
| 10 | 12 | 22 | 1.8333 |
| 20 | 22 | 42 | 1.9091 |
| 50 | 52 | 102 | 1.9615 |
| 100 | 102 | 202 | 1.9804 |

这证明 2 对“一般任意 work-conserving priority”基本 tight，但不等于证明每个具体优先级都存在趋近 2 的反例。

### 3. Longest-tail 与 LRPT 反例族

基础实例为：

```text
链 A：C(2) → L(3) → C(1) → L(1)
链 B：C(1) → L(2) → C(2) → L(1)
```

Longest-tail 在初始状态对两条链得到相同 downstream tail，稳定 tie-break 选择 A；LRPT 加入当前 flow 后同样先选 A。结果：

$$
OPT=8,\qquad T_{LTF}=T_{LRPT}=9.
$$

将所有 communication 和 lag 同乘 `k`，得到无限实例族：

| k | OPT | Longest-tail | LRPT | ratio |
|---:|---:|---:|---:|---:|
| 1 | 8 | 9 | 9 | 9/8 |
| 2 | 16 | 18 | 18 | 9/8 |
| 4 | 32 | 36 | 36 | 9/8 |
| 8 | 64 | 72 | 72 | 9/8 |

因此一般情况下 Longest-tail/LRPT 的 worst-case ratio 至少为 `9/8`。这只是 lower-bound construction，尚未得到它们小于 2 的通用 upper bound。

### 4. 实现的算法

#### 4.1 多项式 priority baseline

- FIFO；
- SPT/LPT；
- Longest-delay；
- Longest-tail；
- LRPT；
- Earliest-slack；
- TicTac-style pairwise comparator。

在独立链、共同 makespan 目标下，动态 latest-start slack 为：

$$
slack_i=H-(remaining_i+tail_i),
$$

其中公共 horizon `H` 对所有 ready 链相同，所以 minimum-slack 与 maximum `remaining+tail` 的 LRPT 排序完全等价。本实现和测试都确认二者决策序列相同。这意味着在基础并行链模型里单独加入 earliest-slack 不会产生新算法；到一般 DAG/optimizer deadline 后它才可能不同。

TicTac-style comparator 对任意两条 ready flow 比较局部 `A→B` 与 `B→A` 对两段 newly-released compute 的完成时间，再做确定性 tournament。它是受 TIC/TAC 启发的局部 comparator，不宣称复现原论文的完整 induction。

#### 4.2 Top-k one-step rollout

每次从 Longest-tail 排名前 `k` 的 ready flows 中：

1. 假设执行候选一个时间单位；
2. 从新状态用 Longest-tail 完成整个 residual instance；
3. 选择预测 makespan 最小的候选。

实现 `k=2/4`。它始终选择 ready flow，因此保留 work-conserving 2-bound。

#### 4.3 简单 Monte Carlo

独立采样 64 个 work-conserving 完整 schedule：

- 70% 概率从 tail 接近最优的候选中随机选择；
- 30% 概率在全部 ready flows 中探索；
- 返回样本中 makespan 最小的 schedule；
- 固定 seed，可复现且天然适合并行化。

它不是 MCTS，没有 tree statistics 或 UCB；优势是实现简单、随时可停止，并能发现 greedy tie-break 之外的顺序。

#### 4.4 Bounded beam search

按时间层扩展所有 ready-flow successor，合并相同 residual states，只保留评分最好的 `B` 个状态。评分结合：

- residual `max(P,Q,L)`；
- 从该状态用 Longest-tail 完成的上界。

实现 `B=8/32`。固定 beam width 时是有界近似搜索；beam 足够大且不裁剪时退化为 exact state search。

#### 4.5 伪多项式 exact DP

状态为每条链：

```text
(下一条 communication 的位置, 当前 flow 剩余量, compute cooldown)
```

对所有 ready chain 枚举下一单位通信并 memoize residual state。复杂度依赖数值化 duration，而不是只依赖输入 bit-length，因此是伪多项式/指数参数化算法。

实用优化包括：

- 相同结构链内的状态排序，消除 replica/microbatch 对称性；
- residual lower bound；
- state-count hard limit；
- 先用 heuristic 获得 incumbent；
- 只枚举 ready frontier，而不是全任务排列。

对于固定链数、固定每链深度和较小整数 `P`，它可以实用地得到最优解；不能据此称一般问题为多项式可解。

#### 4.6 二分 makespan + 可行性 DP

在：

$$
LB=\max(P,Q,L)
$$

与 rollout upper bound 之间二分 horizon `H`。对每个 `H`，使用 memoized DFS 判断是否存在 `<=H` 的 ready-flow 序列，并用 residual LB 提前剪枝。

40 个实例上二分结果与直接 exact DP 全部一致。二分不是必然更快：它可能为多个 horizon 重复搜索；其价值在于 deadline feasibility、较紧上下界以及未来 window-demand pruning。

### 5. 正式随机实验

配置：100 个固定 seed (`260813`) 实例，2--5 条链，每链 1--3 个 flow，communication 1--4，compute lag 0--6。所有实例由 direct pseudo-polynomial DP 给出 exact OPT。

#### 5.1 解质量与 Python 原型耗时

| algorithm | mean ratio | P50 | P95 | max | mean runtime |
|---|---:|---:|---:|---:|---:|
| FIFO | 1.1918 | 1.1765 | 1.4231 | 1.5217 | 0.38 ms |
| SPT | 1.1466 | 1.1111 | 1.4000 | 1.5217 | 0.43 ms |
| LPT | 1.2009 | 1.1875 | 1.4348 | 1.5000 | 0.45 ms |
| Longest-delay | 1.0688 | 1.0526 | 1.1923 | 1.3077 | 0.39 ms |
| Longest-tail | **1.0037** | **1.0000** | **1.0370** | **1.1176** | 0.45 ms |
| LRPT | 1.0088 | 1.0000 | 1.0476 | 1.1538 | 0.44 ms |
| Earliest-slack | 1.0088 | 1.0000 | 1.0476 | 1.1538 | 0.42 ms |
| TicTac-style | 1.0688 | 1.0526 | 1.1923 | 1.3077 | 0.40 ms |
| Rollout-2 | **1.0008** | 1.0000 | 1.0000 | 1.0769 | 9.39 ms |
| Rollout-4 | **1.0008** | 1.0000 | 1.0000 | 1.0769 | 15.40 ms |
| Beam-8 | **1.0004** | 1.0000 | 1.0000 | 1.0370 | 77.48 ms |
| Beam-32 | **1.0000** | 1.0000 | 1.0000 | 1.0000 | 215.70 ms |
| Monte Carlo-64 | **1.0004** | 1.0000 | 1.0000 | 1.0400 | 32.08 ms |

时间是当前 Python 实现完成整个小 instance 的 wall-clock，只适合比较预算量级，不代表接入 C++ executor 后的绝对开销。Beam-32 在这 100 个实例上全优只是 observed result，不是 exactness guarantee。

#### 5.2 Exact search 开销

| method | instances | mean states | P95 states | mean runtime | P95 runtime |
|---|---:|---:|---:|---:|---:|
| Direct pseudo-polynomial DP | 100 | 33115.6 | 204697 | 902.94 ms | 5841.64 ms |
| Binary feasibility DP | 40 | 12539.7 | 未单独记录 | 656.04 ms | 未单独记录 |

均值被少数困难实例明显拉高。与亚毫秒 greedy 相比，exact DP 慢约三到四个数量级；但它作为离线 oracle、短窗口 solver 或极小 frontier 的有预算在线优化仍然可用。

#### 5.3 Overlap 指标

| policy | network idle fraction | compute idle fraction | overlap fraction | mean preemptions |
|---|---:|---:|---:|---:|
| FIFO | 0.2617 | 0.7377 | 0.4141 | 0.93 |
| Longest-tail | 0.1303 | 0.6929 | 0.6054 | 0.73 |
| Rollout-4 | 0.1281 | 0.6917 | 0.6137 | 0.74 |

这里 channel idle 并不违反 work-conserving：它表示所有未完成链都在 compute lag 中、当前没有 communication ready。Longest-tail 的主要收益正是更早启动长 compute，从而让后续 flow 更早 ready，减少这种被迫 network idle。

### 6. 规模扩展与切换条件

每个规模取一个随机样例，exact DP state limit 为 30000：

| chains | exact DP | states | Beam-8 | Beam-32 | MC-64 |
|---:|---:|---:|---:|---:|---:|
| 4 | 16 | 938 | 16 | 16 | 16 |
| 6 | 超过状态预算 | >30000 | 31 | 31 | 31 |
| 8 | 超过状态预算 | >30000 | 34 | 34 | 34 |
| 10 | 超过状态预算 | >30000 | 34 | 34 | 34 |

这不是严谨的平均 scalability curve，只是明确展示状态爆炸可以在 6 条链出现。实际切换逻辑应同时看：

```text
ready frontier k
× 每条链剩余 flow 数
× duration 量子化后的 P
× 已探索状态数
× 剩余 wall-clock budget
```

而不能只规定“ready flow 少于 10 就跑 exact”。

### 7. Restricted cases

#### 7.1 严格可证明

**每条链只有一个 flow。** 问题变为所有 job release time 为 0、processing time 为 communication、delivery tail 为 compute 的 `1||Lmax/q_j` 等价形式。按 tail 非增排序最优；可由相邻交换证明：若相邻 `q_a < q_b` 却先排 `a`，交换为 `b,a` 不会增大 `max(C_i+q_i)`。这就是 Longest-delay/Longest-tail 在该子类上的最优性。200 个随机实例全部验证。

**所有 compute lag 为 0。** makespan 恒为 `P`，所以任意 work-conserving schedule 最优。100 个实例全部验证。

#### 7.2 小参数穷举得到的经验结果

**所有 communication size 都为 1。** 穷举 3 条链、每链 2 个 flow、每个 lag 属于 `{0,1,2}` 的全部 729 个实例：Longest-tail 和 LRPT 全部最优；Longest-delay 最坏 `4/3`。这支持继续研究 equal-communication restricted class，但目前不是证明，不能声称一般等长 flow 下最优。

**每链 lag 非增。** 穷举 2 条链、每链 2 个 flow、communication 属于 `{1,2}`、lag 属于 `{0,1,2}` 且链内非增的 576 个实例：Longest-tail/LRPT 最坏达到 1.2。因此“compute lag 单调”单独不足以保证最优。

**重复链。** 对完全相同的 chain template，对称 canonicalization 保留最优值并减少 DP states。这与 LLM 中重复 microbatch/replica 很匹配，是比静态贪心更值得利用的结构。

### 8. 文献边界

相邻的 single-machine precedence-delay 文献说明不能轻易期待一般多项式最优算法：1996 年的工作在 unit processing、integer precedence delays 下已给出强 NP-hard 结果，同时也给出某些 unit-delay 特例的多项式算法：[Single machine scheduling subject to precedence delays](https://doi.org/10.1016/0166-218X(96)00110-2)。

Coupled-task 文献进一步区分 exact gap、minimum gap、抢占和 chain precedence；其中不少看似简单的 non-exact/minimum-gap 变体仍为 NP-hard，而若干 identical/exact-gap chain 子类可多项式求解：[New algorithms for coupled tasks scheduling – a survey](https://www.numdam.org/article/RO_2012__46_4_335_0.pdf)。2026 年关于 bounded delay parameterization 的结果还表明，一般 minimum-delay precedence 问题即使结合 width/delay 参数仍可能具有很强的参数化困难：[Single Machine Scheduling with Precedence Constraints and Bounded Maximum Delay Value](https://doi.org/10.1007/s10878-026-01411-w)。

这些问题与本文“communication 在机器上、compute lag 在机器外并行执行、communication 可抢占”的模型并不完全相同，所以这里只用作复杂性警示，不能直接把其 hardness 或 approximation ratio 搬过来。本文的一流单 flow 结论对应经典 equal-release delivery-tail 排序；相关文献也明确回顾了按 non-increasing tails 的 Jackson rule：[Jackson's semi-preemptive scheduling on a single machine](https://doi.org/10.1016/j.cor.2010.02.008)。

### 9. 推荐的实际算法层级

建议阶段 3 接入 executor 时使用 anytime cascade：

1. **立即结果：** Longest-tail，亚毫秒 Python 原型开销并保留 2-bound；
2. **约 10--20 ms 整图预算：** top-2/top-4 rollout；
3. **约 30 ms 且可并行：** Monte Carlo-64，随时返回当前最好 schedule；
4. **离线或较宽预算：** Beam-8/32；
5. **极小窗口/生成 benchmark：** exact DP 或 binary feasibility，受 state/time hard limit 控制；
6. 任一增强算法超时，立即退回已知 incumbent，不允许主动 idle 或没有决策结果。

对真实 executor，预算应按“单次事件决策”重新测量；本阶段数据是完成整个小实例的 Python wall-clock，不能直接把 15 ms rollout 当成每个事件的固定成本。

### 10. 产物与验证

- 阶段 2 研究脚本：`scripts/study_parallel_chains.py`
- 阶段 2 测试：`tests/test_study_parallel_chains.py`
- 实验报告：`outputs/parallel_chain_study/report.json`
- 复现命令：

```bash
python scripts/study_parallel_chains.py --samples 100 --seed 260813
```

测试覆盖：两个 exact 方法一致、2-tight family、Longest-tail/LRPT 的 `9/8` family、单 flow 最优、bounded search 可行性、`P+Q` 上界、重复链对称化和 earliest-slack/LRPT 等价性。

阶段相关回归为 `19 passed`，`python -m py_compile` 通过。加入阶段 2 后全量回归为 `807 passed, 3 skipped, 18 errors`；18 个 error 仍全部来自缺失的外部 Spectrum-X topology fixture，没有新增功能测试失败。

## 阶段 2 补充：四种算法能否证明小于 2（2026-08-15）

### 1. 当前结论

在当前“单通信瓶颈、通信按单位时间可抢占、compute lag 可并行、整数时间”的模型中，暂时**不能严谨声称** Longest-tail、Rollout-2、Beam-8 或 Beam-32 具有严格小于 2 的一般近似比。已经能够严格确定的是：

| 算法 | 已证明上界 | 已构造下界 | 当前状态 |
|---|---:|---:|---|
| Longest-tail | 2 | 趋近 `5/4` | 是否存在 `<2` 上界仍开放 |
| Rollout-2 | 2，且逐实例不差于 Longest-tail | 趋近 `5/4` | policy improvement 不等于改善 worst-case factor |
| Beam-8 | 2 | 趋近 `5/4` | 固定宽度会裁掉最优分支 |
| Beam-32 | 2 | 趋近 `5/4` | 增大固定宽度只推迟反例尺度 |

所以目前最准确的表述是：四者的一般 worst-case ratio 都落在 `[5/4, 2]` 内。实验强烈支持它们在 LLM 小 frontier 实例上远好于 2，但这属于 instance-dependent 表现，不是一般近似保证。

### 2. 共同的 2 上界为什么仍然成立

令 `P` 为所有 communication 的总时长，`Q` 为任意单链 compute lag 总和的最大值。任一 work-conserving schedule 的 makespan 可写成

\[
T=P+I,
\]

其中 `I` 是网络被迫空闲的总时间。取该 schedule 中最后完成的链：在任何网络空闲 tick，它尚未完成且没有 communication ready，因此一定正在消耗自己的某段 compute lag；故 `I<=Q`。另一方面 `OPT>=P` 且 `OPT>=Q`，所以

\[
T\le P+Q\le 2OPT.
\]

四种实现都不会在存在 ready communication 时主动 idle，因此都继承这个上界。这个证明完全没有使用 tail 排序或 beam 的评分函数；要把 2 降低，必须证明这些额外规则限制了“可避免 idle”的数量。

### 3. Rollout-2 的一个严格正面结果

记 Longest-tail 基策略为 `π`，从状态 `s` 按它执行到底的剩余时间为 `J_π(s)`。Rollout-2 检查 tail 最大的两个 ready 动作，而其中一定包含 `π(s)`；它选择使

\[
1+J_π(f(s,a))
\]

最小的动作。由于选择 `π(s)` 恰好得到 `J_π(s)`，rollout 选出的后继 `s'` 必有

\[
1+J_π(s')\le J_π(s).
\]

沿 rollout 自己产生的轨迹反复应用该式，得到 `T_rollout2<=T_longest-tail`。这是确定性的逐实例支配证明，不依赖随机实验。

但该结论只说明 Rollout-2 不会变差。如果 Longest-tail 的坏实例恰好让“一步偏离后再切回 Longest-tail”的估值看不出收益，Rollout-2 仍会复制坏决策；下面的反例正是这种情况。

### 4. 四种算法共享的渐近 `5/4` 反例

对整数 `k>=2` 构造两条链：

```text
A: C(2k) -> L(3k+1) -> C(2k)
B: C(k)  -> L(2k)   -> C(3k)
```

总通信量为 `8k`。最优顺序先完整执行 `B1`，再执行 `A1`，随后执行 `B2`；等待 1 tick 后执行 `A2`，故 `OPT=8k+1`。这个 1 tick 也不可消除：两个首段通信共 `3k`，无论怎样交错，若让 `B2` 尽早在 `3k` ready，就必须先完成 `B1`，此时 `A1` 只能在 `3k` 完成，而 `A2` 最早为 `6k+1` ready。

初始时 A 的 tail 为 `5k+1`，严格大于 B 的 `5k`，因此 Longest-tail 先做完 `A1`，再做 `B1`。两条链随后几乎同时处于 compute lag，网络产生 `2k` idle，最终 `T_LT=10k`。于是

\[
\frac{T_{LT}}{OPT}=\frac{10k}{8k+1}\longrightarrow\frac54.
\]

这不是 tie-breaking 反例。对 Rollout-2，先给 B 一个 tick 后，补全策略仍因 A 的严格较大 tail 切回 A；两个一步估值不能暴露“必须连续做完 B1”才有的收益，所以 `k>=2` 时也得到 `10k`。

固定宽度 beam 的失败原因更直接：到达好 schedule 需要连续保留“不断推进 B1”的分支 `k` 层，而早期各状态的 residual lower bound 相同或不能区分这项长期收益。当前确定性排序只保留 `B` 个状态；取 `k>B` 后好分支被裁掉。实测：

| 算法 | `k` | OPT | 算法值 | ratio |
|---|---:|---:|---:|---:|
| Beam-8 | 9 | 73 | 90 | 1.2329 |
| Beam-32 | 33 | 265 | 330 | 1.2453 |

随 `k` 增大均趋近 `5/4`。这说明 Beam-32 没有固定宽度 exactness，宽度从 8 增到 32 只是把同一反例推到更大的 duration。对应构造已加入 `scaled_five_four_counterexample()` 和回归测试。

### 5. 为什么当前还没能把上界证明到 3/2 或 5/4

尝试沿“最后完成链”做 charging：网络 idle 可以全部充到该链的 compute lag，因此容易得到 `I<=Q`，也就是原来的 2-bound。要证明 `3/2`，需要进一步把其中**可避免的** idle 充到至少两倍的通信工作、或证明它已经被某条 OPT critical path 覆盖。Longest-tail 只比较 ready flow 的下游 tail；未 ready 链、未来多次 release 和抢占会改变 critical chain，现有 charging 在这里断裂，目前没有得到所需的不等式。

精确搜索也没有提供反证：

- 穷举 2 条链、每链 2 个 flow、communication 属于 `{1,2,3}`、lag 属于 `{0,1,2,3}` 的 20,736 个实例，Longest-tail 最大为 `5/4`，Rollout-2 最大为 `8/7`；
- 新构造按比例放大后把 Rollout-2 的下界也提高到趋近 `5/4`；
- 额外随机搜索 2 条链、每链 3 个 flow 的 800 个精确实例，没有发现超过 `5/4` 的 Longest-tail 反例。

这些结果使“Longest-tail 可能有 `3/2` 上界”和“2-chain/2-flow 子类可能恰为 `5/4`”成为值得继续证明的猜想，但样本不能代替证明。相邻的 coupled-task / precedence-delay 文献研究的往往是不可抢占通信或 exact delay，不能直接移植其近似比；例如 coupled-task 的某些 equal exact-delay 变体甚至有 `5/4-ε` 不可近似结果，而本模型是可抢占通信加 minimum release lag，不能据此宣称这里也存在同样门槛：[Approximating Coupled-Task Scheduling Problems with Equal Exact Delays](https://doi.org/10.1007/978-3-319-44914-2_21)、[Parameterized Complexity of Scheduling Chains of Jobs with Delays](https://arxiv.org/abs/2007.09023)。

### 6. 下一步最有价值的理论路线

1. 先限定为 **2 chains / 2 flows**，把事件顺序分情况，尝试证明 Longest-tail 的 `5/4` 上界；该子类已经有匹配的渐近下界。
2. 对一般链尝试证明较松的 `3/2`：把 `T-P` 分成 critical-chain 必需 idle 与 priority inversion 引起的额外 idle，只对后者做双重 charging。
3. 把 Rollout-2 改成“承诺执行到当前 flow 完成”的 operation-level rollout，或至少同时评价连续执行 `x` ticks；当前 unit-step rollout 看不到上面反例中的连续投资收益。
4. Beam 必须保留 Longest-tail incumbent/path，最终返回 `min(beam, incumbent)`。这不能自动给出 `<2`，但能严格获得“不差于 Longest-tail”，避免评分裁剪产生额外理论风险。
5. 对 LLM 场景给出参数化保证比强求一般常数更现实，例如证明当 `frontier<=w`、每个 flow duration 量化后 `<=p` 时，宽度或 DP 状态达到某个 `f(w,p)` 即 exact。
