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

## 阶段 3：从并行链推广到一般 DAG（2026-08-17）

### 1. 阶段目标与模型边界

阶段 3 已实现一个独立研究原型 `scripts/study_general_dag_heuristics.py`，将阶段 2 的 tail、rollout 和 bounded search 推广到带 fork、join 和多层依赖的一般 DAG。执行模型与阶段 1 exact oracle 完全一致：

- 所有 communication 共享一个单位容量、可抢占瓶颈；
- ready compute 立即开始，彼此可并行；
- duration 为整数时间量子；
- compute resource order 已经编码为 DAG edge；
- 调度器只能选择 ready communication，有 ready flow 时不允许主动 idle。

这仍然是局部单瓶颈研究模型，不是 topology-aware executor。下面的实验不能证明多 NIC、多链路、fluid bandwidth sharing 下仍有相同近似比。

### 2. Residual dynamic tail

不再为完整 DAG 只计算一次静态 tail。每个决策状态先完成 compute closure，再按照当前剩余 duration 在 residual DAG 上反向计算：

\[
q_s(v)=\max_{(v,x)\in E,\ x\text{ unfinished}}
\left(d_s(x)+q_s(x)\right).
\]

其中已经完成的节点被删除，正在执行的 compute/communication 使用 remaining duration，尚未开始的节点使用 profile duration。这样有三个直接效果：

1. 正在并行执行的下游 compute 会不断缩短，旧的关键分支可以自动降级；
2. join/fork 后的关键后继可以随 residual state 改变，不需要永久 chain decomposition；
3. ready flow 的分数反映“从现在开始还剩多少关键工作”，而不是初始 DAG 上已经过时的距离。

原型同时计算两个可证明安全的 residual lower bound：

- `P_res`：所有未完成通信的剩余总量；
- `L_res`：忽略通信竞争后的 residual critical path。

rollout/beam 使用 `max(P_res,L_res)` 剪枝或打分，但不把它误当成精确 cost-to-go。

### 3. Join gating 的实现与负面发现

按照规划实现了最后阻塞者指标。对于 ready flow `v` 的直接 join 后继 `x`：

\[
g_s(v,x)=\max\left(0,EF_s(v)-
\max_{u\in pred(x),u\ne v}EF_s(u)\right).
\]

若 `v` 比其它未完成输入更晚到达，则 `g>0`；若其它输入明显更晚，则 `g=0`。最初还尝试过根据“等待其它输入”的时间直接折减 tail，但专项搜索发现这种折减会变差，因此已经撤销：optimistic EF 没有计入通信竞争，不能安全地拿来改写 critical path。

即使只把原规划中的 `g` 作为加分项，仍然不能安全地单独作为贪心策略。新增固定反例 `last_blocker_overboost`：

| method | makespan |
|---|---:|
| Exact OPT | 20 |
| Dynamic-tail | 20 |
| Dynamic-tail + raw `g` | 21 |
| Rollout-2 with incumbent | 20 |

原因是 `g` 容易把“当前剩余时间较长”再次奖励一遍，产生类似 LRPT/LPT 的 double counting。阶段 3 因此得到一个重要设计修正：

> join gating 适合用于扩充 top-k 候选、识别需要 lookahead 的冲突，不应未经 rollout 验证就直接线性加到最终优先级。

### 4. Event-level top-k rollout

实现了 `Rollout-2/4/8`。每次决策按 `dynamic tail + join signal` 筛选候选，对每个候选执行以下预测：

1. 承诺执行候选 flow，直到该 flow 完成、某个 active compute 完成或有新 flow ready；
2. 在新 residual state 上重新计算 dynamic tail、join signal 和 lower bound；
3. 用 Dynamic-tail 基策略补全剩余 schedule，得到可行 upper bound；
4. 先按完整 upper bound 选择，`delta + residual LB` 只用于 tie-break；
5. 到预测的事件边界后重新规划。

这修正了阶段 2 unit-tick rollout 的主要缺陷：如果一个通信必须连续推进多个 tick 才能释放长 compute，现在 rollout 能看到这项收益。

研究脚本还单独保留一份完整 Dynamic-tail incumbent。如果增强 schedule 在确定性 profile 下反而更差，就返回 incumbent。因而当前离线原型逐实例不差于 Dynamic-tail。真实在线 executor 中 profile 可能有误差，接入时应保留已生成的 baseline plan，而不能假定预测 upper bound 就是真实完成时间。

### 5. Local event beam

实现了 receding-horizon `Beam-8`：

- 展开单位是 communication/compute event，不是 tick；
- 默认只看未来 3 个事件层；
- 每层保留 8 个 residual state；
- 排序首先使用 `elapsed + residual LB`，再使用 Dynamic-tail 补全 upper bound；
- 完整 Dynamic-tail schedule 始终作为最终 incumbent。

它解决的是局部冲突窗口，不搜索整个 iteration。固定宽度仍没有 exact 或小于 2 的一般保证；incumbent 只能保证当前确定性研究模型中的返回值不差于基线。

### 6. Benchmark 扩展

本阶段共评估 73 个 exact-oracle 可解实例：

| category | 数量 | 说明 |
|---|---:|---|
| adversarial | 8 | 阶段 1 反例加 last-blocker overboost |
| LLM motif | 7 | PP wave、1F1B、ZB B/W fork、W/DP optimizer join、TP+PP 等 |
| real reduction | 8 | 从 992-node 真实 1F1B effective DAG 的 8 个高密度时间桶缩减 |
| random general DAG | 50 | 2--5 个分支、可选第二段通信、nested join 和 optimizer join |

真实窗口提取器也从“只取最密集时间桶”扩展为可指定 `bucket_rank`，因此可以扫描多个冲突位置。每个窗口仍保留直接前驱/后继、join 的 companion inputs、外部 release 和保守 downstream tail，并只纳入 exact state limit 内可解的窗口。

### 7. 正式实验结果

复现参数为 `samples=50, seed=260817`：

| method | mean ratio | observed max | optimal fraction | mean Python runtime |
|---|---:|---:|---:|---:|
| Static Longest-tail | 1.00361 | 1.1250 | 95.89% | 1.21 ms |
| 旧 Static gate-aware | 1.01440 | 1.1250 | 76.71% | 1.28 ms |
| Residual Dynamic-tail | 1.00190 | 1.0909 | 97.26% | 2.24 ms |
| Dynamic-tail + raw last-blocker `g` | 1.00258 | 1.0909 | 95.89% | 2.20 ms |
| Event Rollout-2 | **1.00000** | **1.0000** | **100%** | 28.07 ms |
| Event Rollout-4 | **1.00000** | **1.0000** | **100%** | 33.25 ms |
| Event Rollout-8 | **1.00000** | **1.0000** | **100%** | 33.22 ms |
| Local Beam-8 | **1.00000** | **1.0000** | **100%** | 290.70 ms |

这些是 73 个实例上的 observed ratios，不是一般近似比证明。尤其不能由“全部命中最优”推出 Rollout-2 是 exact algorithm。

Dynamic-tail 的两个非最优随机实例分别为：

```text
random_join_30: Dynamic-tail 24, OPT 22, Rollout-2 22
random_join_40: Dynamic-tail 22, OPT 21, Rollout-2 21
```

因此 rollout 确实修复了可区分实例，而不是只在所有策略相同的图上得到 100%。另外，Rollout-2/4/8 在当前小图上结果完全相同，说明 frontier 很小时 `k=2` 已足够；没有证据支持在线默认使用更贵的 `k=8`。

### 8. LLM motif 与真实窗口应如何解读

7 个手工 LLM motif 和 8 个真实缩减窗口上，所有主要策略都得到 OPT。这首先说明 DAG 语义、residual 更新和 oracle 对接正确，但不能证明 heuristic 在真实完整训练中有收益。

真实缩减窗口无法区分算法的主要原因是：

- 当前窗口只保留 4 个 seed flows，选择空间仍偏小；
- 边界 release 与保守 tail 可能主导 makespan；
- 单瓶颈量化隐藏了不同 PP/TP/DP 路径之间的资源差异；
- 窗口从 baseline earliest-start 时间桶选取，未必正好对应 heuristic 会改变的 conflict state。

阶段 4 不能继续只增加相似 motif 数量。更有价值的是从执行 trace 中寻找“至少两种基线给出不同动作或不同 makespan”的 endogenous conflict window，再交给 exact oracle。

### 9. 理论性质与尚未解决的问题

四种新策略始终只选择 ready flow，且有 ready flow 时不主动 idle，因此在当前单瓶颈模型下保留规划中的 work-conserving 2-approx 安全底座。Dynamic-tail、join bonus 或有限 rollout 尚未带来严格小于 2 的一般证明。

已得到的更实际性质是：

1. residual tail 不需要静态 chain decomposition，能自然处理 fork/join；
2. event rollout 修复了 unit-tick lookahead 看不到连续投资的问题；
3. 完整 baseline incumbent 使增强搜索在确定性 profile 下不会降低结果；
4. raw join-gating bonus 不是安全贪心，需要由 rollout 验证；
5. 固定 Beam-8 的实测收益与 Rollout-2 相同，但开销约高一个数量级，目前没有在线使用优势。

### 10. 推荐的阶段 3 产出策略

当前最合理的一般 DAG 算法不是单独的 `Gate-aware priority`，而是：

```text
Residual Dynamic-tail baseline
        + join/gate-aware candidate generation
        + top-2 event-level rollout
        + complete baseline incumbent
```

Beam-8 保留为离线/短窗口研究工具。下一阶段接入 LLM 特殊结构时，应优先加入：

- backbone 与 deferred W/DP 的不同 deadline/tail；
- optimizer join 的 latest-start slack，而非 raw last-blocker duration；
- `(stage, phase, microbatch offset, chunk)` 模板状态压缩；
- 多资源 `P_r` 和真实 route overlap；
- 从 heuristic 分歧点提取窗口，而不是从 baseline 时间桶静态取窗。

### 11. 产物与复现

- 一般 DAG 研究脚本：`scripts/study_general_dag_heuristics.py`
- 一般 DAG 回归：`tests/test_study_general_dag_heuristics.py`
- 扩展后的真实窗口参数：`scripts/benchmark_dag_oracle.py::reduce_effective_dag(bucket_rank=...)`
- 正式报告：`outputs/general_dag_heuristics/report.json`
- 复现命令：

```bash
python scripts/study_general_dag_heuristics.py --samples 50 --seed 260817
```

阶段 0--3 定向回归为 `25 passed`，三个研究脚本的 `py_compile` 通过。全量回归为 `816 passed, 3 skipped, 18 errors`；18 个 error 仍全部来自缺失的外部 `Spectrum-X_8g_8gps_400Gbps_H100` topology fixture，与本阶段修改无关。

## 阶段 3.5：困难实例与端到端 Counterfactual Bonus（2026-08-09）

### 1. 为什么需要重做实验

阶段 3 的 73 个实例中，Dynamic-tail 已有 97.26% 最优率，7 个 LLM motif 和 8 个真实缩减窗口又全部无法区分算法。这只能证明实现基本正确，不能可靠判断 join 信息或 rollout horizon 的独立贡献。

阶段 3.5 因此不再从普通随机分布计算“总体最优率”，而是只保留满足以下条件的实例：

```text
exact oracle 可解
且 Dynamic-tail makespan > OPT
```

这相当于直接以

\[
regret_{DT}=T_{DT}-OPT>0
\]

作为 benchmark 准入条件。正式搜索尝试 513 个一般 fork/join DAG，得到 20 个困难实例，接受率 3.90%。这也反过来证实：原来的普通随机 benchmark 中约 96% 的实例确实没有能力区分增强算法。

20 个困难实例上 Dynamic-tail 的 mean ratio 为 1.05276，observed max 为 1.07692。所有实例都有真实正 regret，因此“修复率”和“gap closed”比普通总体最优率更有意义。

### 2. 端到端 bonus 的正式定义

对当前状态 `s` 和候选动作 `v`，令执行到指定 horizon 后的状态为 `s_v`、耗时为 `delta_v`，再用 Dynamic-tail 补全剩余 schedule：

\[
\widehat C(v\mid s)=\Delta_v+\widehat J_{DT}(s_v).
\]

相对于 Dynamic-tail 基础动作 `a_0`，端到端 counterfactual bonus 定义为：

\[
B(v\mid s)=\widehat C(a_0\mid s)-\widehat C(v\mid s).
\]

最终直接选择 `C_hat` 最小、等价地 `B` 最大的候选，不再把 bonus 与 tail 相加。这样 join 解锁、后续 compute、机会成本和通信推迟都通过完整补全 schedule 统一进入端到端评价，避免 raw join bonus 的重复奖励。

完整 Dynamic-tail schedule 继续作为 incumbent；若 counterfactual schedule 更差则返回 incumbent。因此在当前确定性 profile 研究模型中，所有消融结果都不会比 Dynamic-tail 更差。

### 3. 两个正交消融维度

候选来源与动作执行范围被严格拆开。

候选来源：

- `DT`：只取 Dynamic-tail top-k；
- `Join`：只取 last-blocker urgency top-k；
- `Hybrid`：`k=2` 时保留 1 个 Dynamic-tail 候选和 1 个不同的 Join 候选。

动作 horizon：

- `tick`：只推进一个时间量子；
- `event`：推进到当前 flow 完成、active compute 完成或新 flow ready；
- `flow`：强制推进到当前 flow 完成。

正式实验使用 `top_k=2`，形成 7 组主要消融。

### 4. 困难集正式结果

| 方法 | mean ratio | observed max | 修复为 exact | mean gap closed | mean runtime |
|---|---:|---:|---:|---:|---:|
| DT + tick | 1.01471 | 1.05882 | 14/20 | 70% | 32.79 ms |
| **DT + event** | **1.00227** | **1.04545** | **19/20** | **95%** | 21.24 ms |
| DT + flow-complete | 1.02165 | 1.07692 | 12/20 | 60% | 12.79 ms |
| Join + event | 1.00227 | 1.04545 | 19/20 | 95% | 21.44 ms |
| Hybrid + tick | 1.01471 | 1.05882 | 14/20 | 70% | 32.15 ms |
| **Hybrid + event** | **1.00227** | **1.04545** | **19/20** | **95%** | 20.96 ms |
| Hybrid + flow-complete | 1.02165 | 1.07692 | 12/20 | 60% | 12.52 ms |

这是本轮最明确的正面结论：

> 用户提出的“bonus 应衡量端到端改善”是正确方向；在全部由 Dynamic-tail 失败实例组成的困难集上，next-event counterfactual rollout 修复了 19/20，并平均关闭 95% 的 optimality gap。

同时 horizon 不能随意选择：

- `tick` 只关闭 70% gap，仍然看不到需要连续投资一小段时间才能释放的收益；
- `flow-complete` 只关闭 60% gap，对长 flow 过度承诺，错过中途新 compute/flow 事件；
- `next-event` 在信息量和可撤销性之间取得最好平衡。

`flow-complete` runtime 更低不是优势，而是因为决策次数更少；它以明显更差的 schedule quality 换取了这一开销。

### 5. Join 候选有没有独立价值

当前答案仍是：**没有观察到独立正收益。**

20 个困难实例中：

- 15 个实例沿 Dynamic-tail 路径出现过 `ready frontier > 2`；
- 共出现 59 个 `frontier > 2` 的决策 tick；
- Join top-2 真正替换 Dynamic-tail top-2 候选集的状态只有 3 个；
- `DT+event`、`Join+event`、`Hybrid+event` 的最终结果逐实例完全相同。

为避免“困难集仍不是 join-sensitive”的质疑，又做了专项筛选：先要求 Join top-2 与 Dynamic-tail top-2 确实不同，再要求 Dynamic-tail 非最优。在前 1,546 个尝试中只找到 4 个满足条件的实例；这 4 个实例上三种 event rollout 仍全部得到相同结果。

因此现在可以更精确地区分两个结论：

1. **端到端 counterfactual 评价有效；**
2. **当前 `EF last-blocker` join 特征没有显示候选增益。**

这不证明所有 join 信息都无用，只说明当前特征过于局部且激活率太低。下一步若继续研究 join，应改用 optimizer/latest-start slack、join 后 residual critical tail 或真实 resource-delay sensitivity，而不是继续调整 raw `EF(v)-EF(other)` 权重。

### 6. Decision-centric 真实窗口

新增脚本先把完整 992-task 真实 1F1B effective DAG 量化，然后沿 Dynamic-tail schedule 在**同一个 residual state**比较：

- 初始静态 tail；
- residual Dynamic-tail；
- raw gate-tail；
- SPT；
- LPT。

不再按通信密度取窗，而是在动作不同的位置提取 ready flows、两层下游节点、外部 release 与有限 boundary tail。为了让 exact oracle 可解，正式设置为：

- `quantum_us=10000`；
- 最多 24 个窗口任务；
- 下游深度 2；
- boundary release/tail 上限 30 个量子；
- exact state limit 50,000。

完整量化 DAG 共执行 560 tick，其中发现 422 个策略分歧 tick。大量分歧窗口仍因任务数或 exact state limit 被跳过，最终目标 8 个、得到 5 个 exact 可解窗口。

但这 5 个窗口上 Static-tail、Dynamic-tail、Hybrid-event 和 OPT 仍然全部相同，`distinguishing_windows=0`。这个负面结果说明：

> “策略当前动作不同”仍不等于“这个动作会改变局部或端到端 makespan”。

目前窗口压缩还有两项根本限制：10 ms 粗量化会合并小 flow/compute 差异；截断的 boundary tail 只能保持局部结构，不能完整表达 iteration 末端影响。因此真实窗口闭环已经从“没有分歧状态”推进到“有 422 个动作分歧”，但尚未得到真实性能可区分窗口。

### 7. 当前推荐算法

阶段 3.5 后，推荐从原来的泛称 `Gate-Aware Dynamic Tail Rollout` 收敛为：

```text
Residual Dynamic-tail baseline
        + Dynamic-tail top-2 candidates
        + next-event counterfactual completion estimate
        + complete Dynamic-tail incumbent
```

当前没有证据要求在线候选中强制保留 raw join bonus。Join、optimizer slack、backbone/deferred role 可以继续作为实验特征，但必须用同样的 end-to-end counterfactual 评价证明其独立贡献。

### 8. 下一步研究重点

1. 对唯一未被 event rollout 修复的困难实例做最小化，识别需要 two-event 还是更强 lower bound；
2. 将 join urgency 改成 `latest-start slack` 与“join 后关键尾长”，再做同样的 join-sensitive 筛选；
3. 真实窗口改为保留原始微秒 duration，并使用 branch-and-bound/CP-SAT 或 gap certificate，而不是依赖粗量化 DP；
4. 从完整 executor 的多资源状态保存 route/link contention，使 PP/TP/DP 分歧不再被单瓶颈模型抹平；
5. 阶段 4 的 backbone/deferred W/DP 策略也必须在 hard subset 上报告 gap closed，而不是回到普通总体最优率。

### 9. 产物与复现

- Counterfactual/hard benchmark 脚本：`scripts/study_counterfactual_bonus.py`
- 候选来源与 horizon 扩展：`scripts/study_general_dag_heuristics.py::rollout_schedule`
- 回归测试：`tests/test_study_counterfactual_bonus.py`
- 正式报告：`outputs/counterfactual_bonus/report.json`
- 复现命令：

```bash
python scripts/study_counterfactual_bonus.py \
  --hard-samples 20 --real-windows 8 --top-k 2 --seed 260818
```

阶段 0--3.5 定向回归为 `29 passed`。全量回归为 `820 passed, 3 skipped, 18 errors`；18 个 error 仍全部是缺失外部 Spectrum-X topology fixture 的已知环境问题，没有新增功能失败。

## 阶段 4a：从单 channel 扩展到路径资源冲突（2026-08-10）

### 1. 本阶段先回答什么问题

阶段 0--3.5 把所有通信压在同一个 channel 上，因此任意时刻只能推进一条 flow。这个模型适合研究“先传哪一条”，但会把现实中两种完全不同的情况混为一谈：

- 两条 flow 经过同一条 NIC/uplink，确实必须竞争；
- 两条 flow 的 route 完全不重叠，本来可以同时传输。

如果先在这个单 channel 模型上加入 backbone、W/DP deadline 等 LLM 特征，可能会把“虚构出来的冲突”解释成 DAG 策略收益。因此本阶段没有直接修改完整 executor，而是先建立一个小规模可精确求解的 topology-conflict oracle，检查单 channel 结论能否外推。

### 2. 多资源模型

每条通信 (v) 除 duration 和 DAG deps 外，再带一个资源集合 (R_v)。资源可以是实际 route 上的有向链路，也可以是 NIC injection、PP fabric、DP fabric 等逻辑瓶颈。每个整数时间量子内选择一个 ready flow 集合 (A)，要求

\[
R_u\cap R_v=\varnothing,\qquad \forall u\ne v\in A.
\]

也就是说，共享任意资源的 flow 不能同时推进，资源集合互不相交的 flow 可以并行推进。模型继续保留：

- flow 可在量子边界抢占；
- ready compute 立即开始且彼此并行；
- compute resource order 已经编码为 DAG edge；
- 调度动作从“一条 ready flow”变成“一个兼容的 ready flow 集合”。

这仍不是完整带宽模拟。当前每个资源容量归一化为 1，flow 必须同时占有 route 上全部资源，不表达 max-min sharing、不同链路带宽、packet pipeline、ECMP 多路径和细粒度 NIC duplex 约束。它的定位是 topology conflict 的小窗口 oracle，而不是替换 `AnalyticalExecutor`。

### 3. 与现有拓扑代码的连接

新增 `route_resource_sets(workload, route_table)`，直接读取现有 `RouteTable.get_path(task)`，把相邻节点对变成资源：

```text
path [0, 8, 12, 3]
    -> {(0,8), (8,12), (12,3)}
```

默认使用有向链路，与当前 topology/executor 的 full-duplex link 表示一致；敏感性实验可用 `directed=False` 合并正反方向。`task_id_prefix` 可把真实 task id 映射到 effective benchmark 使用的 `t{id}`。这一步没有改通用 `Task` schema、builder、serializer 或 executor。

### 4. 精确 Oracle 与下界

精确 DP 的状态仍是所有 task 的 residual duration。每一步枚举 ready flow 的 inclusion-maximal compatible sets，并推进一个时间量子。这里只枚举 maximal set 是安全的：在当前独占、无 setup cost 的模型里，给一个动作加入不冲突的 ready flow 不会延迟原动作、compute 或任何其他资源，只可能让新 flow 更早完成。

多资源下界改为

\[
LB=\max\left\{L,\max_r P_r\right\},
\]

其中 (L) 是忽略资源竞争的 residual critical path，(P_r) 是所有仍会使用资源 (r) 的 flow residual work 之和。单 channel 的总通信量 (P) 不再是合法的多资源下界，因为不重叠流量可以并行；它只在把所有 (R_v) 都压成同一个资源时恢复。

### 5. 对照算法

本阶段实现了四类动作选择：

1. `Dynamic-tail pack`：按 residual tail 排序，再贪心装入互不冲突的 flow。这是把阶段 3.5 基线自然提升到多资源后的版本；
2. `Resource-tail pack`：tail 优先，在相近选择中加入 residual bottleneck load；
3. `Bottleneck-first`：优先处理所经资源剩余负载最大的 flow；
4. `Set rollout-k`：候选不再是单条 flow，而是 maximal compatible set；对每个集合执行到 next event，再用完整 Dynamic-tail pack 补全端到端 makespan。完整基线继续作为 incumbent。

### 6. Benchmark 设计

正式实验包含 53 个 exact-oracle 可解实例：3 个手工 topology motif 加 50 个随机 fork/join DAG。每条随机通信经过 endpoint NIC、按 PP/DP/TP role 区分的 fabric，并以 35% 概率再经过 shared uplink，从而同时包含 disjoint、完全重叠和部分重叠 route。

三个手工 motif 验证了模型语义：

| motif | multi-resource OPT | 压成 single-channel OPT | 含义 |
|---|---:|---:|---|
| `disjoint_routes` | 7 | 11 | 两条 4-tick flow 同时传，之后各有 3-tick compute tail |
| `shared_uplink` | 11 | 11 | endpoint 不同但共享 uplink，不能并行 |
| `pp_dp_partial_overlap` | 9 | 13 | PP/DP 共享 NIC，TP 使用独立本地资源 |

### 7. 正式实验结果

复现参数为 `samples=50, seed=260819, max_states=500000`：

| method | mean ratio | observed max | optimal fraction | mean Python runtime |
|---|---:|---:|---:|---:|
| Dynamic-tail pack | 1.01749 | 1.15789 | 75.47% | 2.56 ms |
| Resource-tail pack | 1.01677 | 1.15789 | 75.47% | 2.55 ms |
| Bottleneck-first | 1.02947 | 1.21053 | 71.70% | 2.56 ms |
| **Set rollout-2** | **1.00463** | **1.08696** | **92.45%** | 22.54 ms |
| Set rollout-4 | 1.00463 | 1.08696 | 92.45% | 24.64 ms |

这些仍是有限小实例上的 observed ratios，不是多资源模型的一般近似比。

把同一批 DAG 的所有 flow 压成一个 channel 后，即使两边都取 exact OPT，single-channel makespan 平均仍比多资源 OPT 高 **14.88%**，最大高 **57.14%**。因此拓扑扩展不是只改变算法实现：单 channel 确实会制造大量不存在的串行化，并可能改变 heuristic 的相对评价。

### 8. 困难子集与特征贡献

53 个实例中有 13 个满足 `Dynamic-tail pack > multi-resource OPT`。只在这个困难子集上统计：

| method | 修复到 exact | mean gap closed |
|---|---:|---:|
| Resource-tail pack | 1/13 | 10.26% |
| Bottleneck-first | 3/13 | -15.38% |
| **Set rollout-2** | **9/13** | **69.23%** |
| Set rollout-4 | 9/13 | 69.23% |

主要发现是：

1. **资源负载不能直接取代 DAG tail。** 纯 Bottleneck-first 虽偶尔修复实例，但平均 gap closed 为负，说明提前清理热点资源可能推迟真正的 critical unlock；
2. **简单 Resource-tail 的独立价值很小。** 它与 Dynamic-tail 只在 3/53 个实例上产生不同 makespan，其中 2 个更好、1 个更差；只靠一个静态负载 tie-break 不足以理解集合选择的后果；
3. **阶段 3.5 的端到端 counterfactual 思路可以自然推广。** 把候选动作改成兼容集合后，Rollout-2 修复 9/13 个困难实例并关闭 69.23% gap；
4. **当前 frontier 下 `k=4` 没有额外收益。** Rollout-2 与 Rollout-4 在全部 53 个实例上 makespan 完全相同，不支持在线默认扩大到 4；
5. **调度对象应是兼容集合，不再是一条 flow。** 真实 topology 下只给 flow 排一个全序会丢失最重要的并行性；全序至多适合作为 greedy packing 的候选顺序。

### 9. 对近似保证的影响

单 channel 的 work-conserving 2-approx 证明不能直接搬到这里。原证明把所有 network-busy 时间 charge 到总通信量 (P)，但在多资源模型中：

- 多条 flow 可同时推进，makespan 不能再由总 work (P) 正确刻画；
- 一个 maximal compatible set 仍可能选错“组合”，长时间占住多个关键资源；
- 不同资源的 busy interval 会重叠，简单相加 (P_r) 会重复计时。

当前可安全使用的是下界 `max(L, max_r P_r)` 和 exact small-window oracle；尚未得到 Dynamic-tail pack 或 Set rollout 的常数近似比。若要继续做理论保证，应研究 route-resource hypergraph 的结构参数，例如每条 flow 最多占用的资源数、冲突图的 interval/chordal 性质、树拓扑路径的特殊性质，而不能继续沿用单机抢占调度的证明。

### 10. 下一步

1. 用真实 workload 的 effective DAG task id 加 BFS/Greedy route，提取 PP/TP/DP/EP 的 route-resource sets 和冲突图统计；
2. 不直接把完整 992-task 图交给指数 Oracle，而是在真实执行状态中提取 `ready frontier + downstream boundary` 的 decision-centric 多资源窗口；
3. 先保留有向链路独占模型做 oracle，再逐步加入 NIC injection resource 和每资源容量；
4. 用完整 executor 的 max-min bandwidth allocation 复核小模型中产生分歧的动作，测量独占冲突模型与真实共享带宽的误差；
5. 在资源语义可信之后，再加入 backbone/deferred W/DP、optimizer latest-start slack 和周期模板，并继续报告 hard-subset gap closed。

### 11. 产物与复现

- 多资源模型、Oracle、heuristic 与 route adapter：`scripts/study_multiresource_dag.py`
- 回归测试：`tests/test_study_multiresource_dag.py`
- 正式报告：`outputs/multiresource_dag/report.json`
- 复现命令：

```bash
python scripts/study_multiresource_dag.py \
  --samples 50 --seed 260819 --max-states 500000
```

阶段 4a 新增定向回归 `7 passed`；阶段 1--4a 联合定向回归 `22 passed`。只读 syntax 检查和 `git diff --check` 通过；当前环境未安装 `ruff`。全量回归为 `827 passed, 3 skipped, 18 errors`，18 个 error 仍全部来自缺失的外部 `Spectrum-X_8g_8gps_400Gbps_H100` topology fixture，与本阶段修改无关。

## 阶段 4b：真实 effective DAG 与 AlibabaHPN route 窗口（2026-08-12）

### 1. 本阶段连接了哪些真实组件

阶段 4a 的 53 个实例使用人工/随机资源集合，只证明了多资源模型有必要，不能证明真实 LLM DAG 中存在同样的调度空间。本阶段把以下仓库内真实组件闭环连接：

```text
WorkloadBuilder / advanced pipeline builder
    -> pipeline compute serializer
    -> data deps + compute_order resource edges
    -> effective DAG
    -> AlibabaHPN_16g topology
    -> BFS RouteTable
    -> 每条 flow 的 directed-link resource set
    -> decision-centric multi-resource exact window
```

使用的 probe 为 `pp=2,tp=2,dp=2,ga=4,layers=4`，包含 PP、TP 和 DP 流量；通信大小与计算时长来自 `build_hybrid_input` 的固定 homogeneous profile。因而这里的“真实”准确含义是：真实 builder、真实 effective dependency、真实 topology 文件和真实 BFS route；它仍不是 GPT-7B/13B 实测 AICB profile，不能把绝对 makespan 当成模型训练性能。

新增脚本在构造 benchmark 时保留原始 task id 映射：effective DAG 的 `t{id}` 与 workload flow id 一一对应，因此 route 不是按 role 猜测或随机分配的。

### 2. NIC resource 敏感性

仅比较 route edge 仍可能漏掉一种冲突：同一 GPU 的 PP 和 TP flow 可能分别走不同物理边，但共享 NIC injection。`route_resource_sets` 因此新增两个可选资源：

```text
("nic_tx", src)
("nic_rx", dst)
```

正式报告默认使用 `directed links + NIC-TX + NIC-RX`，共 32 个资源；另跑一遍 `--link-only`，只保留 16 个有向链路资源。两种模型在本轮五种 pipeline 的以下所有统计上完全相同：

- full schedule 的 conflict fraction；
- policy disagreement tick；
- 提取出的 5 个窗口；
- 每种 heuristic 的 makespan 与 exact ratio。

这不是说 NIC 永远无关，而是说明当前 probe 的 ready 波次没有同时出现“端点相同但 route edge 不同”的 flow。NIC 资源已经保留为后续真实 trace、多 job 和不同 placement 的敏感性选项。

### 3. 多资源 decision-centric 窗口

对每种 pipeline，沿 `Dynamic-tail pack` 的完整执行轨迹，在同一个 residual state 上比较：

- Dynamic-tail pack；
- Resource-tail pack；
- Bottleneck-first；
- SPT/LPT pack；
- PP-first/DP-first pack。

当至少一个策略选出的 maximal compatible set 不同时，以这些 flow 和共享资源的 ready competitors 为 seed。由于 depth-1/2 窗口会迅速扩张到 42 个以上节点，本阶段正式使用 `depth=0 + boundary tail/release`：保留当前决策 flow 的原始 residual duration 与 route resources，并用保守 boundary compute 表达其下游 tail。它适合判断“当前集合选择是否会影响有限 horizon”，但不能代替完整 iteration 的因果影响；窗口结果必须和 full trace 的分歧密度一起解释。

### 4. 五种 pipeline 的完整轨迹统计

时间量子为 25 us。完整图和 Dynamic-tail packed schedule 的结果如下：

| pipeline | tasks | simulated ticks | mean ready width | mean action width | ready-pair conflict | disagreement ticks | exact windows |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1F1B | 992 | 2764 | 4.107 | 4.053 | 0.810% | 2 | 1 |
| Interleaved 1F1B | 1056 | 2502 | 4.135 | 4.135 | 0% | 0 | 0 |
| Zero Bubble | 992 | 2666 | 4.000 | 4.000 | 0% | 0 | 0 |
| Bidirectional/Chimera | 1088 | 2678 | 6.552 | 6.069 | 3.407% | 16 | 4 |
| DualPipe | 1072 | 2225 | 8.000 | 8.000 | 0% | 0 | 0 |

最重要的结构发现不是某个新 heuristic 获得明显收益，而是：

> 当前单 job、固定 placement 的真实 route ready frontier 大部分时间接近一个天然兼容集合；除 Bidirectional 外，route conflict 极少。单 channel 模型把 mean action width 4--8 的并发通信强制串行，严重改变了问题本身。

Zero Bubble 和 DualPipe 的 frontier 并不小，但所有 ready flow 都能同时装入兼容集合，所以不存在“先传哪条”的离散选择。这个结果也再次提醒：serializer 的执行顺序或 pipeline 名称不能自动制造 network scheduling opportunity，必须检查 effective deps 与 route overlap。

### 5. Exact 窗口结果

共得到 5 个 exact 多资源窗口：1 个来自 1F1B，4 个来自 Bidirectional。1F1B 窗口包含 4 条 TP 和 2 条 PP flow；4 个 Bidirectional 窗口均为 6 条 DP flow。

| method | mean ratio | observed max | optimal fraction |
|---|---:|---:|---:|
| Dynamic-tail pack | 1.00000 | 1.00000 | 100% |
| Resource-tail pack | 1.00000 | 1.00000 | 100% |
| Bottleneck-first | 1.00000 | 1.00000 | 100% |
| SPT pack | 1.00000 | 1.00000 | 100% |
| LPT pack | 1.06667 | 1.16667 | 60% |
| PP-first / DP-first | 1.00000 | 1.00000 | 100% |
| Set rollout-2 | 1.00000 | 1.00000 | 100% |

5 个窗口中只有 2 个能区分算法，区别仅来自 LPT：在两个 `OPT=6` 的 Bidirectional DP 窗口中，LPT 得到 7。Dynamic-tail 与所有拓扑增强方法都命中 OPT，因此本轮没有证据说明 Resource-tail、Bottleneck-first 或 Rollout 在真实 probe 上比 Dynamic-tail 更好。

这与阶段 4a 并不矛盾：阶段 4a 的困难随机资源图证明 Set rollout 在“确有组合冲突”时有价值；阶段 4b 说明当前真实 probe 很少产生这种组合冲突。算法评价必须同时报告“机会是否出现”和“出现后是否修复”，不能只报告总体最优率。

### 6. Single-channel 对照

5 个窗口中，4 个 single-channel exact 对照在 100,000 状态内完成；另一个 1F1B 窗口因串行排列状态爆炸记录为 `state_limit`，没有把超限当作数值结果。4 个可解窗口中：

- 多资源 OPT 分别为 6、4、6、4；
- single-channel OPT 分别为 10、8、10、8；
- single-channel 平均高估 **83.33%**。

这个窗口级结果比阶段 4a 的 14.88% 平均高估更强，原因是窗口专门位于并行 ready wave。它进一步证明：后续不能在单 channel 上设计出 priority，再直接把相对收益解释成真实拓扑收益。

### 7. Placement 探索

额外对 1F1B 检查了四种 8-GPU rank 排列：contiguous、跨服务器交错、TP group 分裂式排列和另一组 cross permutation。四者在本轮 probe 上都得到相同的 `0.810%` ready-pair conflict 和 2 个 disagreement ticks。AlibabaHPN 的对称路径与 Ring collective 的分步端点配对使这些简单 permutation 没有形成新的 ready-route overlap。

这只是诊断，不应外推成“placement 不重要”。要产生可信的 placement 对照，需要使用论文/生产 placement、完整 16/32 GPU 并行配置和真实 AICB phase timing，而不是继续手工试 permutation。

### 8. 对下一步的修正

本阶段结果意味着不应立刻在当前 homogeneous single-job probe 上继续调资源 bonus，因为可利用冲突太少。下一步优先级应改为：

1. **接入真实 GPT AICB profile。** 使用已经验证的 GPT-7B/13B 配置，保留原始微秒 duration，检查 phase timing 是否让 PP/TP/DP 真正同时 ready；
2. **提取 executor endogenous contention window。** 不只沿离散 exclusive model 的 schedule，而从 `AnalyticalExecutor` trace 中找实际 active flows 共享 link/NIC 且不同 policy 会改变完成顺序的时段；
3. **从独占集合扩展到容量分配动作。** 当 flow 可以 max-min 共享链路时，动作不只是 compatible set，而是 ready/active flow 的 bandwidth allocation；小窗口 Oracle 可离散化 allocation level 或使用 time-expanded LP/CP-SAT；
4. **研究多 job contention。** 单 job collective wave 天然规整，多 job 相位错开更可能在 core/uplink 产生非模板化冲突；但必须保证不同 job 使用合法、不重叠的 GPU placement；
5. **修复/确认 ZB 的 B-W DAG 解耦后再评价 deferred-W。** 当前 ZB 路径没有 route conflict，既可能来自天然并行，也可能与 W 依赖建模限制调度自由度有关，不能据此否定 ZB-specific heuristic；
6. 只有当真实 hard windows 出现后，再比较 Dynamic-tail、resource-delay sensitivity、backbone/deferred deadline 和 Set rollout 的 gap closed。

### 9. 产物与复现

- 真实 route effective-window 脚本：`scripts/study_llm_route_windows.py`
- route/NIC resource adapter：`scripts/study_multiresource_dag.py::route_resource_sets`
- 回归测试：`tests/test_study_llm_route_windows.py`、`tests/test_study_multiresource_dag.py`
- 正式报告：`outputs/llm_route_windows/report.json`
- link-only 敏感性报告：`outputs/llm_route_windows/report_link_only.json`
- 复现命令：

```bash
python scripts/study_llm_route_windows.py \
  --target-per-mode 4 --quantum-us 25 --max-states 100000

python scripts/study_llm_route_windows.py \
  --target-per-mode 4 --quantum-us 25 --max-states 100000 --link-only \
  --output outputs/llm_route_windows/report_link_only.json
```

本阶段结果没有修改通用 `Task` schema、pipeline builder、serializer、baseline strategy 或 executor。

阶段 1--4b 联合定向回归为 `26 passed`，只读 syntax 检查和 `git diff --check` 通过。全量回归为 `831 passed, 3 skipped, 18 errors`；18 个 error 仍全部来自缺失的外部 Spectrum-X topology fixture，没有新增功能失败。当前环境仍未安装 `ruff`。

## 阶段 4c：手工小拓扑与可控冲突实验（2026-08-14）

### 1. 为什么补做这一阶段

阶段 4b 使用 16-GPU AlibabaHPN，而 probe 只占 8 张 GPU，且默认 placement 大量通信位于同一 server 的近无阻塞路径。五种 pipeline 的 ready-pair route conflict 大多为 0%，因此“拓扑 heuristic 没有收益”很可能只是缺少调度机会。

本阶段保留真实 pipeline builder、serializer 和 effective DAG，只把 topology 改成三个透明、可手算的 8-GPU 网络，并显式控制哪个并行维度跨越瓶颈。目标不是模拟某台真实机器，而是做机制实验：如果冲突变强，Dynamic-tail、resource-aware packing 和 set rollout 是否开始分化。

### 2. 三个手工拓扑

#### `single_switch`

8 个 GPU 各通过独立双向 leaf link 连接一个交换机。不同端点 flow 不共享 route edge，是阶段 4b 近无阻塞情况的最小对照。

```text
GPU0 ... GPU7
  \       /
   switch8
```

#### `two_rack`

GPU 0--3 和 GPU 4--7 分属两个 rack，rack switch 之间只有一对 100 Gbps 有向 uplink。所有同方向跨 rack flow 共享 `(8,9)` 或 `(9,8)`。

```text
GPU0--3 -- switch8 == shared uplink == switch9 -- GPU4--7
```

#### `four_rack_core`

每个 rack 有 2 个 GPU，4 个 rack switch 连接同一 core。来自同一 rack 的跨 rack flow 会共享 access-to-core link，适合制造部分重叠而不是把所有通信压成一个 channel。

```text
GPU0,1--S8  \
GPU2,3--S9   \
              core12
GPU4,5--S10  /
GPU6,7--S11 /
```

三个拓扑都继续使用有向 route links 加独立 `NIC-TX/NIC-RX` 资源，不把交换机节点本身粗暴设为容量 1。

### 3. 三种 placement

逻辑 rank 顺序仍为 `[PP][DP][TP]`，每组均使用同样的 8 张 GPU：

| placement | assigned nodes | 目的 |
|---|---|---|
| `pp_cross` | `0,1,2,3,4,5,6,7` | stage 0/1 位于不同 rack，使 PP 跨瓶颈 |
| `dp_cross` | `0,1,4,5,2,3,6,7` | 每个 stage 的 DP replica 分居 rack |
| `tp_cross` | `0,4,1,5,2,6,3,7` | 每个 TP pair 跨 rack，故意制造高冲突 |

正式矩阵使用 `ga=2,layers=2,quantum=25 us`，研究 1F1B 与 Bidirectional，共 `3 topology × 3 placement × 2 pipeline = 18` 个完整场景。

### 4. 冲突确实被制造出来了

18 个场景的平均 ready-pair conflict fraction 为 **15.15%**，其中 15/18 出现至少一次策略动作分歧。相比阶段 4b 的 0--3.4%，手工小拓扑显著扩大了可调度空间。

1F1B 的代表性结果：

| topology / placement | conflict fraction | disagreement ticks | Dynamic-tail | Resource-tail | Bottleneck-first | LPT |
|---|---:|---:|---:|---:|---:|---:|
| single-switch / 任意 | 0% | 0 | 758 | 758 | 758 | 758 |
| two-rack / PP-cross | 29.2% | 12 | **766** | 768 | 768 | 769 |
| two-rack / DP-cross | 7.6% | 6 | 762 | 762 | 762 | 762 |
| two-rack / TP-cross | 21.1% | 2 | 771 | 771 | 771 | 772 |
| four-rack / TP-cross | 25.7% | 16 | 765 | 765 | **764** | 773 |

Bidirectional 对冲突更加敏感。最强场景 `four-rack-core + TP-cross` 为：

```text
Dynamic-tail       782
Resource-tail      780
Bottleneck-first   777
LPT                787
```

在 18 个完整场景中，有 8 个场景的 Dynamic-tail 不是四种快速 heuristic 中的 best observed。最大快速改进发生在上述 Bidirectional 场景：Bottleneck-first 相对 Dynamic-tail 减少 5 tick，即约 **0.64%**。

### 5. 资源优先不是普遍改进

小拓扑同时产生了正例和反例：

- four-rack TP-cross 中，优先清理热点资源有收益；
- two-rack PP-cross 1F1B 中，Dynamic-tail=766，而 Resource-tail/Bottleneck-first=768，反而退化 2 tick；
- two-rack TP-cross Bidirectional 中，Dynamic-tail=780，而两个资源策略均为782。

因此正确结论不是“拓扑小了以后 Bottleneck-first 更好”，而是：

> route load 是有效信息，但不能脱离 DAG unlock/tail 独立排序；需要端到端集合评价来判断当前是应保护关键链，还是先释放共享瓶颈。

### 6. 完整图 Set Rollout

对冲突最强的 `four-rack-core + TP-cross` 单独运行昂贵的完整图 rollout：

| pipeline | LB | Dynamic-tail | Bottleneck-first | Rollout-2 | **Rollout-4** |
|---|---:|---:|---:|---:|---:|
| 1F1B，304 tasks | 758 | 765 | 764 | 765 | **762** |
| Bidirectional，400 tasks | 768 | 782 | 777 | 779 | **775** |

这给出了本阶段最明确的算法进步：

- 1F1B 中 Rollout-4 比 Dynamic-tail 减少 3 tick（0.39%），并把相对 LB gap 从 7 降到 4；
- Bidirectional 中 Rollout-4 减少 7 tick（0.90%），把相对 LB gap 从 14 降到 7；
- Rollout-2 在两个场景中都明显弱于 Rollout-4，阶段 4a/4b 的“top-2 已够”不能外推到高冲突拓扑；
- 这些仍是更好的可行解，不是 exact full-DAG OPT，因为当前 LB 尚未闭合。

代价也非常明显。快速 heuristic 的 Python 原型约 0.8--1.3 秒；完整 Rollout-4 约 36--42 秒，慢约两个数量级。因此 Rollout-4 当前适合作为离线 teacher/upper-bound 搜索，不适合作为在线默认策略。

### 7. Exact 小窗口结果及其限制

从18个完整场景中提取了43个 depth-0 exact 窗口，其中10个能区分至少一种算法：

| method | mean ratio | observed max | optimal fraction |
|---|---:|---:|---:|
| Dynamic-tail / Resource-tail / Bottleneck-first | 1.00000 | 1.00000 | 100% |
| SPT | 1.00465 | 1.20000 | 97.67% |
| LPT | 1.04208 | 1.33333 | 79.07% |
| Set Rollout-2 | 1.00000 | 1.00000 | 100% |

这些窗口证明简单 LPT/SPT 会在局部冲突上犯错，但没有解释完整图中 Resource/Bottleneck/Rollout 的差异。对最强 Bidirectional 场景继续尝试 depth-1/2，strong methods 仍全部命中窗口 OPT。

原因是完整图的 1--7 tick 差异来自多个相隔较远的选择累积，而 boundary tail 把后续复杂状态压成一条 compute proxy。由此得到一个方法论修正：

> 对拓扑冲突，单个 depth-0 decision window 可以检查动作合法性和短期错误，但不足以评价跨多个 collective wave 的长期资源占用；下一步需要 multi-event window、保留资源 demand profile 的 boundary state，或完整图的更强 lower bound/branch-and-bound certificate。

### 8. 当前结论

用户提出的“先手工构造小拓扑”已经得到肯定答案：

1. 原 AlibabaHPN probe 的冲突确实偏少，不能充分区分 topology heuristic；
2. 加入可解释的共享 uplink/core 后，冲突从接近 0 提升到最高约29%；
3. Resource-aware heuristic 开始产生正收益，但也存在明确退化反例；
4. Set Rollout-4 在两个完整 LLM-structured DAG 上均优于 Dynamic-tail 和 Rollout-2；
5. 改进幅度目前小于1%，但它是首次在真实 builder/effective DAG 加手工 route bottleneck 上观察到的稳定方向；
6. 下一步应优化候选集合生成和 lookahead，而不是简单扩大 `k` 后直接上线。

### 9. 下一步

1. 候选集合由 `Dynamic-tail top-k` 改成多样化集合：至少保留 Dynamic、Bottleneck、PP/DP deadline 和一组最大资源互补集合；
2. 用两阶段 rollout：第一层比较集合，第二层只在分歧资源上展开，争取接近 Rollout-4 质量但把开销降到毫秒级；
3. 为完整图构造更强的 per-resource release/deadline demand bound，判断762/775距离 OPT 还有多少；
4. 将窗口 boundary 从单一 tail 改为 `(critical tail, per-resource future demand, next collective release)`；
5. 最后用真实 GPT AICB + oversubscribed topology 验证手工拓扑上发现的收益是否保留。

### 10. 产物与复现

- 手工拓扑、placement、矩阵与 focused rollout：`scripts/study_small_topology_sensitivity.py`
- 拓扑语义测试：`tests/test_study_small_topology_sensitivity.py`
- 正式矩阵：`outputs/small_topology_sensitivity/report.json`
- focused 结果：`outputs/small_topology_sensitivity/focused_1f1b.json`、`focused_bidirectional.json`
- 复现命令：

```bash
python scripts/study_small_topology_sensitivity.py \
  --modes 1f1b bidirectional --target-per-scenario 3

python scripts/study_small_topology_sensitivity.py \
  --focused-only --modes 1f1b \
  --output outputs/small_topology_sensitivity/focused_1f1b.json

python scripts/study_small_topology_sensitivity.py \
  --focused-only --modes bidirectional \
  --output outputs/small_topology_sensitivity/focused_bidirectional.json
```

阶段 4c 拓扑相关定向回归为 `15 passed`，只读 syntax 检查与 `git diff --check` 通过。全量回归为 `835 passed, 3 skipped, 18 errors`；18 个 error 仍全部来自缺失的外部 Spectrum-X topology fixture，与本阶段无关。当前环境未安装 `ruff`。

## 六、阶段 4：利用 LLM DAG 的特殊结构（2026-08-15）

### 1. 研究方法：语义用于生成候选，不直接叠加 bonus

阶段 3 已证明 raw join bonus 会重复奖励，阶段 4c 又证明单独的 Bottleneck-first 有正例也有反例。因此本阶段不构造统一线性分数，而是为同一个 residual state 生成少量、语义不同但都合法的 maximal compatible sets：

```text
Dynamic-tail / Bottleneck / Resource-complement
Backbone-first / Optimizer-deadline / Deferred gap-fill
Dimension round-robin / DP replica wavefront / Ring chunk wavefront
```

每个候选仍使用统一的端到端评价：执行到 next event，再用 Dynamic-tail 补全剩余 schedule。完整 Dynamic-tail schedule 保留为 incumbent。LLM 特征只负责回答“还值得评估哪些动作”，最终选择仍由预测 makespan 决定，不会因相关特征重复加分。

### 2. Analyzer-owned LLM sidecar

没有修改通用 `Task` schema。研究 sidecar 为每条 flow 保存：

- PP/TP/DP/EP dimension 和 forward/backward phase；
- iteration、layer、item、stage；
- Ring `chunk_id/num_chunks`；
- 物理 src/dst/route；
- 根据 job 的 `[PP][DP][TP]` assigned-node 顺序恢复的逻辑坐标。

当前角色定义为：

```text
backbone = PP/TP flow 且 phase 属于 forward/backward_input
deferred = DP flow 或 backward_weight flow
```

Optimizer slack 的首版近似为：

\[
slack(v)=LB_{residual}-\bigl(p_v+tail_v\bigr).
\]

它比 raw join duration 更接近“还能推迟多久”，但仍不是严格 latest-start time，因为没有显式求 optimizer deadline 和未来 bandwidth waiting。

### 3. Teacher 覆盖率

在阶段 4c 最强的 `four-rack-core + TP-cross` 场景中，对每个 conflict event 枚举全部 maximal compatible sets，执行一次 next-event 后用 Dynamic-tail 补全，得到 exhaustive one-event teacher。teacher 是确定的反事实上界选择器，不是 full-DAG exact OPT。

| pipeline | choice events | mean exhaustive sets | mean semantic sets | value coverage | action coverage |
|---|---:|---:|---:|---:|---:|
| 1F1B | 26 | 4.77 | 1.81 | **100%** | **100%** |
| Bidirectional | 40 | 6.70 | 2.10 | **100%** | **100%** |

语义候选把每次需要评价的集合数减少约62%和69%，同时覆盖全部 teacher-optimal value/action。

第一版 sidecar 只有 phase/stage/layer，1F1B 覆盖率为96.15%。唯一漏掉的 event 是四条同模板 TP flow：teacher 选择 DP replica 0，而 Dynamic-tail 因1 tick tail差异选择 replica 1。加入逻辑 DP 坐标后的 `replica_wavefront` 将覆盖率补到100%。

Bidirectional 第一版覆盖率为97.5%。唯一漏掉的两条 flow 连 endpoint、route、phase、stage、replica 都相同，只差 Ring `chunk_id`；teacher 选择 chunk 1，Dynamic-tail 选择 chunk 0。加入 `chunk_wavefront` 后也达到100%。这说明并行坐标和collective内部结构具有实际信息量，不只是给已有tail换名字。

### 4. 完整 DAG 结果与消融

使用 `ga=2,layers=2,quantum=25 us` 的真实 builder/effective DAG，拓扑为 four-rack shared core，placement 为 TP-cross。

`Topology-only rollout` 只允许 Dynamic、Bottleneck、Resource-complement；`Semantic rollout` 再加入 backbone、deferred/deadline、dimension、replica 和 chunk 候选。

| pipeline | LB | Dynamic | Bottleneck | Topology-only | Semantic | 先前 Rollout-4 |
|---|---:|---:|---:|---:|---:|---:|
| 1F1B | 758 | 765 | 764 | 764 | **762** | 762 |
| Bidirectional | 768 | 782 | 777 | 776 | **773** | 775 |

结论：

1. 1F1B 中 topology-only 只关闭1/7的LB gap；加入LLM语义后关闭3/7并追平Rollout-4；
2. Bidirectional 中 topology-only 从782降到776，语义候选继续降到773，比Rollout-4还好2 tick；
3. 相对Dynamic-tail，Semantic rollout分别改善3 tick（0.39%）和9 tick（1.15%）；
4. 相对相同counterfactual框架的Topology-only，LLM语义的独立边际收益为2 tick和3 tick；
5. 这些是更好的可行解，不是OPT；最终相对下界gap仍为4和5。

1F1B 的最终动作使用为 Dynamic 73次、Replica-wavefront 1次、Bottleneck 2次、Chunk-wavefront 1次。Bidirectional 为 Dynamic 88次、Replica-wavefront 1次、Dimension-round-robin 1次、Chunk-wavefront 3次、Deferred-gap-fill 3次。

收益不是“所有LLM规则频繁介入”，而是绝大多数event沿用Dynamic-tail，仅在少数对称replica/chunk或跨维度冲突点引入替代集合。

### 5. 特征证据边界

已有直接证据：

- DP replica wavefront 修复1F1B唯一teacher miss，并在完整schedule实际被选择；
- Ring chunk wavefront 修复Bidirectional唯一teacher miss，并被选择3次；
- Dimension round-robin 在Bidirectional被选择1次；
- 语义候选组合相对Topology-only获得2/3 tick独立收益。

尚无独立makespan贡献证明：

- Optimizer-deadline 在teacher中命中少量event，但高冲突完整schedule没有选择；
- Deferred gap-fill 在Bidirectional teacher命中7个event且完整schedule选择3次，但还缺少移除该候选的单特征消融；
- Backbone-first 大多与Dynamic-tail生成相同集合，没有观察到独立候选增量。

因此不能宣称 backbone/deferred 策略已经完成；下一步需要 leave-one-feature-out 消融和 W/DP overlap 更强的DAG。

### 6. 周期性结果

按不含microbatch编号的 `(dimension, phase, stage, layer)` 压缩ready frontier：

| pipeline | unique templates | repeated templates | action consistency |
|---|---:|---:|---:|
| 1F1B | 9 | 7 | 82.86% |
| Bidirectional | 27 | 8 | 68.75% |

存在明显重复，但同一粗模板不能唯一决定动作，差异来自replica/chunk位置、residual duration和resource load。因此周期表只能作为候选或缓存key；更合理的key应加入 `(replica wave, chunk progress, bottleneck-load bucket)`。

### 7. 开销与部署定位

高冲突场景中快速贪心约0.6--1.8秒，语义Rollout约28.7秒和31.8秒，先前Rollout-4约41.7秒和36.2秒。语义候选减少了集合数，但Python原型仍反复完整补全schedule，尚不适合在线。

当前价值是离线teacher和策略发现。下一步应缓存周期状态的continuation value，并只在 `unique_candidates > 1` 且存在真实资源冲突时调用lookahead。

### 8. Zero Bubble 暂不纳入性能结论

当前ZB effective DAG仍保留 `B -> W` 依赖，W没有在DAG层面真正脱离B关键链。若直接实验，候选生成器看不到真实ZB允许的W滞后自由度。应先在隔离研究路径建立 `F -> B`、`F -> W`、无 `B -> W` 的语义版本，再验证deferred-W，不能由serializer顺序代替因果依赖。

### 9. 当前推荐原型

```text
Dynamic-tail packed incumbent
  + topology candidates: bottleneck / resource-complement
  + LLM candidates:
      backbone / deferred-deadline / dimension
      replica-wavefront / chunk-wavefront
  + next-event end-to-end counterfactual
  + periodic continuation cache（待实现）
```

关键变化是：调度器选择兼容flow集合，LLM结构只扩展少量候选；不把全部特征相加成一个priority。

### 10. 下一步

1. 对optimizer-deadline、deferred-gap、replica、chunk做leave-one-out完整消融；
2. 将粗slack替换为optimizer latest-start和join后resource-aware critical tail；
3. 设计 `(template, replica, chunk, load bucket)` 周期缓存，减少完整补全次数；
4. 用更强per-resource release/deadline lower bound认证762/773的剩余gap；
5. 建立隔离的ZB B/W解耦研究DAG，再验证deferred-W；
6. 最后接入真实GPT AICB profile，检查收益能否跨profile保留。

### 11. 产物与复现

- LLM sidecar、候选、teacher覆盖与语义rollout：`scripts/study_llm_structured_candidates.py`
- route-aware sidecar扩展：`scripts/study_llm_route_windows.py`
- 回归测试：`tests/test_study_llm_structured_candidates.py`
- 正式高冲突报告：`outputs/llm_structured_candidates/report_high_conflict.json`
- 完整正/负对照报告：`outputs/llm_structured_candidates/report.json`
- 复现命令：

```bash
python scripts/study_llm_structured_candidates.py \
  --modes 1f1b bidirectional \
  --topologies four_rack_core \
  --output outputs/llm_structured_candidates/report_high_conflict.json
```

阶段4相关定向回归为 `18 passed`，只读 syntax 检查与 `git diff --check` 通过。全量回归为 `838 passed, 3 skipped, 18 errors`；18 个 error 仍全部来自缺失的外部 Spectrum-X topology fixture，与本阶段无关。当前环境未安装 `ruff`。

## 七、阶段 4 收尾：压缩搜索、安全分区与隔离 ZB（2026-08-09）

### 1. 更强的多资源下界

在原有 `max(critical path, max per-resource load)` 上加入了逐资源 release/deadline demand bound。对每条 route resource，使用 precedence-only earliest release 和 residual downstream tail 构造必要时间窗；若某个区间内必须经过该资源的通信总量大于区间长度，则候选 horizon 不可行。不同资源独立检查，因此它仍是合法下界，不假设多跳 flow 可以分拆执行。

该下界使用单调二分搜索，只定位为离线认证/搜索剪枝工具。新测试在一个“compute release 后两条共享链路 flow、随后各有 tail”的例子上得到：

```text
critical path = 11
max resource load = 6
resource-window LB = OPT = 14
```

在高冲突 ZB probe 原图上，它也把 `critical_path=726` 收紧到 `combined=730`，而 Dynamic-tail 为733，未认证 gap 从7缩到3。

### 2. Frontier-state 与周期缓存

阶段4的 semantic candidate portfolio 本身就是一种压缩 frontier search：不枚举全部 maximal compatible sets，只保留 Dynamic、拓扑瓶颈、backbone/deferred、dimension、replica 和 chunk 等少量代表集合，再做 next-event counterfactual。

新增两级周期 key：

```text
detailed = (template, replica, TP position, chunk, route width,
            residual-duration bucket, bottleneck-load bucket)
coarse   = (template, replica, TP position, chunk, route width)
```

缓存只复用“候选标签”，每次仍重新生成当前状态下的合法 compatible set；完整 Dynamic-tail schedule 始终作为最终 incumbent，因此 key alias 不会让返回结果差于 Dynamic-tail。

| pipeline | key | hits / decisions | makespan | runtime |
|---|---|---:|---:|---:|
| 1F1B | detailed | 0 / 77 | 762 | 13.17 s |
| 1F1B | coarse | 30 / 77 | 762 | 8.79 s |
| Bidirectional | detailed | 1 / 96 | 773 | 18.58 s |
| Bidirectional | coarse | 10 / 96 | 773 | 17.04 s |

结论是周期复用确实可用，但 load/remaining 放进 key 会使状态几乎不重复；去掉它们后，1F1B 的命中率约39%、原型时间下降约33%，Bidirectional 只有约10%命中和8%左右降时。周期策略不能脱离 pipeline 模式单独宣称有效。

### 3. Leave-one-feature-out 结果

在 `four-rack-core + TP-cross, ga=2, layers=2` 上共享 continuation cache，逐个移除语义候选族：

| removed feature | 1F1B | Bidirectional |
|---|---:|---:|
| none | **762** | **773** |
| backbone | 762 | 773 |
| optimizer deadline | 762 | 773 |
| deferred gap-fill | 762 | 773 |
| dimension round-robin | 762 | 775 |
| replica wavefront | 764 | 776 |
| chunk wavefront | 762 | 773 |

因此当前可归因的独立贡献只有：1F1B 的 replica wavefront（2 tick），以及 Bidirectional 的 replica wavefront（3 tick）和 dimension round-robin（2 tick）。Deferred-gap 和 chunk 候选虽然在完整轨迹中被选择过，但移除后有替代候选得到相同端到端结果；不能把“被选择次数”解释成独立收益。Backbone 和 optimizer-deadline 当前也没有独立证据。

### 4. 安全 DAG 分区

实现了最保守的 strict-series partition：只有当一个节点与 DAG 中所有其他节点都存在明确先后关系时，才把它当作全局 barrier 切分。该条件保证 barrier 两侧不能重叠，局部最优解才可以严格串联。

38 个完整 topology/effective-DAG 参数点中，`exact_composition_safe` 全部为 false。也就是说，当前单 iteration LLM DAG 中没有可用于“大量小问题独立求解再拼接”的非平凡全局切口。Dominator/SESE region 仍可用于压缩状态，但边界必须携带 Pareto demand profile，不能输出一个局部 schedule 后直接拼接。这个负结果支持前面“动态 frontier 而非硬切 DAG”的路线修正。

### 5. 隔离的真实 ZB B/W 语义

没有修改生产 builder。研究脚本复制 Zero Bubble effective DAG，并按同一 `(node, iteration, layer, item)`：

1. 删除 W 对同一 B compute 及其 backward-input collective completion 的直接依赖；
2. 加入对应 `F -> W`；
3. 保留 W 后的 DP 和 optimizer gating。

本例改变48个W节点，删除80条B/IG-result到W的边，加入48条F到W的边。结果为：

| DAG | LB | Dynamic-tail | Bottleneck-first |
|---|---:|---:|---:|
| 当前 builder ZB | 730 | 733 | 732 |
| 隔离 B/W 解耦 | 726 | 729 | 728 |

解耦使 critical chain 和两个可行 schedule 都缩短4 tick，证明原 `B -> W` 确实会改变可调度空间；但 heuristic 相对 LB 的 gap 没有改善，当前实例仍没有证明 deferred-W 规则优于一般 bottleneck 策略。它只能作为语义对照，不能替代正式修复 builder 后的 executor 验证。

## 八、阶段 5：统一评测（2026-08-09）

### 1. 评测范围

统一入口 `scripts/study_heuristic_plan_completion.py` 汇总：

- 30个随机并行链 exact/伪多项式 DP；
- 53个一般小 DAG（对抗、LLM motif、真实缩减和30个随机 join DAG）的 exact oracle；
- 38个完整 route-aware effective DAG 参数点，覆盖1F1B、当前ZB、Interleaved、Bidirectional、DualPipe，single-switch/four-rack-core，不同GA、层数、量化及通信缩放；
- 两种高冲突 pipeline 的语义消融和周期缓存；
- 真实 GPT-13B AICB + 16-GPU AlibabaHPN 的 route-aware 缩减。

完整拓扑模型报告 makespan、相对LB、network idle和Python调度时间。当前研究模型把 compute 当作 precedence-only 并行任务，未建模物理GPU互斥、activation memory和max-min bandwidth sharing，所以没有伪造“GPU idle、activation violation、真实抢占次数”这三个指标；这些只能在生产 executor 集成后测量。

### 2. 单通道 exact 结果

30个随机并行链中：Longest-tail、Rollout-2/4、Beam-8/32和MC-64本批样本均命中OPT；LRPT/earliest-slack observed max为1.05。FIFO、SPT、LPT、Longest-delay/TicTac的observed max分别为1.5714、1.3667、1.4762和1.35。该结果只是有限样本表现；先前构造的9/8 Longest-tail族和趋近2的通用work-conserving反例仍然有效，不能由本批“全部最优”推出更强近似比。

53个一般 DAG 中：

| method | mean ratio | observed max | optimal fraction | mean runtime |
|---|---:|---:|---:|---:|
| Longest-tail | 1.00411 | 1.125 | 94.34% | 0.64 ms |
| Dynamic-tail | **1.00176** | **1.04762** | 96.23% | 1.05 ms |
| Gate-dynamic-tail | 1.00270 | 1.05 | 94.34% | 1.00 ms |
| Rollout-2/4/8 | 1.00000 | 1.00000 | 100% | 12.2--14.0 ms |
| Beam-8 | 1.00000 | 1.00000 | 100% | 111.9 ms |

Raw gate 特征再次略微伤害平均值和最优率；Dynamic-tail 是最有价值的低成本基线。Rollout 在该有限 suite 上最优，但没有小于2的最坏界证明，且开销高一个数量级以上。

### 3. 按 P/Q 分层的完整拓扑结果

这里 `P=max per-resource route load`，`Q=precedence path 上的纯 compute load`。为覆盖通信主导区域，额外对通信 duration 做8/16/32倍 profile scaling；这些点是敏感性实验，不是实测带宽。

| stratum | scenarios | Dynamic | Resource-tail | Bottleneck | SPT | LPT |
|---|---:|---:|---:|---:|---:|---:|
| compute dominated | 33 | 1.01026 | 1.01018 | **1.00471** | 1.01121 | 1.01786 |
| balanced `P≈Q` | 4 | 1.10066 | **1.09601** | 1.14214 | 1.17130 | 1.56870 |
| communication dominated | 1 | 1.23888 | **1.23211** | 1.42408 | 1.30368 | 2.28723 |

表中是 mean `makespan/LB`，不是 approximation ratio，因为完整图没有OPT。主要结论：Bottleneck-first 在compute-dominated点表现最好，却在平衡/通信主导点明显退化；Resource-tail跨分层最稳定，Dynamic-tail非常接近；LPT在通信主导点极差。LLM-specific semantic rollout的0.39%/1.15%收益只在两个高冲突点验证，尚未达到“多组参数稳定提升”的退出条件。

### 4. 真实 GPT-13B AICB profile

新增真实 profile 入口使用未修改的：

```text
A100 GPT-13B, world=16, TP=8, PP=2, DP=1, GA=8
AlibabaHPN 16-GPU topology, BFS routes
```

完整 effective DAG 有48,416个任务、130,992条有效边和39,776条flow，其中TP 39,648、PP 128；全部flow route为2 hop。原 dominance 审计在该规模上因平方空间触发 `MemoryError`，因此真实入口改用线性的 data-edge + serializer-edge exporter，只跳过全量dominance集合，不改变有效依赖。

从最密集的3个时间桶各保留2条seed flow，使用100 ms量化做exact缩减。三个窗口分别有94/96/96个任务；DP在5万状态内没有完成枚举，但三种可行heuristic均得到56，且新的resource-window LB也为56，因此三个窗口都由“可行解=下界”直接认证最优。它们没有区分Dynamic-tail、Resource-tail和Bottleneck-first。

这说明真实AICB profile已完成结构、route、缩减和certificate闭环，但当前配置 `DP=1`、TP占99.7%的flow，不能验证deferred DP或多维重叠流量。下一轮真实实验应选择或生成 `TP/DP/PP` 同时非1的AICB，而不是从这个负结果推出heuristic无效。

### 5. 最终判断与退出条件

规划中的研究基础设施已经实现完毕，但“研究计划实现完成”不等于“算法已满足上线条件”：

1. 单通道2-近似安全底座、exact oracle、反例、一般DAG rollout、多资源模型、LLM候选压缩、周期缓存、安全分区和统一评测均已闭环；
2. 没有得到多资源一般DAG的常数近似保证；`makespan/LB`不能冒充近似比；
3. LLM特化在1F1B和Bidirectional各有正收益，但只验证一个高冲突参数点，未满足“多组参数稳定改进”；
4. 离线semantic rollout即使有缓存仍需8.8--17.0秒，远高于贪心的亚秒到约2秒，未满足“收益大于运行时开销”；
5. 真实GPT AICB缩减闭环完成，但该profile没有DP维度且窗口内算法无差异；
6. 因而当前推荐仍是：生产候选为增量Dynamic/Resource-tail；semantic rollout作为离线teacher。暂不接入executor默认策略，也不宣称优于2的理论保证。

### 6. 产物与复现

- 阶段4/5统一入口、安全分区、ZB隔离和真实AICB缩减：`scripts/study_heuristic_plan_completion.py`
- 多资源window lower bound：`scripts/study_multiresource_dag.py`
- 真实AICB route adapter：`scripts/study_llm_route_windows.py`
- 周期缓存与特征消融：`scripts/study_llm_structured_candidates.py`
- 正式综合报告：`outputs/heuristic_plan_completion/report.json`
- 真实GPT报告：`outputs/heuristic_plan_completion/real_gpt_aicb.json`
- 新增回归：`tests/test_study_heuristic_plan_completion.py`及三个对应研究脚本测试。

```bash
python scripts/study_heuristic_plan_completion.py \
  --samples 30 --include-expensive \
  --output outputs/heuristic_plan_completion/report.json

python scripts/study_heuristic_plan_completion.py \
  --real-aicb-only \
  --output outputs/heuristic_plan_completion/real_gpt_aicb.json
```

本轮相关定向回归为 `45 passed`，只读syntax检查与`git diff --check`通过。当时完整测试集为 `843 passed, 3 skipped, 18 errors`；后续阶段9加入测试后的最新总数见下文。18个error全部是仓库已知的外部Spectrum-X fixture缺失。本轮没有修改通用Task/schema/executor、baseline策略或高级流水线builder。

## 九、真实多维 AICB 与 max-min executor 验证（2026-08-09）

### 1. Workload 筛选与 DP 覆盖语义

扫描了 `inputs/aicb-workload` 下936份AICB。文件覆盖GPT-7B/13B/22B/175B、Mixtral和Llama，不同TP、PP、EP、GBS、MBS与GA；但按文件名和header计算，现有文件都满足：

```text
header_all_gpus = TP × PP
header_DP = 1
```

因此不能声称原始文件自带多维DP。本阶段沿用仓库Hermod/Puppeteer实验已经验证的语义：保持AICB中每rank的compute/communication profile不变，通过`dp_override`扩大`Job.parallelism.dp`和assigned nodes，使builder按新的DP group展开真实DP collective。

选取的主配置为：

```text
GPT-7B / GPT-13B / GPT-22B
TP=4, PP=2, DP override=2, GA=8
AlibabaHPN 16-GPU, contiguous / cyclic_pp_dp
```

扩展验证使用：

```text
GPT-13B, TP=4, PP=2, DP=2, GA=2/4/8
GPT-13B, TP=4, PP=2, DP=4, GA=4
Hermod 32-GPU topology
```

`build_route_aware_aicb`现支持`dp_override`、`contiguous/cyclic_pp_dp`和`gpus_per_server`，报告同时记录header DP和expanded DP，避免语义混淆。

### 2. 隔离的真实 executor heuristic

新增`scripts/study_real_aicb_executor_heuristics.py`。它不注册生产policy，而是复用：

- 真实AICB parser和WorkloadBuilder/高级pipeline builder；
- pipeline serializer产生的compute order；
- BFS route；
- `AnalyticalExecutor`；
- 真实链路容量；
- Hermod allocator同口径的逐tier progressive-filling max-min分配。

Default对所有active flow做普通fair-share；研究policy按照effective-DAG downstream tail构造严格优先tier，同一tail内继续max-min，低优先级只使用高优先级留下的链路容量。由于ready/active flow的所有后继必然尚未完成，其static downstream tail与该flow的residual downstream tail一致。Resource-tail额外维护所有未完成flow的per-link residual bytes；任务完成时增量扣减，不重扫完整DAG。

同一实验的route和compute order完全相同，只改变bandwidth allocation。报告记录：

- makespan；
- PP flow完成到直接下游compute启动的unlock delay；
- allocation calls与真实链路冲突calls；
- paused assignments和真实正带宽到零带宽的preemption；
- allocator/executor Python wall-clock。

早期遥测曾把正常完成flow离开active set误计为preemption，已经修正为“仍active且正带宽变零”才计数，后续正式数据使用修正口径。

### 3. DP=2、GA=8的跨模型结果

#### Bidirectional

| model | placement | conflict calls | Default (us) | Dynamic-tail (us) | improvement |
|---|---|---:|---:|---:|---:|
| GPT-7B | contiguous | 238 | 2,029,788 | 1,942,558 | **4.30%** |
| GPT-7B | cyclic | 332 | 2,048,330 | 2,023,624 | **1.21%** |
| GPT-13B | contiguous | 257 | 2,865,630 | 2,723,615 | **4.96%** |
| GPT-13B | cyclic | 299 | 2,888,844 | 2,851,646 | **1.29%** |
| GPT-22B | contiguous | 265 | 4,453,576 | 4,155,197 | **6.70%** |
| GPT-22B | cyclic | 308 | 4,431,851 | 4,358,662 | **1.65%** |

严格tail优先在3个模型、两种placement上全部为正。Contiguous收益更大；cyclic虽然制造更多冲突，但跨更慢链路的通信基线更长，能由调度关闭的比例反而较小。

这6个场景中Dynamic-tail、Resource-tail、Bottleneck-first和完整LLM tie最终makespan相同。当前证据支持的是“关键链严格优先优于全流fair-share”，不支持resource load或LLM语义在DP=2下有独立贡献。

#### 1F1B负对照

| model | placement | Default (us) | Dynamic-tail (us) | change |
|---|---|---:|---:|---:|
| GPT-7B | contiguous | 1,787,639 | 1,787,639 | 0 |
| GPT-7B | cyclic | 1,863,921 | 1,863,970 | -0.0026% |
| GPT-13B | contiguous | 2,431,784 | 2,431,784 | 0 |
| GPT-13B | cyclic | 2,551,447 | 2,551,795 | -0.0136% |
| GPT-22B | contiguous | 3,659,163 | 3,659,163 | 0 |
| GPT-22B | cyclic | 3,849,759 | 3,849,534 | +0.0058% |

1F1B的变化都接近零。其expanded workload只有32条DP flow，而Bidirectional因双副本梯度同步有224条DP flow；后者才形成足够长的共享链路竞争窗口。不能把Bidirectional收益外推到普通1F1B。

### 4. GA敏感性

GPT-13B、TP4×DP2×PP2、Bidirectional：

| GA | contiguous | cyclic |
|---:|---:|---:|
| 2 | **15.55%** | **4.12%** |
| 4 | **7.84%** | **2.03%** |
| 8 | **4.96%** | **1.29%** |

收益随GA增加下降，但6个参数点全部为正。解释是GA较小时固定DP/副本同步尾部占iteration比例更高，优先调度更容易缩短端到端尾部；GA增加后大量正常F/B计算稀释了同步收益。

### 5. DP=4与LLM replica特征

GPT-13B、GA=4、TP4×DP4×PP2在Hermod 32-GPU topology上有1,088条DP flow。Dynamic-tail结果：

| placement | Default | Dynamic | improvement |
|---|---:|---:|---:|
| contiguous | 8,268,837 | 7,712,679 | **6.73%** |
| cyclic | 15,295,405 | 13,874,545 | **9.29%** |

逐特征executor消融：

- dimension tie：与Dynamic完全相同；
- chunk tie：与Dynamic相同，cyclic只改变少量事件而不改变makespan；
- replica tie：contiguous得到7,566,512，相对Default改善**8.49%**，比Dynamic再缩短146,167 us；
- replica tie：cyclic得到13,894,348，比Dynamic慢19,803 us。

物理映射解释为：

```text
contiguous replica server order:
  stage0 [0,0,1,1], stage1 [2,2,3,3]  -> monotone

cyclic replica server order:
  stage0 [0,1,2,3], stage1 [1,2,3,0]  -> non-monotone
```

单纯加入route hop、resource width或residual load不能消除cyclic退化，因为发生分歧的对称replica在这些字段上仍相同。

因此实现了`guarded_replica_tie`：只有所有PP stage的DP replica server序列都单调时启用replica tie，否则自动退化为Dynamic-tail。验证结果：

| placement | Dynamic | raw replica | guarded replica |
|---|---:|---:|---:|
| contiguous | 7,712,679 | **7,566,512** | **7,566,512** |
| cyclic | **13,874,545** | 13,894,348 | **13,874,545** |

这是目前第一个在真实AICB、真实route和max-min executor中观察到独立收益的LLM-specific特征。但guard只验证一个模型/DP=4/拓扑组合，应表述为restricted placement rule，而不是通用最优规则。

### 6. Profile误差

只扰动调度器估计，executor继续使用原始AICB真实duration。

整类compute或communication统一缩放到80%/120%时，Dynamic-tail的动作和makespan完全不变。进一步对每个task使用3个独立固定seed的±20%乘性噪声：

| placement | nominal Dynamic improvement | noisy improvements |
|---|---:|---:|
| contiguous | 6.73% | 8.08%, 8.09%, 8.17% |
| cyclic | 9.29% | 9.23%, 9.27%, 9.55% |

端到端收益没有被噪声破坏，但控制开销明显恶化：nominal allocation calls约1,012/1,392、allocator时间约0.68/0.87秒；噪声后变成5,500--6,812次和4.4--5.2秒。原因是近似相同的tail被噪声拆成大量唯一tier，产生更多暂停和重分配事件。

尝试1 ms固定tail bucket没有降开销；100 ms bucket虽在部分噪声场景减少calls，却明显损害makespan：nominal contiguous收益从6.73%降到2.84%，cyclic从9.29%降到4.21%。固定分桶方案应否定。下一步更合理的是只在真实冲突集合变化时重算，或给priority变化设置hysteresis/minimum residency，而不是粗化tail本身。

### 7. 开销

DP=2的GPT-13B/22B中，Dynamic allocator平均每次调用约0.23--0.28 ms；DP=4 nominal约0.62--0.74 ms。Guarded replica在contiguous中因改变事件轨迹产生2,615次调用，总allocator时间约2.14秒，平均0.82 ms；cyclic退化为Dynamic后1,392次、约1.08秒，平均0.78 ms。

这是Python离线executor wall-clock，不等于真实训练控制面开销，但已经达到“单事件亚毫秒量级”的原型目标。需要继续降低调用次数，而不是进一步复杂化每次评分。

### 8. 当前结论

1. 真实AICB的DP覆盖、两种PP策略、两种拓扑规模和真实max-min allocator已经闭环；
2. Strict Dynamic-tail对Bidirectional在多模型、多GA、DP=2/4和两种placement下稳定优于Default fair-share；
3. 1F1B没有可测收益，因此尚未满足“至少两种PP策略稳定提升”的原退出条件；
4. Resource-tail在所有正式场景中都没有独立收益，暂不应增加在线复杂度；
5. Replica wavefront在DP=4 contiguous中有独立收益，placement monotonic guard能保留正例并规避已知cyclic反例；
6. Profile噪声没有消除makespan收益，却显著增加重分配开销；固定tail bucket不是解决办法；
7. 当前最值得继续的方向是Bidirectional专用的Dynamic-tail + guarded replica，并加入冲突触发和priority hysteresis；不是继续添加bonus。

### 9. 产物

- 真实executor研究policy、DP覆盖、strict progressive filling、guard和profile扰动：`scripts/study_real_aicb_executor_heuristics.py`
- allocator定向测试：`tests/test_study_real_aicb_executor_heuristics.py`
- DP=2跨模型Bidirectional：`outputs/real_aicb_executor_heuristics/gpt13b_22b_bidirectional_matrix.json`
- DP=2跨模型1F1B：`outputs/real_aicb_executor_heuristics/gpt13b_22b_1f1b_matrix.json`
- GA扫描：`outputs/real_aicb_executor_heuristics/gpt13b_bidirectional_ga_matrix.json`
- DP=4特征消融：`outputs/real_aicb_executor_heuristics/gpt13b_bidirectional_dp4_ablation.json`
- placement guard：`outputs/real_aicb_executor_heuristics/gpt13b_bidirectional_dp4_guarded.json`
- profile噪声：`outputs/real_aicb_executor_heuristics/gpt13b_bidirectional_dp4_profile_noise.json`
- 分桶负结果：`outputs/real_aicb_executor_heuristics/gpt13b_bidirectional_dp4_bucket100ms.json`

本阶段仍未修改通用Task/schema/executor、默认policy注册或baseline runner。

本阶段定向回归为`21 passed`。最新完整测试集为`845 passed, 3 skipped, 18 errors`；18个error仍全部来自缺失的外部`Spectrum-X_8g_8gps_400Gbps_H100` fixture，与本阶段无关。只读syntax检查和`git diff --check`通过。

---

## 修订阶段 R0：模型规范与语义回归（2026-08-09）

### 1. 本阶段目的

此前的 exact DP、rollout、Beam、多资源 Oracle 和真实 executor 实验允许通信在 tick/event 边界切换，部分路径还使用 max-min 带宽共享。用户进一步明确目标问题后，最终语义改为：

1. 通信和计算节点一旦开始，就必须连续执行到完成；
2. 单 channel 上采用极化分配，一次把全部带宽交给一条通信；
3. 调度器研究的是完整 flow 的开始顺序，不研究时间片或带宽比例；
4. channel 空闲时，即使已有 ready flow，也允许主动等待将来的 compute completion/release；
5. 节点完成后可以重新规划尚未开始的任务，但不能改变正在运行的节点。

因此 R0 不做算法优劣比较，先建立所有后续 Oracle 和 heuristic 必须共同遵守的语义基线。

### 2. 隔离实现

新增 `scripts/nonpreemptive_dag_model.py`，复用阶段 1 的只读 `BenchmarkDAG`/`BenchTask`，但没有复用旧 `_tick` 或可抢占 Oracle，也没有修改通用 `Task`、schema、executor 和 baseline policy。

状态对每个节点显式记录：

- `pending`：尚未开始；
- `running`：已经开始，并记录剩余时间与开始时刻；
- `completed`：已经完成，并记录开始/结束时刻；
- 当前绝对时间；
- channel 上的 active flow。

调度器只观察 channel 空闲的稳定决策状态。合法动作只有：

- `FLOW(i)`：若 flow `i` 已 ready，则从当前时刻开始，用完整 channel 连续运行到完成；
- `WAIT`：若至少有一个 active compute，则推进到最近的 compute completion。

`FLOW(i)` 的内部仍逐 compute-completion 处理依赖释放。例如长 flow 运行期间可以有 compute 完成，并使另一条 flow 变为 ready；但这个中间事件只更新状态，不开放调度决策，新 flow 必须等当前 flow 完成。这样既保留了精确的 DAG 因果关系，也不会偷偷恢复通信抢占。

所有无依赖或刚被解锁的 compute 都会立即启动；它们互相并行，也可以和通信重叠。compute 只减少 remaining time，不存在暂停动作。`WAIT` 只到下一个真实完成事件；没有 active compute 时不枚举 WAIT，避免无限原地等待。

时间线为每个已完成节点保存唯一的 `[start,end)` 区间。`assert_nonpreemptive_trace` 会检查：

- 区间长度严格为正；
- 同一个节点不能有两个区间；
- 每个 completed 节点必须恰有一个区间；
- 区间端点必须与 runtime 的 started/completed 时刻一致。

### 3. 三类手算 DAG

#### 3.1 长 flow 内发生新释放

```text
release: compute(2) ──> new_flow: comm(1) ──> compute_tail(3)

long_flow: comm(5)，t=0 ready
```

在 `t=0` 启动 `long_flow` 后，`release` 在 `t=2` 完成，`new_flow` 随即 ready；但 `long_flow` 仍连续占用 channel 到 `t=5`。随后 `new_flow` 在 `[5,6)` 运行，尾部 compute 在 `[6,9)` 运行，makespan 为 `9`。这同时验证：flow 不可抢占、compute/communication 可重叠、compute 完成可在 flow 内释放新任务、新任务等待 active flow 完成。

#### 3.2 连续主动等待

```text
release_1: compute(1) ──> flow_1: comm(1)
release_2: compute(3) ──> flow_2: comm(1)
ready_now: comm(4)，t=0 ready
```

调度器可在已有 `ready_now` 时先 WAIT：第一次从 `t=0` 等到 `t=1`，第二次仍在有 ready flow 时从 `t=1` 等到 `t=3`。两个 compute 都完成后不再存在未来 release，因此 WAIT 从合法动作集合消失。再按 `flow_1、flow_2、ready_now` 执行，makespan 为 `9`。这验证连续 WAIT 能跨多个 release，且不会生成无意义 WAIT。

#### 3.3 主动等待严格必要

取规划中的永久反例，令 `M=10`：

```text
A: comm(10)，t=0 ready，无后继
release_B: compute(1) ──> B: comm(1) ──> tail_B: compute(10)
```

Work-conserving 调度在 `t=0` 只能启动 A：

```text
A [0,10) -> B [10,11) -> tail_B [11,21)，makespan = 21
```

允许主动等待时：

```text
WAIT [0,1) -> B [1,2)
                 ├─ tail_B [2,12)
                 └─ A      [2,12)
makespan = 12
```

结果精确符合 `T_WC=2M+1=21`、`OPT=M+2=12`。随着 `M` 增大，两者比值趋近 `2`。这个差距不是 flow 排序 tie-break 造成的，而是“是否允许暂时不启动当前唯一 ready flow”造成的；因此 WAIT 必须进入后续精确 Oracle 的动作空间。

### 4. 语义回归覆盖

新增 `tests/test_nonpreemptive_dag_model.py`，共 6 个测试，覆盖：

1. active flow 不会被中途释放的 flow 抢占；
2. compute 一旦启动连续运行，并可与通信重叠；
3. 有 active compute 时可主动 WAIT，且允许连续 WAIT；
4. 没有未来 compute event 时 WAIT 非法；
5. 主动等待反例的 `21` 与 `12` 两条完整时间线；
6. task ID、依赖和 DAG 无环性不被运行过程修改，并拒绝有环或零时长输入。

R0 测试与旧阶段 1 benchmark/oracle 回归一起运行，结果为：

```text
11 passed
```

`git diff --check` 和人工 100 字符行宽检查通过。环境没有安装 `ruff`，因此 `python -m ruff check` 未执行；pytest 已成功导入并执行新增模块。一次额外 `py_compile` 因沙箱不允许写已有 `scripts/__pycache__` 而退出，这不影响上述测试结果。

完整 `pytest tests -q` 结果为 `851 passed, 3 skipped, 18 errors`。18 个 error 全部来自缺失的仓库外 `Spectrum-X_8g_8gps_400Gbps_H100` topology fixture，与 R0 修改无关；相较此前完整回归，新增的 6 个 R0 测试全部进入 passed 计数。

### 5. 当前结论与边界

R0 的退出条件已经满足：三个手算 DAG 的动作、事件、时间线和 makespan 已逐项核对；已完成节点均只有一个连续区间；主动等待和无意义等待的边界已经固定为回归测试。

但本阶段只是语义闭环，不是优化算法闭环。当前模块还没有搜索最优动作序列，也没有给 Dynamic-tail、rollout 或 Beam 产生新模型下的正式结果。旧的 preemptive/tick-level 数字仍保留为历史记录，但从本节开始不得再把它们当作目标模型的性能结论。下一阶段 R1 应在这个状态机上实现并交叉验证：

- 允许 WAIT 的 `OPT_optional_idle`；
- 有 ready flow 时禁止 WAIT 的 `OPT_work_conserving`；
- 最优动作和连续时间线重建；
- ordering regret 与 idle regret 的分离。

---

## 修订阶段 R1：Non-preemptive + Optional-idle Exact Oracle（2026-08-09）

### 1. 目标与产物

R1 在 R0 的不可抢占状态机上实现两种最优值：

- `OPT_optional_idle`：ready flow 存在时仍允许 `WAIT`；
- `OPT_work_conserving`：只在没有 ready flow 时允许 forced WAIT。

新增：

- `scripts/nonpreemptive_dag_oracle.py`：memoized residual-cost DP、独立 forward branch-and-bound、lower bound、动作重放与等待分类；
- `scripts/study_nonpreemptive_oracle.py`：固定种子正式矩阵与 JSON 报告；
- `tests/test_nonpreemptive_dag_oracle.py`：两套 Oracle、主动等待反例、catalog、随机 fork/join、remaining-time key 和 hard limit 回归；
- `tests/test_study_nonpreemptive_oracle.py`：正式 runner 的小矩阵 smoke test；
- `outputs/nonpreemptive_oracle/r1_summary.json`：175 个正式小图的完整动作、下界、状态数和运行时间。

仍未修改通用 `Task`、schema、executor、bandwidth allocator 或 baseline policy。

### 2. 精确状态与动作

Oracle 的 memoization key 是：

$$
K(s)=\big((status_v,remaining_v)\big)_{v\in V}.
$$

其中 pending/completed 节点的 remaining 为 0，running compute 必须保留真实 remaining。绝对时刻、历史 started/completed timestamp 不影响未来 residual cost，因此不进入 key；最优动作求出后，再从真实 `t=0` 状态用 R0 状态机重放，恢复绝对时间线。

这个 key 不能简化成“已完成 flow 集合”或“通信顺序前缀”。例如同一批 flow 完成后，一个 active compute 还剩 1 单位和还剩 5 单位，会改变下一次 release、WAIT 长度和最优选择，必须是不同状态。

在一个稳定决策状态上：

$$
F(s)=\min_{a\in A(s)}\{\Delta(s,a)+F(T(s,a))\}.
$$

- `FLOW(i)` 的 $\Delta=p_i$，直接运行完整 flow；
- `WAIT` 的 $\Delta$ 是最近 active compute completion 的 remaining；
- optional-idle 的 $A(s)$ 包含 ready flows 和合法 WAIT；
- work-conserving 在 ready set 非空时从 $A(s)$ 删除 WAIT。

如果两个动作产生完全相同的 successor key，只保留一个。这是按精确后继状态做的安全合并，不依赖不可靠的 role、microbatch 或 chain 标签。

### 3. 两套相互校验的搜索

#### 3.1 Memoized residual-cost DP

DP 对每个 key 缓存精确的 residual makespan 和完整动作后缀。每个状态先用 FIFO、Longest-tail 以及 optional-idle 下的 wait-first 变体构造可行后缀上界 $U(s)$，然后只剪掉满足：

$$
\Delta(s,a)+LB(T(s,a))\ge U(s)
$$

的动作。因为 $U(s)$ 是真实可行调度，$LB$ 是必要下界，被剪动作不可能产生更小结果；这个剪枝是局部于当前 residual state 的，不使用可能污染缓存值的全局 cutoff。

#### 3.2 独立 forward branch-and-bound

第二套 Oracle 从初始状态向前 DFS，维护完整可行 incumbent，并使用：

- elapsed + residual lower bound 剪枝；
- 同一 residual key 的更早 elapsed 支配更晚 elapsed；
- lower-bound-first 动作顺序；
- 独立重建的 incumbent action path。

两套搜索共用经过 R0 测试的语义 transition，但递推方向、缓存内容和剪枝结构不同。每个正式实例的两种 idle mode 都要求两者 makespan 完全一致，否则 runner 立即失败。

### 4. Lower bound

保留旧的合法松弛：

$$
LB_0=\max(P,Q,L,LB_{window},LB_{cut}).
$$

新增 non-preemptive release/tail subset bound。为避免把通信工作重复计数，release 和 tail 只累计 compute duration。对任意 release threshold $r$ 和 compute-tail threshold $q$，令：

$$
S(r,q)=\{i:r_i^{comp}\ge r,\ q_i^{comp}\ge q\},
$$

则：

$$
LB_{rt}(r,q)=r+\sum_{i\in S(r,q)}p_i+q.
$$

理由是集合中的所有通信都不早于 $r$ 可用，必须在单 channel 上串行；其中最后完成的通信至少还有 $q$ 的纯计算后继。枚举实际出现的 $r,q$ threshold 即可。还记录初始状态在“没有 ready flow”时到下一 compute event 的 forced-wait bound。最终取所有下界最大值。

在本批 175 图中：

- combined lower bound 在 153 图上直接等于 `OPT_optional_idle`；
- 平均 `OPT/LB=1.00918`，观察到的最大值为 `1.25`；
- 新 `release_tail` 没有在现有样本上超过旧 combined bound。

因此新增下界是安全的 non-preemptive 剪枝部件，但当前没有证据表明它比旧 window/critical-path bound 更有判别力。

### 5. R0 的零时长 compute 兼容修正

旧链 benchmark 用 `compute(0)` 表示两段通信之间没有 delay。R0 初版错误地把所有零时长节点都拒绝，导致无法重跑原反例。现已明确：

- flow duration 必须严格为正；
- compute duration 可以为 0，并在被解锁的同一时刻 start/complete；
- 零时长 compute 用退化区间 `[t,t)` 记录；
- communication 仍绝不允许零时长或多个执行区间。

对应语义回归已加入 R0 测试。这只是兼容 benchmark 的瞬时闭包，不引入可抢占。

### 6. 正式 benchmark 配置

命令：

```bash
python scripts/study_nonpreemptive_oracle.py
```

共 175 个图，每个图求 optional-idle/work-conserving 两种模式，并分别由 DP/B&B 求解，即 700 次 exact solve：

| category | 数量 | 配置 |
|---|---:|---|
| adversarial | 10 | 主动等待 tight-2、原 tail/5/4 构造、阶段 1 七类对抗图 |
| LLM motif | 7 | PP wave、1F1B、ZB fork、W/DP optimizer、TP+PP 等 |
| random chain | 100 | seed `260813`，2--5 chains，每链 1--3 flows |
| random general | 50 | seed `260817`，fork/join、第二段 flow、nested/optimizer join |
| real reduction | 8 | 真实 1F1B effective DAG 的 8 个时间桶，原始微秒 duration |

真实窗口显式使用 `real_max_flows=1`。这不是说窗口最终只有一条 flow：join companion closure 后最多达到 13 条 flow、32 个节点。`bucket_rank=6` 在 `max_flows=4` 时会膨胀到 48 条 flow，optional-idle DP 超过 30 秒，不再属于本阶段 exact 小窗口；因此没有把它的非精确结果混入报告。参数仍可在 runner 中调整。

### 7. 主要结果

所有 175 个正式图上：

```text
DP OPT_optional_idle == B&B OPT_optional_idle
DP OPT_work_conserving == B&B OPT_work_conserving
```

每个解重放后都自动验证：DAG 完成、所有 flow 只有一个连续区间、不同 flow 的 channel 区间不重叠。

主动等待在 10/175 个图中严格改善：

| category | 改善数/总数 | 最大绝对 idle regret |
|---|---:|---:|
| adversarial | 1/10 | 9 |
| LLM motif | 0/7 | 0 |
| random chain | 7/100 | 3 |
| random general | 2/50 | 3 |
| real reduction | 0/8 | 0 |

这说明主动等待不是只存在于手工极端反例：固定种子随机链中有 7%，随机一般 DAG 中有 4% 会受益；但在当前简单 LLM motif 和真实小窗口中尚未观察到收益，不能据此宣称真实完整 LLM DAG 一定需要等待。

10 个 idle-hard 实例为：

```text
optional_wait_tight_two: 12 vs 21
random_chain_14: 21 vs 22
random_chain_52: 20 vs 21
random_chain_60: 15 vs 16
random_chain_70: 18 vs 19
random_chain_77: 26 vs 29
random_chain_86: 24 vs 25
random_chain_98: 17 vs 18
random_join_23: 17 vs 20
random_join_46: 22 vs 23
```

这里前一个数是 `OPT_optional_idle`，后一个数是 `OPT_work_conserving`。每个最优解都只使用 1 次 voluntary WAIT；其主动等待时长分别为 `1,1,3,1,1,1,1,2,1,1`。总 WAIT time 还可能包含之后没有 ready flow 时不可避免的 forced wait，因此报告将两者分开。

几个既有反例的新模型最优值：

| instance | optional idle | work-conserving | combined LB |
|---|---:|---:|---:|
| motivating/`longest_tail_counterexample` | 8 | 8 | 8 |
| scaled tail，scale=4 | 32 | 32 | 32 |
| scaled 5/4，scale=4 | 33 | 33 | 32 |
| `random_join_30` | 22 | 22 | 22 |
| `random_join_40` | 21 | 21 | 21 |

因此 `random_join_30/40` 的旧最优值恰好保持，但这是新 Oracle 重新求出的结果，不是沿用旧 tick Oracle。它们属于 ordering-hard，不属于 idle-hard。相反，`random_join_23` 在旧可抢占模型中没有暴露差距，在新模型中等待 `t=1` 即将释放的短关键 flow 可把 makespan 从 20 降到 17，是 non-preemption 与 optional idle 共同产生的新反例。

### 8. 搜索规模与适用边界

optional-idle 比 work-conserving 多一个 WAIT 分支，明显更贵：

| mode | DP mean | DP max | B&B mean | B&B max |
|---|---:|---:|---:|---:|
| optional idle | 781.62 ms | 21,576.69 ms | 141.63 ms | 4,024.21 ms |
| work-conserving | 202.82 ms | 5,684.87 ms | 63.36 ms | 1,653.33 ms |

最难的 `random_chain_88` 有 12 条 flow：optional-idle DP 探索 5,168 个 residual states，用时约 21.6 秒；B&B 探索 13,322 次 DFS visit，用时约 4.0 秒。状态数不能直接横比，因为 DP state 只计 cache miss，而 B&B visit 包含被剪分支。

这套 Oracle 适合作为 R2/R3 的小图 teacher、反例验证器和 optimality label 生成器，不适合作为在线 scheduler。默认 hard limit 为每次 `2,000,000` states、30 秒；超限会显式抛错，不会返回伪装成最优的 incumbent。

### 9. Regret 分解与结论

对 work-conserving heuristic $H$：

$$
T_H-OPT_{idle}
=
\underbrace{T_H-OPT_{wc}}_{ordering\ regret}
+
\underbrace{OPT_{wc}-OPT_{idle}}_{idle\ regret}.
$$

R1 已经能给两项分别打 exact label。后续 R2 不应再把所有失败都解释成“tail 排序错误”：

- `random_join_30/40` 应用于研究 ready-flow ordering；
- `optional_wait_tight_two`、`random_join_23/46` 和 7 个随机链应用于研究 WAIT 判定；
- 两类 hard set 应分别报告，再增加同时含两种损失的 combined-hard set。

R1 退出条件已经满足：两套独立搜索在全部正式小图、两种 idle 模式上给出相同最优值；每条最优时间线均通过不可抢占连续区间验证。下一阶段 R2 可以开始把 FIFO/SPT/LPT/Longest-delay/Dynamic-tail 等全部改为 whole-flow 版本，并以本 Oracle 重新测经验最优率、近似比反例和 WAIT-aware rollout。

最终定向回归为 `19 passed`。完整 `pytest tests -q` 为 `859 passed, 3 skipped, 18 errors`；18 个 error 仍全部来自缺失的仓库外 `Spectrum-X_8g_8gps_400Gbps_H100` topology fixture，与 R1 无关。`git diff --check` 和人工 100 字符 Python 行宽检查通过；当前环境仍未安装 `ruff`。

---

## 修订阶段 R2：并行链算法与理论（2026-08-09）

### 1. 新的 operation-level 并行链模型

新增 `scripts/study_nonpreemptive_parallel_chains.py`。旧 `study_parallel_chains.py` 继续保留为 legacy tick/preemptive 实验，不在原文件上修改语义，以免历史结果和新模型混淆。

第 $k$ 条链表示为：

$$
(r_k; p_{k,1},q_{k,1},p_{k,2},q_{k,2},\ldots,p_{k,m_k},q_{k,m_k}),
$$

其中 $r_k$ 是第一条 flow 前的 initial compute/release delay，$p$ 是不可抢占通信时长，$q$ 是通信后的 compute delay。旧 `Chain(comm,delay)` 等价于 $r_k=0$。

紧凑状态只记录：

$$
s_k=(j_k,c_k),
$$

即下一条尚未开始的通信位置 $j_k$ 和当前 compute cooldown $c_k$。不再记录“通信 remaining”，因为 flow 不允许执行一半后留在状态里。

- `FLOW(k)`：运行完整 $p_{k,j_k}$；期间所有 active cooldown 同步减少，但新释放 flow 不能抢占；结束时该链进入 $q_{k,j_k}$；
- `WAIT`：推进 $\min_{k:c_k>0}c_k$，至少完成一个 compute；
- work-conserving 模式仅在 ready set 为空时 WAIT；
- optional-idle 模式在有 active compute 时始终允许 WAIT。

每种 heuristic 的完整动作序列都会转换回 `BenchmarkDAG`，交给 R0 状态机重新执行，并验证 makespan 相同、DAG 完成、每个 flow 只有一个连续区间、channel 上 flow 区间不重叠。正式实验中所有算法的 `preemptions=0`。

### 2. 实现的方法是什么

#### 2.1 Whole-flow priority baseline

- **FIFO**：选择编号最小的 ready chain，作为固定到达顺序基线；
- **SPT**：选择当前完整 flow 最短者，希望尽快释放下一批任务；
- **LPT**：选择当前完整 flow 最长者，优先消化大通信；
- **Longest-delay**：只看当前 flow 紧随的下一段 compute，优先释放最长的下一段计算；
- **Dynamic-tail**：计算该 ready flow 完成后的整条 residual chain tail，即后续所有 compute 和 future communication 的总长度，选择 tail 最大者；
- **LRPT**：在 Dynamic-tail 上再加当前 flow 时长，优先 residual processing time 最大者；
- **Earliest-slack**：在独立链 makespan 模型中与 LRPT 排序代数等价，作为命名对照保留；
- **TicTac-style pairwise**：对候选 A/B 比较“A 完整执行后再 B”和“B 后再 A”对两条 residual tail 的局部影响，以锦标赛方式选出一个 flow。

这些 baseline 都是 idle-unaware/work-conserving：有 ready flow 就必须开始一条完整 flow。

#### 2.2 Full-flow rollout

以 Dynamic-tail 为 base policy。对 top-2 ready flow 分别评估：

$$
\widehat C(a|s)=\Delta(s,a)+J_{DT}(T(s,a)),
$$

其中 $J_{DT}$ 是从 successor 开始用 whole-flow Dynamic-tail 补全到结束的真实模拟 makespan。

- **Rollout-flow-2**：只比较完整 flow；
- **Rollout-WAIT-2**：在有 active compute 时额外比较 WAIT 到下一 completion。

base action 无条件进入候选。因此即使 top-2 截断或增强搜索超时，也可以回退到 Dynamic-tail。默认 wall-clock budget 为 2 秒；超时后剩余调度直接使用 base policy，并记录 fallback。

#### 2.3 Beam 与 Monte Carlo

- **Beam-8/32**：一层扩展一个完整 FLOW 或一次 WAIT；按 elapsed + residual LB 和 Dynamic-tail completion estimate 排序，只保留 8/32 个状态；始终保留完整 Dynamic-tail incumbent；默认 `100,000` state、2 秒 budget；
- **MC-64**：采样 64 条完整 operation-level action path。70% 概率从 Dynamic-tail top-2 中随机，15% 概率在 WAIT 合法时尝试等待；最终与 Dynamic-tail incumbent 取最好值。

Beam/MC 是有明确预算的非多项式增强，不是 exact solver。固定 Beam 反例见后文。

#### 2.4 Exact DP 与二分 feasibility

直接 DP 的递推为：

$$
F(s)=\min_{a\in A(s)}\{\Delta(s,a)+F(T(s,a))\}.
$$

它分别求 `OPT_optional_idle` 和 `OPT_work_conserving`。相同 `ParallelChain` 的状态在组内排序 canonicalize，因此只合并真正相同链的置换。

二分法在 $[LB,U]$ 上搜索 horizon $H$，feasibility DP 使用：

$$
\operatorname{Feasible}(s,B)
=\bigvee_{a:\Delta_a\le B}
\operatorname{Feasible}(T(s,a),B-\Delta_a),
$$

若 residual `max(P,Q,L)>B` 立即返回 false。这里 WAIT 是正式 action，不再像旧二分 DP 那样只有 forced tick idle。

这两种 DP 对数值 duration 是伪多项式/状态空间指数型 teacher，不是在线算法。默认 hard limit 是 200 万 states、30 秒。

### 3. 与 R1 Oracle 的交叉验证

新增 `tests/test_study_nonpreemptive_parallel_chains.py`。其中 12 个固定随机实例被转换成一般 `BenchmarkDAG`，compact DP 与 R1 Oracle 在两种 idle mode 下全部一致；另有 8 个实例的 operation-level binary feasibility 与 direct DP 一致。

正式 100 图中，前 40 图再次运行两种 mode 的 binary feasibility：全部与 direct DP 一致。optional-idle direct DP 平均 1,891.8 states、46.21 ms，p95 为 8,696 states；work-conserving DP 平均 665.06 states、11.82 ms。前 40 图的 optional binary 平均 471.03 states、11.64 ms。这个样本上二分更省，但它并不改变 worst-case 指数状态数。

### 4. 正式随机实验

配置与旧阶段保持相同以便对照：100 个固定 seed `260813` 实例，2--5 条链，每链 1--3 个 flow，communication 1--4，compute delay 0--6。所有比值分母是新的 `OPT_optional_idle`。

| method | exact optimal | mean ratio | p95 | observed max | mean runtime |
|---|---:|---:|---:|---:|---:|
| FIFO | 14/100 | 1.15997 | 1.3750 | 1.5000 | 0.21 ms |
| SPT | 24/100 | 1.13358 | 1.4000 | 1.4783 | 0.21 ms |
| LPT | 8/100 | 1.16787 | 1.3684 | 1.5385 | 0.18 ms |
| Longest-delay | 30/100 | 1.08975 | 1.2500 | 1.3333 | 0.18 ms |
| Dynamic-tail | 83/100 | 1.01444 | 1.1111 | 1.1875 | 0.19 ms |
| LRPT / Earliest-slack | 58/100 | 1.03041 | 1.1429 | 1.1875 | 0.20 ms |
| TicTac-style | 83/100 | 1.01444 | 1.1111 | 1.1875 | 0.17 ms |
| Rollout-flow-2 | 91/100 | 1.00525 | 1.0476 | 1.1154 | 1.74 ms |
| Rollout-WAIT-2 | **95/100** | **1.00234** | **1.0000** | **1.0556** | 2.54 ms |
| Beam-8 | 100/100 | 1.00000 | 1.0000 | 1.0000 | 7.05 ms |
| Beam-32 | 100/100 | 1.00000 | 1.0000 | 1.0000 | 15.38 ms |
| MC-64 | 91/100 | 1.00587 | 1.0476 | 1.1154 | 17.14 ms |

正式结果与 R2 先验审计的核心数字一致：Dynamic-tail 仍为 83/100，idle-hard 仍为 7/100。正式 rollout 略好于临时原型的 89/92，得到 91/95；原因是正式版统一使用完整 flow transition、相同 whole-flow Dynamic-tail 补全、base-action incumbent 和稳定 tie-break。应以本节结果为准。

所有方法 fallback 均为 0。Beam-32 在这组很小的 frontier 上没有比 Beam-8 增加命中率，却把平均时间从 7.05 ms 增到 15.38 ms；MC-64 更慢且只达到 rollout-flow-2 的命中数，当前没有作为默认方法的价值。

TicTac-style 在这 100 图上与 Dynamic-tail 的 makespan 指标完全相同。这不证明 comparator 等价，只说明当前短链/小 duration 分布没有产生可见增益。

### 5. WAIT 消融和 hard set

三类困难实例数量为：

```text
idle-hard:             OPT_wc > OPT_idle                 7/100
ordering-hard:         Dynamic-tail > OPT_wc            13/100
combined-hard:         两种差距同时存在                  3/100
```

Dynamic-tail 在 87/100 图上达到 `OPT_wc`，但因为其中部分 `OPT_wc>OPT_idle`，最终只在 83/100 图上达到真正目标最优。

Rollout-flow-2 在 98/100 图上达到 `OPT_wc`，说明 full-flow counterfactual 几乎消除了 ready-flow ordering error；但它不能主动等待，所以最终只有 91/100 达到 `OPT_idle`。

加入 WAIT 后，Rollout-WAIT-2 在 4 个实例上严格改善 flow-only，0 个变差：

```text
sample 52: 21 -> 20 = OPT_idle
sample 60: 16 -> 15 = OPT_idle
sample 77: 29 -> 26 = OPT_idle
sample 98: 18 -> 17 = OPT_idle
```

另外 3 个 idle-hard 实例 `14/70/86` 没被一步 WAIT rollout 修复；它们同时属于 combined-hard，必须先修 ordering 或需要更深的 wait/order 联合 lookahead。WAIT 不是“加入候选就自动最优”，但消融已经证明它有独立实际作用。

### 6. 不可抢占 work-conserving 的紧 2-approximation

令总通信量：

$$
P=\sum_{k,j}p_{k,j},
$$

令单条链上的最大总计算量（含 initial release）：

$$
Q=\max_k\left(r_k+\sum_jq_{k,j}\right).
$$

对任意 work-conserving whole-flow 调度 $H$，channel busy time 恰为 $P$，记总 forced idle 为 $I_H$，则：

$$
T_H=P+I_H.
$$

取在 $T_H$ 时最后完成的链 $k^*$。在任意 channel idle 区间中，$k^*$ 尚未完成；如果它有 ready flow，work-conserving 规则就不允许 channel idle，因此它此时必在执行自己的 compute。链内 compute 不重叠，所以所有 channel idle 都可注入式地收费到 $k^*$ 的 compute 区间：

$$
I_H\le r_{k^*}+\sum_jq_{k^*,j}\le Q.
$$

另一方面，任何允许主动等待的最优解也必须在单 channel 上完成全部通信，并在每条链上经历其计算，所以：

$$
OPT_{idle}\ge P,\qquad OPT_{idle}\ge Q.
$$

因此：

$$
T_H\le P+Q\le2\max(P,Q)\le2OPT_{idle}.
$$

这个证明不要求通信可抢占，也不要求某种 priority；FIFO、SPT、LPT、Dynamic-tail、flow-only rollout 等任意 work-conserving whole-flow 顺序都成立。

### 7. 为什么 2 是 tight

构造两条链：

```text
A: comm(M) -> compute(0)，t=0 ready
B: initial compute(1) -> comm(1) -> compute(M)
```

在 `t=0` 只有 A ready，所以任何 work-conserving 算法都必须运行 A：

$$
T_{wc}=M+1+M=2M+1.
$$

允许主动等待的最优解为：

```text
WAIT [0,1) -> B [1,2) -> A [2,M+2)
                       └─ B compute [2,M+2)
```

故：

$$
OPT_{idle}=M+2,
\qquad
\frac{T_{wc}}{OPT_{idle}}=\frac{2M+1}{M+2}\to2.
$$

因此 2 不只是当前证明松，而是所有 work-conserving 算法相对 optional-idle OPT 的紧界。Dynamic-tail 作为其中之一也有 tight 2；继续尝试证明普通 Dynamic-tail 严格小于 2 已经没有意义。

在这个族上 Rollout-WAIT-2 会比较 `FLOW(A)` 和 `WAIT` 的完整后果，并选择 WAIT，得到 OPT。这说明 WAIT rollout 修复了 tight family，但尚不能推出它在一般实例上有 `<2` 保证。

### 8. Rollout、Beam、MC 的可证明界

令 base Dynamic-tail 的 cost-to-go 为 $J_B(s)$。rollout 的候选始终包含 base action $a_B$，并选择：

$$
a_R\in\arg\min_{a\in C(s)}\{\Delta_a+J_B(T(s,a))\}.
$$

因为 $a_B\in C(s)$：

$$
\Delta_{a_R}+J_B(T(s,a_R))
\le
\Delta_{a_B}+J_B(T(s,a_B))
=J_B(s).
$$

沿 rollout 轨迹递推/望远镜求和，得到 $J_R(s)\le J_B(s)$。因此 flow-only 和 WAIT-aware rollout 都不差于 Dynamic-tail，并继承 2-approximation 上界。超时后回退 base 也保持该性质。

Beam 和 Monte Carlo 总是把完整 Dynamic-tail schedule 作为 incumbent，所以返回值同样不差于 Dynamic-tail，也继承 2 上界。但“继承 2”不代表存在严格小于 2 的一般界；当前尚无这样的证明。

旧的渐近 5/4 family 在 whole-flow 模型下仍使 Dynamic-tail 得到：

$$
OPT=8k+1,\qquad T_{DT}=10k,\qquad T_{DT}/OPT\to5/4.
$$

但新的 full-flow rollout 在所有测试 scale 上直接得到 $8k+1$，所以这个 family 不能再作为 rollout 或 Beam 下界。

固定宽 Beam 也不是 exact。固定 seed `260820` 搜索得到 6-chain 反例，允许 WAIT 的 Beam-8 和
Beam-32 都返回 47，而 OPT 为 46；该实例已固化为 `fixed_beam_counterexample()` 和永久测试。
100/100 只能表述为当前分布上的经验命中率。补充实验还表明，不允许 WAIT 的 Beam-32 在这个
特定反例上能得到 46；不同动作空间会改变启发式剪枝结果，并不存在简单的单调支配关系。

### 9. 复杂性与 restricted results

本模型已经包含经典单机 release-time/tail 问题 $1|r_j,q_j|C_{max}$：对每个 job 建一条只有一次通信的链：

```text
initial compute(r_j) -> comm(p_j) -> compute(q_j)
```

两边的可行不可抢占 schedule、主动 idle 和目标值逐一相同，因此这是保持目标值的直接嵌入，不需要借用可抢占或 exact-lag reduction。该单机问题已知强 NP-hard；Hall 与 Shmoys讨论了 Jackson rule、无 precedence 的 PTAS 和有 precedence 的 4/3 approximation：[Jackson's Rule for Single-Machine Scheduling](https://doi.org/10.1287/moor.17.1.22)。Vakhania给出 release-time/tail 模型及 equal processing-time 等特殊多项式情形：[Single-Machine Scheduling with Release Times and Tails](https://doi.org/10.1023/B:ANOR.0000030692.69147.e2)。

可保留的特殊结论：

1. **每链单 flow、全部初始 ready：** 按 compute tail 非增序执行最优。相邻交换中，若 $q_i<q_j$ 却把 $i$ 放在 $j$ 前，交换二者不会增加 $\max(C_i+q_i,C_j+q_j)$；因此 Dynamic-tail exact。20 个随机回归全部验证。
2. **所有 initial/post compute 都为 0 且初始 ready：** 任意 work-conserving 顺序 makespan 都等于 $P$。
3. **相同链重复：** 可按 `(next operation,cooldown)` 组内排序，exact canonicalization 不改变最优值。
4. **有界链数/每链深度/整数 duration：** operation-level DP 可作为伪多项式 exact 方法；复杂度仍随状态组合指数增长。
5. **单 flow 且 processing time 全相等、允许 release：** 文献给出多项式算法，但这不等于当前 Dynamic-tail 自动最优，也不能直接推广到多 flow chain。

### 10. 一般 fork/join 的界暂不外推

独立链证明用到了“最后完成链在每段 network idle 中都在 compute”这一关键事实。一般 fork/join 中，最后 sink 的不同阻塞阶段可能来自不可比较分支；是否能把所有 idle 区间拼成一条合法 blocking chain，需要重新构造证明。R2 不把上述 $P+Q$ 证明直接宣称为一般 DAG 定理。

R3 应在 effective DAG 上尝试按“最后完成前驱”反向构造 blocking chain，并检查 join 切换是否会重复收费；在证明完成前，一般 DAG 只安全使用 $P+L$ 或可验证的 residual lower bound，不能引用本节独立链的 $P+Q$ charging。

### 11. 当前结论

1. Non-preemptive Dynamic-tail 仍是最有价值的低成本 baseline：约 0.19 ms、83% exact，明显优于只看 size 或下一段 delay；
2. 它相对 optional-idle OPT 的一般 2 界已经证明且 tight，不能期待普通 work-conserving priority 突破；
3. Full-flow rollout 主要修 ordering，98/100 达到 `OPT_wc`；
4. WAIT rollout 有独立价值，把 exact 率从 91% 提到 95%，但仍漏掉 3 个 combined-hard 和 2 个普通 ordering case；
5. Beam-WAIT-8 在当前 100 图全最优但已有 47/46 反例；Beam-WAIT-32 没提高样本质量，只增加开销；
6. MC-64 比 rollout 更慢且不更准，暂不推荐；
7. R2 的推荐 incumbent 是 `Dynamic-tail + Full-flow top-2 rollout + WAIT candidate + timeout fallback`；R3 应围绕 combined-hard 设计更深但受限的 joint wait/order lookahead，而不是继续堆静态 bonus。

正式 R2 输出为 `outputs/nonpreemptive_parallel_chains/r2_summary.json`。本阶段没有修改 `docs/260804组会.md`，也没有修改通用 schema/executor/baseline。

最终 R0--R2 联合定向回归为 `29 passed`，R2 单独为 `10 passed`。完整 `pytest tests -q` 为 `869 passed, 3 skipped, 18 errors`；18 个 error 仍全部来自缺失的仓库外 `Spectrum-X_8g_8gps_400Gbps_H100` topology fixture，与 R2 无关。`git diff --check` 和人工 100 字符 Python 行宽检查通过；环境仍未安装 `ruff`。

## R2 补充：旧实验矩阵的不可抢占全因子复现（2026-08-09）

### 1. 为什么补做

上一节已经重做了 Dynamic-tail、Rollout-2、WAIT-aware Rollout、Beam 和 Monte Carlo，
但没有把早期阶段二中用过的候选数、Beam 宽度以及是否允许 WAIT 全部逐项展开。
本节在同一批固定种子实例上补齐这些组合，避免把“搜索方法变化”和“主动等待变化”混在一起。

统一语义仍是：每个 flow 一旦开始便独占单 channel 直至完成；compute 也不可抢占；只有节点完成时
才能重规划；optional-idle 算法可以等待到下一个 compute 完成事件。所有启发式解都通过 R0 DAG
执行器回放。正式输出仍为 `outputs/nonpreemptive_parallel_chains/r2_summary.json`。

补齐的增强算法如下：

- `Rollout-flow-{2,4}`：从 Dynamic-tail 排名前 2 或 4 的 ready flow 中，分别假设完整执行该
  flow，再用 Dynamic-tail 补全，选择预计完工时间最小者；不允许主动等待。
- `Rollout-WAIT-{2,4}`：除上述 flow 外，把“等到下一次 compute 完成”也作为候选动作。
- `Beam-flow-{8,32}`：仅扩展 flow 动作，每层保留预计最好 8 或 32 个部分调度。
- `Beam-WAIT-{8,32}`：同时扩展 flow 与 WAIT 动作，每层保留 8 或 32 个状态。
- `MC-flow-64` 与 `MC-WAIT-64`：分别在不含/包含 WAIT 的动作空间内做 64 次随机补全，保留
  最好结果。二者使用相同总采样预算，而不是每类动作各 64 次。

### 2. 随机小实例的全因子结果

100 个正式实例使用 seed `260813`；表中最优率和比值均相对允许主动等待的 exact oracle。
运行时间只是本机单次 Python 小实例开销，用于看同组实验的相对成本。

| 算法 | 最优数 | 平均比 | 最坏比 | 平均时间 |
|---|---:|---:|---:|---:|
| Rollout-flow-2 | 91/100 | 1.00525 | 1.1154 | 1.83 ms |
| Rollout-flow-4 | 91/100 | 1.00525 | 1.1154 | 2.74 ms |
| Rollout-WAIT-2 | 95/100 | 1.00234 | 1.0556 | 2.55 ms |
| Rollout-WAIT-4 | 95/100 | 1.00234 | 1.0556 | 3.37 ms |
| Beam-flow-8 | 93/100 | 1.00436 | 1.1154 | 5.70 ms |
| Beam-flow-32 | 93/100 | 1.00436 | 1.1154 | 11.09 ms |
| Beam-WAIT-8 | **100/100** | **1.00000** | **1.0000** | 7.09 ms |
| Beam-WAIT-32 | **100/100** | **1.00000** | **1.0000** | 14.99 ms |
| MC-flow-64 | 91/100 | 1.00532 | 1.1154 | 14.80 ms |
| MC-WAIT-64 | 91/100 | 1.00587 | 1.1154 | 16.22 ms |

逐因素配对结果为：

| 改动 | 改善 | 相同 | 变差 |
|---|---:|---:|---:|
| Rollout-flow-2 -> flow-4 | 0 | 100 | 0 |
| Rollout-WAIT-2 -> WAIT-4 | 0 | 100 | 0 |
| Beam-flow-8 -> WAIT-8 | 7 | 93 | 0 |
| Beam-flow-32 -> WAIT-32 | 7 | 93 | 0 |
| MC-flow-64 -> WAIT-64 | 2 | 95 | 3 |

因此，top-4 在当前短链分布上没有补到 top-2 漏掉的选择，只增加约 32%--50% 的时间；Beam
加入 WAIT 后恰好修复全部 7 个 idle-hard 实例。Monte Carlo 的反常结果不是 WAIT 本身有害，而是
固定 64 次预算被更大的动作空间稀释：它虽然改善 2 图，却使 3 图变差。若继续研究 MC，应给
FLOW 和 WAIT 分层采样或分别保证配额，并始终保留 deterministic rollout/Beam incumbent。

### 3. 受限情形矩阵

为区分偶然命中和结构性规律，又补做四组枚举或随机受限实例：

| 实例族 | 数量 | idle-hard | Longest-delay 最优率/最坏比 | Dynamic-tail 最优率/最坏比 | LRPT 最优率/最坏比 |
|---|---:|---:|---:|---:|---:|
| 每链单 flow、初始全 ready | 200 | 0 | 100% / 1.000 | 100% / 1.000 | 62.5% / 1.250 |
| 所有 compute 为 0、初始全 ready | 100 | 0 | 100% / 1.000 | 100% / 1.000 | 100% / 1.000 |
| 等长通信、3 链各 2 flow | 729 | 0 | 74.49% / 1.333 | 100% / 1.000 | 100% / 1.000 |
| 链内 compute lag 非增 | 576 | 8 | 81.42% / 1.429 | 93.92% / 1.200 | 88.02% / 1.286 |

前两行分别对应上一节已经证明的“单 flow tail 排序”和“纯通信总量”结论。后两行目前只是有限
枚举证据：尤其不能由 729 个等长通信实例直接宣称 Dynamic-tail 或 LRPT 对任意链数、深度都最优。
非增 lag 仍出现 8 个 idle-hard，说明“链内计算越来越短”并不足以消除主动等待价值。

### 4. 规模与 exact oracle 边界

使用同一生成器各取一个较大实例，并给 exact DP 设置 30000 状态、2 秒预算：

| 链数 | flow 数 | exact/状态数 | Beam-flow/wait 8 | Beam-flow/wait 32 | MC-flow/wait 64 |
|---:|---:|---:|---:|---:|---:|
| 4 | 7 | 16 / 623 | 16 / 16 | 16 / 16 | 16 / 16 |
| 6 | 14 | 31 / 19411 | 31 / 31 | 31 / 31 | 31 / 31 |
| 8 | 15 | 超预算 | 34 / 34 | 34 / 34 | 34 / 34 |
| 10 | 19 | 超预算 | 34 / 34 | 34 / 34 | 34 / 34 |

4、6 链时可由 oracle 确认所有列最优；8、10 链只有算法间一致，不能写成“已经证明最优”。
这也验证了 operation-level DP 的组合爆炸：从 7 flow 的 623 状态上升到 14 flow 的 19411 状态，
再稍微增大就触及预算。因此 exact 更适合小图 oracle、局部窗口或结构压缩后的子问题。

### 5. 固定反例复核

- Dynamic-tail 的旧 $5/4$ 渐近族在 scale `1,2,4,8,16` 上分别得到
  `OPT=9,17,33,65,129` 和 `DT=10,20,40,80,160`；两种 Rollout-2 均得到 OPT。
- Work-conserving tight-2 族在 $M=10,20,50,100$ 上的比值依次为
  `1.75, 1.8636, 1.9423, 1.9706`；Rollout-WAIT-2 均选择等待并得到 OPT。
- 固定 Beam 反例的 `OPT=46`：Beam-flow-8、Beam-WAIT-8、Beam-WAIT-32 都为 47，只有
  Beam-flow-32 为 46。因此“本批 100 图全最优”仍不能当作 Beam-WAIT 的 exact 性质，也不能认为
  width 32 单调支配 width 8；启发式截断和评分会改变保留的状态集合。

### 6. 补充实验后的选择

在线默认仍推荐 `Dynamic-tail + full-flow top-2 rollout + WAIT + timeout fallback`：它在这批实例上
达到 95% exact，成本显著低于 Beam/MC，而且候选数从 2 增到 4 没有收益。Beam-WAIT-8 可作为
小窗口离线增强或更强实验对照，它确实解决了本批全部 idle-hard，但已有固定反例。当前 MC-64
不推荐；若重新设计，应先解决动作分层采样问题。以上表格取代上一节未展开 flow/WAIT 因子的增强
算法行；上一节的基础 priority 结果和 2-approximation/tight 证明不变。

补充后的 R0--R2 联合定向回归为 `26 passed`，其中 R2 文件为 `12 passed`。完整
`pytest tests -q` 为 `871 passed, 3 skipped, 18 errors`；18 个 error 仍全部来自缺失的仓库外
`Spectrum-X_8g_8gps_400Gbps_H100` topology fixture，与本次修改无关。本段计数取代上一节补充实验
之前记录的 `29/10/869` 历史计数。

## 修订阶段 R3：从并行链推广到一般 DAG（2026-08-09）

### 1. 阶段目标与实现边界

本阶段把 R2 的并行链算法推广到含 fork、join、多层依赖的一般 DAG，并重新检查旧阶段 3 的结论。
实现位于 `scripts/study_nonpreemptive_general_dag.py`，仍是隔离研究原型，没有修改通用
schema、executor 或 Default/Puppeteer/Hermod。它直接调用 R0 状态机，因此：

- FLOW 动作会让一条通信独占 channel，完整执行到结束；
- compute 一旦依赖满足便立即开始，并连续执行到结束；
- active flow 期间发生的 compute completion 只更新依赖，不产生抢占点；
- channel 空闲时才能重新选择 `FLOW(v)` 或 `WAIT_TO_NEXT_RELEASE`。

正式集合包含 7 个旧 adversarial motif、1 个 raw-join 固定例、3 个 combined-hard 固定图和 seed
`260817` 生成的 100 个 random join DAG，共 111 图。每图都由 R1 exact oracle 分别求
`OPT_idle` 和 `OPT_wc`，启发式时间线再由 R0 回放并检查每个节点只有一个连续区间。

### 2. Residual Dynamic-tail 与一般 DAG 特征

对决策状态 $s$，令 $d_s(v)$ 为节点剩余时间：completed 为 0，running compute 使用 remaining，
pending 节点使用原 duration。忽略 channel contention 的 residual earliest finish 为：

$$
E_s(v)=d_s(v)+
\max_{u\in pred(v),\ d_s(u)>0}E_s(u).
$$

反向计算 residual path 和 flow 完成后的 tail：

$$
L_s(v)=d_s(v)+\max_{w\in succ(v)}L_s(w),
\qquad
Q_s(v)=\max_{w\in succ(v)}L_s(w).
$$

Non-preemptive Dynamic-tail 只在 channel 空闲时，从 ready flows 中选 $Q_s(v)$ 最大者。这里的
tail 是 residual DAG 上的动态量；active flow 期间可以重新计算未来分数，但绝不据此切换 active
flow。安全 residual lower bound 使用：

$$
LB(s)=\max\left(
\sum_{v\text{ is unfinished FLOW}}d_s(v),
\max_v L_s(v)
\right).
$$

另外实现了四类候选来源：SPT、直接后继 compute 最长、join latest-blocker、以及能在下一次
compute release 前完成或最少越过 release 的 flow。它们只负责扩充动作集合，不与 tail 做线性加权。

### 3. Join 建模：候选和最终评价必须分开

对 join 子节点 $x$ 的未完成父节点 $v$，原始 last-blocker 信号近似为：

$$
g_s(v,x)=\max\left(0,
E_s(v)-\max_{u\in pred(x)\setminus\{v\}}E_s(u)
\right).
$$

`Raw-join` 直接用 `tail + join_gain` 排序；`Hybrid` 则只把 Join/SPT/long-delay/future-release
各自最优的 flow 加入候选，最终统一评价完整后果。对动作 $a$：

$$
\widehat C(s,a)=
\Delta(a)+J_{DT}(T(s,a)),
$$

其中 FLOW 的 $\Delta$ 是完整通信时长，WAIT 的 $\Delta$ 是到下一 compute completion 的时间，
$J_{DT}$ 是从后继状态用 whole-flow Dynamic-tail 补全的端到端 makespan。等待价值因此是：

$$
V_{wait}(s)=
\min_{a\in FLOW(s)}\widehat C(s,a)-\widehat C(s,WAIT).
$$

只有 $V_{wait}>0$ 才值得主动空闲；不存在额外的手调 WAIT bonus。

Join 消融给出了很清楚的结果：

| 检查 | 结果 |
|---|---:|
| Raw-join 相对 Dynamic-tail 改善/相同/变差 | **0 / 86 / 25** |
| Join 在 Dynamic 时间线上提供 top-2 外候选的图 | 24/111 |
| 新 Join 候选出现次数 | 38 |
| Hybrid-WAIT-2 相对纯 Dynamic-WAIT-2 改善/变差 | **0 / 0** |

所以 raw join bonus 在不可抢占模型下仍然有重复奖励/错误放大问题，而且比旧模型暴露得更明显。
Join 候选并非没有改变候选集，但这 38 次差异全部被端到端评价否决；当前没有证据要求在线策略
强制保留 Join 候选。保留“候选生成器”接口仍有价值，因为真实 LLM DAG 可能产生当前随机生成器
没有覆盖的 join pattern，但在得到独立收益前不能进入默认策略。

### 4. Full-flow rollout 与二层联合 lookahead

本阶段比较以下增强：

- `Rollout-flow-2`：Dynamic-tail top-2，只评价完整 FLOW，不主动等待；
- `Rollout-WAIT-2`：在上述候选中加入 WAIT；
- `Hybrid-flow/WAIT-2`：再加入四类结构候选；
- `Hybrid-WAIT-{2,4}-depth2`：枚举连续两个完整动作后再用 Dynamic-tail 补全，用于处理
  “先修正顺序、下一步才看出等待价值”的 combined-hard；
- `Beam-WAIT-8`：在完整 FLOW/WAIT 状态空间保留 8 个最好状态，并始终保留完整 Dynamic-tail
  schedule 作为 incumbent。

所有 rollout 的候选都包含 base action，最终还与完整 baseline 比较；超时会回退 baseline。因此实验
中的增强逐实例不差于 Dynamic-tail。不过 R2 的独立链 2-approximation 证明不能直接用于一般 DAG，
所以这里的 incumbent safeguard 只证明相对支配 $T_{enhanced}\le T_{DT}$，不宣称一般 DAG 上已有
常数近似比。

### 5. 正式总体结果

比值均相对 `OPT_idle`：

| 方法 | 最优数 | 平均比 | 最坏比 | 平均时间 |
|---|---:|---:|---:|---:|
| Dynamic-tail | 97/111 | 1.010168 | 1.176471 | 1.14 ms |
| Raw-join | 72/111 | 1.027700 | 1.190476 | 1.08 ms |
| Rollout-flow-2 | 101/111 | 1.006561 | 1.176471 | 9.67 ms |
| Rollout-WAIT-2 | 107/111 | 1.001868 | 1.062500 | 12.53 ms |
| Hybrid-flow-2 | 101/111 | 1.006561 | 1.176471 | 11.09 ms |
| Hybrid-WAIT-2 | 107/111 | 1.001868 | 1.062500 | 14.52 ms |
| Hybrid-WAIT-2-depth2 | **110/111** | **1.000429** | 1.047619 | 34.43 ms |
| Hybrid-WAIT-4-depth2 | **110/111** | **1.000429** | 1.047619 | 40.16 ms |
| Beam-WAIT-8 | **110/111** | **1.000375** | **1.041667** | 45.37 ms |

`top-4` 与 `top-2` 的 makespan 在 111 图上完全相同，只把二层 rollout 平均开销从 34.43 ms
增加到 40.16 ms。Hybrid 与纯 Dynamic candidate rollout 也完全相同，说明当前收益来自完整
counterfactual、WAIT 和第二层联合搜索，不来自 Join 等额外候选。

WAIT 的动作并没有被滥用：Rollout-WAIT-2 在 9 图上各主动等待一次，总等待 11 个时间单位；
二层方法在 10 图上共等待 10 次、16 个时间单位；Beam 在 10 图上共等待 10 次、12 个时间单位。

### 6. 三类困难集

定义保持与规划一致：

```text
ordering-hard: Dynamic-tail > OPT_wc
idle-hard:     OPT_wc > OPT_idle
combined-hard: 两个不等式同时成立
```

三个集合可以重叠；combined-hard 同时计入前两类。表中 `改善` 是严格优于 Dynamic-tail，
`exact` 是达到 `OPT_idle`，gap closed 为
`(DT - H) / (DT - OPT_idle)`：

| 方法 | ordering 7：改善/exact/gap | idle 10：改善/exact/gap | combined 3：改善/exact/gap |
|---|---:|---:|---:|
| Rollout-flow-2 | 5/4/66.67% | 1/0/6.67% | 1/0/22.22% |
| Rollout-WAIT-2 | **7/4/80.95%** | **9/7/81.67%** | **3/1/72.22%** |
| Hybrid-WAIT-2-depth2 | **7/6/95.24%** | **10/9/96.67%** | **3/2/88.89%** |
| Beam-WAIT-8 | 6/6/85.71% | **10/10/100%** | **3/3/100%** |

这解释了为什么只看总体最优率会误导：flow-only rollout 几乎不能修 idle-hard，而 WAIT-2 已经在
三类集合上都稳定正收益；第二层又专门补上“一次动作还看不见等待后果”的 combined case。
三个 combined 固定图的 `(OPT_idle, OPT_wc, DT, WAIT-2, depth2, Beam)` 分别为：

```text
combined_chain_14: (21, 22, 24, 22, 22, 21)
combined_chain_70: (18, 19, 20, 18, 18, 18)
combined_chain_86: (24, 25, 26, 25, 24, 24)
```

二层 rollout 唯一未最优的是 `combined_chain_14`：22 对 OPT 21；Beam 修复它。Beam 唯一未最优
的是 ordering-only 的 `random_join_60`：25 对 OPT 24，而二层 rollout 修复它。二者各 110/111
但失败集合不同，再次说明有限搜索之间不存在简单支配关系。

### 7. `random_join_30/40` 的不可抢占重验

两图都属于 ordering-hard，不属于 idle-hard；新 oracle 仍给出 22 和 21。通信动作顺序为：

```text
random_join_30
DT:      WAIT, c1_0, c0_0, WAIT, c0_1, c1_1, WAIT, final_comm, WAIT  -> 24
Rollout: WAIT, c0_0, c1_0, WAIT, c0_1, c1_1, WAIT, final_comm, WAIT  -> 22

random_join_40
DT:      WAIT, c1_0, c0_0, c3_0, c2_0, WAIT, c0_1, c1_1, c2_1, ... -> 22
Rollout: WAIT, c1_0, c2_0, c0_0, c3_0, c2_1, c1_1, c0_1, ...       -> 21
```

这里的省略部分都是 join 后的强制 WAIT、`final_comm` 和 sink compute。每条通信都是一个连续
区间；改善来自重新排列完整 flow，而不是在中途切换。`random_join_30` 先触发 `c0_0` 的长后续
compute，`random_join_40` 则更早触发 `c2_0/c2_1` 分支，从而让 join 更早满足。

### 8. 理论边界与 R3 结论

独立链的证明能把所有 forced idle 注入最后完成链的 compute，总有 $I\le Q$。一般 fork/join 中，
不同 idle 区间的最后阻塞者可能来自不可比较分支；按实际 schedule 加入 channel-order 边后虽然能
得到一条长度等于 makespan 的 augmented critical path，但其中的 compute 段未必构成原 DAG 的
单一路径。因此当前不能用原 DAG critical path $L$ 直接证明 $I\le L$，也不能把 R2 的 tight-2
定理未经证明地外推到一般 DAG。

本阶段得到的可证明保证只有：候选始终含 Dynamic base action、完整补全用于评价、最终保留完整
baseline，因此 rollout/Beam 返回值不差于 Dynamic-tail。若要研究一般 DAG 常数界，需要继续证明
augmented blocking chain 的 compute 能否由 `OPT` 充电，或给出反例；111 图的最大经验比 1.176
不是理论上界。

R3 退出条件已经满足：Full-flow + WAIT rollout 相对 Non-preemptive Dynamic-tail 在 ordering-hard
改善 7/7、idle-hard 改善 9/10、combined-hard 改善 3/3；二层版本进一步达到 7/7、10/10、3/3，
且所有方法保留 baseline incumbent。当前在线候选建议仍是便宜的 Dynamic-WAIT-2；如果局部窗口
允许约 3 倍开销，再使用 depth-2。Join 候选暂不默认启用，Beam-WAIT-8 继续作为离线强对照。

正式输出为 `outputs/nonpreemptive_general_dag/r3_summary.json`。

R0--R3 联合定向回归为 `32 passed`，R3 专项为 `6 passed`。完整 `pytest tests -q` 为
`877 passed, 3 skipped, 18 errors`；18 个 error 仍全部来自缺失的仓库外
`Spectrum-X_8g_8gps_400Gbps_H100` topology fixture，与 R3 无关。

## 修订阶段 R4：多资源拓扑的不可抢占扩展（2026-08-09）

### 1. 为什么旧多资源结果必须重做

旧 `study_multiresource_dag.py` 的状态是每个 tick 重新选择一组 flow：本 tick 被选择的 flow 在下一
tick 可以从集合中消失，之后再恢复；即使 `advance_to_event` 连续走若干 tick，compute completion
也可能成为重新选集合的边界。因此它仍是 integral-quantum preemptive 模型，不能表示“flow 开始后
占有整条 route 直到传完”。旧阶段 4 的 exact、pack、set rollout 以及 0.39%--1.15% 收益继续保留
为历史记录，但不能作为目标模型结论。

R4 新实现为 `scripts/study_nonpreemptive_multiresource_dag.py`，仍不修改生产 executor。复用了旧的
`MultiResourceInstance`、directed-link/NIC route adapter、BFS route 和手工拓扑构造，但状态转移、
exact oracle 与所有调度策略均重新实现。

### 2. 新状态和动作语义

事件状态保存：

```text
每个 task 的 pending/running/completed、remaining、start/completion time
active flows 及其完整 directed-link/NIC resource set
active computes
ready but not started flows
当前事件时间
```

令 active flow 占用资源并集为 $U(s)$，ready flows 为 $R(s)$。合法启动集合满足：

$$
S\subseteq R(s),\qquad
routes(S)\text{ 两两不交},\qquad
routes(S)\cap U(s)=\varnothing.
$$

Optional-idle 动作为：

$$
A_{idle}(s)={START(S):S\ne\varnothing,\ S\text{ 合法}}
\cup\{WAIT\},
$$

其中只有存在 active flow/compute 的未来完成事件时 WAIT 才合法。执行 `START(S)` 后，$S$ 中所有
flow 同时启动；已有 active flow 和新 flow 都保持运行，直到最早的 active task completion。此时只
移除真正完成的 flow，其余 flow 保留 remaining 和 route reservation，然后才能进行下一次决策。

Work-conserving 对照只允许 inclusion-maximal 合法启动集合；如果没有能与 active routes 共存的新
flow，才被迫 WAIT。这里的 work-conserving 是“当前所有可用资源都不能再塞入 ready flow”，不是
单通道中的“只要有 ready flow 就必须启动”。

### 3. 为什么不能只枚举最大兼容集合

固定手算图 `nonmaximal_start_np`：

```text
t=0 ready:
  a: comm(4), route r0
  b: comm(5), route r1

release_c: compute(1)
  -> c: comm(1), route r1
  -> c_tail: compute(6)
```

若要求最大集合，`t=0` 必须同时启动 `{a,b}`。`c` 在 `t=1` ready，却必须等 `b` 到 `t=5` 才能
使用 r1：

```text
a [0,4), b [0,5), c [5,6), c_tail [6,12)  => 12
```

Optional oracle 选择非最大集合 `{a}`，给未来关键 release 留出 r1：

```text
a [0,4), c [1,2), b [2,7), c_tail [2,8)   => 8
```

因此 inclusion-maximal start sets 对目标问题不具支配性；“现在多启动一条与 active flows 不冲突的
flow”可能占住未来关键 flow 需要的 route。该图中 `OPT_wc=12`、`OPT_idle=8`，差距不是带宽共享
或抢占造成的，而是启动时机本身造成的。

另一个 `active_reservation_np` 图验证持久占用：`a(r_shared,4)` 与 `b(r_other,1)` 在 `t=0` 同时
启动；`t=1` 的 `b` 完成并释放 `c(r_shared,1)`，但 `a` 仍 active，所以 `c` 只能等待到 `t=4`。
测试明确检查了 `t=1` 时 `active={a}`、`ready={c}`、`START(c)` 非法。

### 4. Exact oracle、下界和策略

Optional exact DP 枚举所有非空合法子集和 WAIT；work-conserving DP 枚举所有最大合法子集。状态 key
包含每个 task 的 status/remaining，active route 可由 running flows 唯一恢复。相同后继状态被缓存，
最终 action path 重新执行并检查：

- 每条 flow 只有一个连续正长度区间；
- 时间重叠的两条 flow 不共享任何 directed link/NIC resource；
- active flow 未完成前不能从 reservation 中消失；
- compute/flow completion 是唯一决策事件。

安全 residual lower bound 为：

$$
LB(s)=\max\left(
L_{DAG}(s),
\max_{r}\sum_{v:r\in route(v)}d_s(v)
\right).
$$

比较的启发式包括：

- `Dynamic-pack`：按 residual tail 排序，依次装入与 active/已选 routes 兼容的 flow；
- `Resource-pack`：tail 相同时优先 residual resource load 更大的 flow；
- `Bottleneck-pack`：首先处理最大 residual bottleneck load；
- `Rollout-maximal-2`：只从最大兼容集合中选两个候选，用完整 Dynamic-pack 补全评价；
- `Rollout-optional-{2,4}`：允许非最大子集和 WAIT，再用同一端到端补全评价；完整 Dynamic-pack
  始终作为 incumbent，超时回退 incumbent。

### 5. 手工拓扑和 route adapter 验证

除 4 个逻辑资源 motif 外，又通过真实 `BfsStrategy + route_resource_sets` 构造了三个 8-GPU 小图；
资源包括 directed route links、source NIC TX 和 destination NIC RX：

| 拓扑 | 关键冲突 | 多资源 OPT | 单通道 OPT | 单通道高估 |
|---|---|---:|---:|---:|
| single-switch | 四对 GPU 使用互不相交的端口链路 | 9 | 12 | 33.33% |
| two-rack | 两条跨 rack flow 共享 `(8,9)` | 11 | 12 | 9.09% |
| four-rack-core | 同一 rack-pair flow 共享 core links，不同 pair 可并行 | 11 | 12 | 9.09% |

这三个图的 optional exact 分别探索 501/335/273 个状态、869/595/498 个动作，所有启发式都得到
对应 OPT。它们的目的不是模拟真实机器性能，而是透明验证 BFS route、active reservation 和并行
合法性确实贯通。

### 6. 正式 37 图结果

集合包含 4 个手算 motif、3 个 BFS 小拓扑和 seed `260819` 的 30 个 random route-conflict DAG。
比值均相对 non-preemptive optional-idle multi-resource OPT：

| 方法 | 最优数 | 平均比 | 最坏比 | 平均时间 |
|---|---:|---:|---:|---:|
| Dynamic-pack | 30/37 | 1.027245 | 1.500000 | 0.92 ms |
| Resource-pack | 30/37 | 1.023642 | 1.500000 | 0.97 ms |
| Bottleneck-pack | 31/37 | 1.024092 | 1.500000 | 1.10 ms |
| Rollout-maximal-2 | 33/37 | 1.018861 | 1.500000 | 9.81 ms |
| Rollout-optional-2 | **36/37** | **1.001422** | **1.052632** | 11.69 ms |
| Rollout-optional-4 | **36/37** | **1.001422** | **1.052632** | 12.34 ms |

Dynamic-pack 非最优的 hard subset 有 7 图：Resource/Bottleneck 各严格改善 1 图，Bottleneck 修到
exact 1 图；Maximal rollout 改善并修到 exact 3 图；Optional rollout 改善 6 图且全部修到 exact。
唯一未修复的是 `random_join_1`：20 对 OPT 19。top-4 与 top-2 无质量差异，只增加开销。

Optional-idle exact 在 4/37 图优于 maximal work-conserving exact，最大改善 4。Optional rollout
显式主动 WAIT 只发生 2 次；另外的收益来自启动非最大子集，而不是空等整个系统。这说明多资源下
“主动等待”应理解为两种控制：`START(non-maximal S)` 保留部分资源，以及 `WAIT` 保留全部资源。

完整时间线中平均每图约有 5.16 个 Dynamic start events、2.30 个 ready-flow conflict events；
Maximal/Optional rollout 平均评价 11.49/15.89 个候选动作。Exact DP 平均探索 1565.5 个状态、
3636.3 个动作，最大为 10878 状态、26222 动作；所有 37 图均未触及 500000 状态或 30 秒限制。

### 7. 单通道抽象会高估多少

把每图所有 route resource 合并成同一 `single_channel`，再求同语义 optional-idle exact。相对真实
route-resource OPT：

```text
平均高估：15.50%
最大高估：57.14%
```

因此单通道适合研究“顺序与等待”的核心机制，但不能用来估计具体拓扑 makespan。高估大小取决于
route 是否真正共享 directed links/NIC，而不是 GPU 数量本身。

### 8. 理论边界和当前结论

多资源问题同时包含 precedence、不可抢占 route reservation、并行 compatible-set packing 和未来
release。只保留 maximal sets 已被 12/8 反例否定；单资源的 $P+Q$ charging 也不能用于多个可并行
resource loads。当前可安全使用的是上述 `max(critical path, max resource load)` 下界和 exact 小窗口，
尚未得到 Dynamic/Resource/Bottleneck pack 或 Optional set rollout 的一般常数近似比。37 图最坏
经验比不是理论保证。

R4 退出条件已经满足：手算图验证了 active route reservation、不可抢占、WAIT 和非最大启动；三种
真实 BFS 小拓扑验证 route adapter；optional/work-conserving exact 在小窗口完成；所有启发式 action
均通过相同合法性回放。下一阶段 R5 可以把 LLM backbone/deferred/optimizer/dimension 特征转成
完整 `START(S)` 候选，并重点判断是否应给即将释放的 PP/TP 关键 flow保留部分 route resources。

正式输出为 `outputs/nonpreemptive_multiresource/r4_summary.json`。

R0--R4 联合定向回归为 `39 passed`，R4 专项为 `7 passed`。完整 `pytest tests -q` 为
`884 passed, 3 skipped, 18 errors`；18 个 error 仍全部来自缺失的仓库外
`Spectrum-X_8g_8gps_400Gbps_H100` topology fixture，与 R4 无关。

## 修订阶段 R5：LLM DAG 结构特化（2026-08-09）

### 1. 目标和旧实现审计

R5 的目标不是再设计一个 `tail + semantic bonus`，而是回答：LLM metadata 能否提出 general
Full-flow + WAIT rollout 没有覆盖的合法 `START(S)`，并在至少两个 workload/placement 场景中缩短
端到端 makespan。

旧 `study_llm_structured_candidates.py` 的 sidecar 定义仍有用，但候选集合在旧 tick/preemptive
`MultiResourceDAG` 上执行，cache 也可能复用“下一 tick 选择哪些 flow”的旧集合。因此旧语义收益
不能沿用。新实现为 `scripts/study_nonpreemptive_llm_structured.py`，直接建立在 R4 状态机上：每个
candidate 都是启动完整 flow/set 或 WAIT，active flow 不会因新事件被移除。

正式结构 probe 使用真实 pipeline builder、serializer、effective DAG、BFS route 和 directed
link/NIC resources；使用的是仓库内 homogeneous probe profile，不是最终真实 AICB 性能实验。真实
AICB、控制开销与扰动属于 R6。

### 2. General baseline 与 LLM 候选

同预算 general portfolio 包含：

```text
Dynamic-pack
Resource-pack
Bottleneck-pack
SPT-pack
LPT-pack
WAIT（存在 active completion event 时）
```

LLM portfolio 在此基础上增加：

- `backbone_first`：优先完整启动 forward/backward-input 的 PP/TP flows；
- `deferred_gap_fill`：预测下一 backbone release，只启动能在 release 前完成或 route 不冲突的
  DP/backward-weight flow，允许形成非最大集合；
- `optimizer_deadline`：生成优先处理 deferred DP/W 的完整集合，由端到端评价决定是否已到 deadline；
- `dimension_{PP,TP,DP,EP}`：分别生成各通信维度优先的 pack；
- `replica_wavefront`：按 DP/TP replica 坐标生成同类 flow 的另一种完整开始顺序；
- `chunk_wavefront`：按 Ring chunk 次序生成同模板 flow 的另一种 wavefront。

候选评价保持统一。对完整动作 $a$：

$$
\widehat C(s,a)=\Delta(a)+J_{DynamicPack}(T(s,a)).
$$

General 和 semantic 使用相同的 Dynamic-pack completion；semantic 最终显式保留完整 general
rollout schedule 作为 incumbent，因此：

$$
T_{semantic}\le T_{general}\le T_{dynamic}.
$$

这只是实现上的逐实例支配，不是新的常数近似保证。

### 3. `deferred_gap_fill` 的不可抢占含义

令下一条 pending backbone flow 为 $b$，乐观 release 时间为 $\delta_b$。Ready deferred flow $v$
只有满足下列至少一个条件才进入 gap-fill candidate：

$$
p_v\le\delta_b
\quad\text{或}\quad
route(v)\cap route(b)=\varnothing.
$$

第一种表示 $v$ 能在 backbone ready 前完整传完；第二种表示即使仍 active，也不会占用 backbone 的
route。其余长且冲突的 DP/W 被暂缓。这不是抢占：一旦选入 $S$，仍必须完整执行。

固定机制图中，general rollout 只能在“同时启动 long-DP + safe-W”和“全局 WAIT”之间选择，得到
10；gap-fill 只启动 route 不冲突的 safe-W，在 `t=1` 启动 PP，得到 exact OPT 9：

```text
General WAIT:     WAIT [0,1), PP + safe-W from t=1                     => 10
Semantic gap-fill: safe-W [0,3), PP [1,2), long-DP [2,7), two tails  =>  9
```

两个交换 route 标签的等价 motif 都复现 `10 -> 9`。Leave-one-out 中只有删除
`deferred_gap_fill` 会退回 10；删除其他任一语义特征仍为 9。两事件 exhaustive teacher 的 value
coverage 由 general 50% 提升到 semantic 100%，唯一 semantic-only 命中正是 gap-fill。

### 4. 四个完整 pipeline probe

配置固定为 `ga=2,layers=2,quantum=25us`，比较 1F1B/基础双向流水线和两种高冲突 placement：

| 场景 | tasks/flows | Dynamic | General | Semantic | 相对 General 改善 |
|---|---:|---:|---:|---:|---:|
| 1F1B + four-rack-core + TP-cross | 304/160 | 772 | 765 | **762** | **3，0.39%** |
| Bidirectional + four-rack-core + TP-cross | 400/256 | 786 | 775 | **773** | **2，0.26%** |
| 1F1B + two-rack + PP-cross | 304/160 | 766 | 766 | 766 | 0 |
| Bidirectional + two-rack + PP-cross | 400/256 | 780 | 780 | 780 | 0 |

两个正收益场景中，semantic schedule 都只使用了一次 general portfolio 外的动作，标签均为
`chunk_wavefront`。去掉它后分别从 762/773 精确退回 general 的 765/775；这是相对 general
WAIT rollout 的独立 leave-one-feature-out 证据，而不只是相对 Dynamic-pack 的收益。

two-rack PP-cross 没有收益同样重要：LLM metadata 不会自动改善所有拓扑。当前 `chunk_wavefront`
的价值限定为 four-rack TP 冲突中，同模板 Ring chunks 的启动顺序会改变后续 compute/flow release
时间。不能把 0.39%/0.26% 外推成任意模型、拓扑或 placement 的平均收益。

### 5. Teacher coverage

前两个高冲突场景各抽取 Dynamic 时间线前 64 个 ready width 不超过 8 的事件，枚举全部合法
非最大 START subsets 和 WAIT，再用同一 Dynamic completion 得到 one-event teacher：

| 场景 | General value coverage | Semantic value coverage | Semantic-only events |
|---|---:|---:|---:|
| 1F1B four-rack TP-cross | 85.94% | **96.88%** | 7 |
| Bidirectional four-rack TP-cross | 73.44% | **78.13%** | 3 |

Teacher 命中标签主要是 `replica_wavefront` 和 `chunk_wavefront`：1F1B 分别命中 12/10 次，
bidirectional 分别命中 1/4 次。Teacher coverage 的提升大于最终 makespan 提升，因为许多局部等价
或更优动作不在最终关键链上；这再次说明候选覆盖不能替代完整 schedule 指标。

### 6. 周期缓存

缓存只保存压缩 frontier 对应的“候选标签”，命中时必须在当前状态重新生成合法完整动作；不会缓存
task id 集合，更不会恢复或中断 active flow。四个场景的 `(hit/miss, semantic ms, cached ms)` 为：

```text
1F1B four-rack:          67/47, 14071 ->  9380 ms
Bidirectional four-rack: 37/83, 23193 -> 17242 ms
1F1B two-rack:           73/50, 15550 ->  9927 ms
Bidirectional two-rack:  34/99, 26585 -> 23150 ms
```

缓存后四图 makespan 全部与未缓存 semantic 相同，并显式保留 general incumbent。但即使缓存后仍为
9.4--23.2 秒，而 Dynamic greedy 只有约 0.14--0.23 秒；当前仍只能作为离线 teacher，不能声称
调度收益大于在线控制开销。

### 7. R5 结论和退出条件

R5 得到了两个层次的正结果：

1. `deferred_gap_fill` 在两个可手算 PP release motif 上相对 general WAIT rollout 独立改善 10 到 9，
   并由 exact、teacher 和 leave-one-out 三重确认；
2. `chunk_wavefront` 在 1F1B 与 Bidirectional 两种完整 pipeline workload、four-rack TP-cross
   placement 上分别相对 general 改善 3 和 2，删除该特征后收益消失。

因此“至少一个 LLM 特征在两种 workload/placement 上相对 general WAIT rollout 有独立收益”的
R5 退出条件已经满足。但结论严格限定为高冲突结构 probe：two-rack 两个场景均为 0，真实 AICB
尚未闭环，运行时也远大于收益。当前推荐是把 `chunk_wavefront` 和 `deferred_gap_fill` 保留为 R6
候选生成器；其他 backbone/optimizer/dimension 特征保留 sidecar 接口，但在出现独立消融收益前不
进入在线默认策略。

正式输出为 `outputs/nonpreemptive_llm_structured/r5_summary.json`。

R0--R5 联合定向回归为 `45 passed`，R5 专项为 `6 passed`。完整 `pytest tests -q` 为
`890 passed, 3 skipped, 18 errors`；18 个 error 仍全部来自缺失的仓库外
`Spectrum-X_8g_8gps_400Gbps_H100` topology fixture，与 R5 无关。
