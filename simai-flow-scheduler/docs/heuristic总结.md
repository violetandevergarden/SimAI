# LLM 训练通信调度 heuristic 研究总结

本文按“先明确问题，再建立精确标尺，然后逐步加入并行链、一般 DAG、网络拓扑和 LLM 结构”的顺序，
总结目前已经完成的研究。每个阶段都回答五个问题：为什么做、做了什么、实验结果如何、能得到什么结论、
还有什么不能宣称。

全文采用同一套最终模型：DAG 中每个计算和通信节点一旦开始，就必须连续运行到完成；调度器只能在节点完成后
重新决策。通信使用“极化”带宽分配，即一条 flow 启动后独占其所需资源并以全速传完。调度器可以主动等待，
也可以在多资源拓扑上只启动一部分互不冲突的 ready flows，为将来的关键通信保留资源。

## 一、先说最终结论

目前最重要的结果可以概括成八点。

1. **问题语义已经统一并有回归测试。** 每个节点都不可抢占；flow 的执行记录只有一个连续区间；活动 flow
   在完成前持续占用整条 route；计算完成只会产生新的决策机会，不会中断正在传输的 flow。
2. **主动等待确实有用，但不是经常有用。** 在 175 个单通道小 DAG 中，允许等待的最优解只在 10 个图上
   优于“有 ready flow 就必须发送”的最优解。它是少数关键反例所必需的能力，而不是每一步都应使用的常态。
3. **独立并行链已有紧的 2-近似结论。** 任何不主动空闲的不可抢占完整-flow 策略都不超过最优解的 2 倍，
   并且存在一族实例让比值无限接近 2。因此，单纯更换 FIFO、SPT 或 Longest-tail 等优先级，不能突破整个
   work-conserving 策略族的最坏 2 界；要改善最坏情况，必须允许等待或使用更强的结构限制。
4. **Dynamic-tail 是合适的廉价基线，但不是最终算法。** 它每次优先发送“完成后仍连接最长剩余关键工作”的
   ready flow。在 100 个随机并行链实例上，它有 83 个最优，平均比值 1.0144，最坏 1.1875；优点是快、稳、
   易解释，缺点是看不到“等一小会儿会释放更重要 flow”的价值。
5. **小规模端到端试走比直接叠加 bonus 更可靠。** WAIT-aware Rollout-2 在并行链上把最优数从 83/100
   提到 95/100；在 111 个一般 DAG 上达到 107/111。二层 lookahead 在一般 DAG 上达到 110/111。
   这些方法不是只看局部分数，而是假设执行候选动作后，用 Dynamic-tail 把余下过程完整跑完，再比较最终
   makespan。
6. **Join 信息本身有意义，但简单 join bonus 没有实际收益。** 原始 bonus 在 111 个一般 DAG 中从未优于
   Dynamic-tail，反而在 25 个图上更差。把 join 用来生成候选确实改变过决策，但在同预算端到端评估下没有
   产生独立收益。因此当前不应把 join 奖励直接加到 tail 分数上。
7. **具体拓扑把“选一条 flow”变成了“选一个兼容 flow 集合”。** 37 个多资源小图上，允许 WAIT 和非最大
   启动集合的 Rollout-2 有 36/37 个最优，平均比值 1.0014；只枚举最大兼容集会漏掉最优解。把所有 route
   压成单通道会平均高估最优 makespan 15.50%，最大高估 57.14%。
8. **LLM 结构已经显示出小而可复现的额外价值，但还不能称为真实 workload 闭环。** `deferred_gap_fill`
   在两个可手算 motif 中把 10 改善到最优 9；`chunk_wavefront` 在 1F1B 和 Bidirectional 的高冲突
   pipeline probe 上，相对通用 rollout 分别改善 0.39% 和 0.26%。另外两个 placement 收益为 0，且当前
   结构 rollout 仍需数秒到数十秒，只适合离线 teacher。

因此，现在可以说 **R0–R5 的算法研究闭环已经完成**：有统一语义、精确 oracle、反例、理论界、一般 DAG、
拓扑和 LLM 结构实验。但还不能说生产执行闭环已经完成：真正的 AICB workload、独立不可抢占 executor、
在线控制开销和输入扰动验证仍属于下一阶段。

## 二、现在研究的到底是什么问题

### 2.1 调度对象是 effective DAG

Workload 中的显式依赖只描述数据因果关系，GPU 上的计算还必须遵守 pipeline serializer 给出的执行顺序。
所以真正参与调度分析的图是

$$
G_{\mathrm{eff}}=G_{\mathrm{data}}\cup E_{\mathrm{compute\ order}}.
$$

调度器不能凭任务名字猜依赖，也不能用优先级代替真实依赖。集合通信内部的 P2P flows、collective join、
PP 前后向关系和 optimizer gating 都应由 DAG 表达；serializer 只补充计算资源上的合法顺序。

### 2.2 单通道模型

单通道用于研究最纯粹的“通信开始顺序”问题：

- 每个通信节点有时长 $p_v$，启动后占满通道直至完成；
- 不同依赖链上的计算可以并行，依赖满足后自动启动；
- ready flow 可以立即启动，也可以主动 `WAIT` 到下一个正在运行的节点完成；
- 目标是最小化最后一个 DAG 节点的完成时间，即 makespan。

`WAIT` 不是按任意微小时间步试探，而是跳到下一个有信息变化的完成事件，所以搜索空间仍是离散的。

### 2.3 多资源拓扑模型

具体拓扑中，每条 flow $v$ 有一个完整资源集合 $R_v$，包含有向链路以及需要时的 NIC-TX/NIC-RX。
若活动 flow 已占用资源并集 $U$，一次合法启动动作 $S$ 必须满足

$$
S\subseteq Ready,\qquad
R_u\cap R_v=\varnothing\;(u\ne v),\qquad
\left(\bigcup_{v\in S}R_v\right)\cap U=\varnothing.
$$

也就是说，动作不再是“选哪一条”，而是“同时启动哪一个完整、互不冲突的 flow 子集”。新事件到来时，
尚未完成的 flow 保留剩余时间和 route reservation。

### 2.4 三种数字必须区分

- `makespan / OPT`：有 exact oracle 时，才是该实例真正的最优比；
- `makespan / LB`：只是相对安全下界的 gap，不是近似比；
- 样本中的最大比值：只是当前测试发现的最差实例，不能替代理论最坏界。

## 三、比较过的方法都是什么

### 3.1 简单优先级

| 方法 | 通俗解释 | 主要局限 |
|---|---|---|
| FIFO | 谁先 ready 就先传谁 | 完全不看后续影响 |
| SPT | 先传最短 flow | 容易推迟能释放长计算链的较大 flow |
| LPT | 先传最长 flow | 可能让短而关键的解锁流等待太久 |
| Longest-delay | 优先选择紧随其后的计算最长者 | 只看下一段计算，不看更深 DAG |
| Longest-tail | 优先选择其后最长剩余路径最大的 flow | 静态版本不能反映已完成工作和关键分支切换 |
| Dynamic-tail | 每个完成事件后，按剩余 DAG 重新计算 Longest-tail | 仍是局部、默认立即工作的规则 |
| LRPT | 当前通信长度和后续 tail 一起考虑 | 容易再次偏向大 flow |
| Earliest-slack | 先做离预计截止点最近的 flow | 截止点本身通常只是近似估计 |
| TicTac-style | 对两个候选的先后顺序做局部比较 | 只看很浅的交换，不是完整搜索 |

Dynamic-tail 是全文主要廉价基线。它不是说“当前 flow 最大”，而是问：“如果现在把这条 flow 做完，
它后面还有多少无法回避的关键工作？”随着节点完成，这个剩余 tail 会重新计算。

### 3.2 Rollout、Beam、Monte Carlo 和精确搜索

- **Rollout-k**：筛出最多 $k$ 个当前动作；分别假设先执行每个动作，然后用 Dynamic-tail 把剩余 DAG
  完整调度到结束；选择预测 makespan 最小者。WAIT-aware 版本还把等待作为候选。
- **Depth-2 rollout**：不仅试当前一步，还试下一决策事件的第二步，再用基线补全。它比一层 rollout
  更容易看见“先解锁、再抢占关键顺序”的组合效果。
- **Beam-8/32**：同时保留估值最好的 8 或 32 个部分调度继续扩展。宽度固定，所以仍可能把真正的好分支
  提前剪掉。
- **Monte Carlo-64**：随机生成 64 个完整合法调度，采样时较偏向高 tail flow，最后取其中最好者。
  它简单、可并行、随时可停，但固定采样数没有最优保证。
- **Memoized DP**：把“各任务状态、剩余时长、活动资源和当前事件时间”压成 residual state；枚举所有
  合法动作并缓存重复状态，得到小实例的精确最优解。
- **二分 makespan + feasibility DP**：二分一个目标完成时间 $H$，用 DP 判断是否存在不超过 $H$ 的调度。
  它适合做 deadline 判断和缩紧界，但可能为多个 $H$ 重复搜索。
- **Branch-and-bound**：深度优先枚举调度，用当前最好解作上界，用安全 lower bound 剪掉不可能更优的分支。
  它与 DP 独立实现，用于交叉验证 oracle。

### 3.3 多资源和 LLM 特化候选

- **Dynamic-pack**：按 Dynamic-tail 排序，把能与活动/已选 routes 共存的 ready flows 贪心装入一个集合。
- **Resource-pack**：在 tail 基础上，更偏向经过剩余负载较高资源的 flow。
- **Bottleneck-pack**：优先清理最拥堵资源上的 flow。
- **Optional set rollout**：既评估最大兼容集合，也允许只启动非最大子集或全部等待。
- **Backbone-first**：生成优先启动 forward/backward-input 的 PP/TP 主干流候选。
- **Deferred gap-fill**：只用能在下一主干流释放前传完、或与其 route 不冲突的 W/DP flow 填空隙。
- **Optimizer-deadline**：生成优先完成接近 optimizer barrier 的 W/DP flow 集合。
- **Dimension candidates**：分别生成 PP、TP、DP、EP 优先的候选，而不是把维度直接加成一个总分。
- **Replica wavefront**：利用 DP/TP replica 的重复坐标生成另一种推进顺序。
- **Chunk wavefront**：利用 Ring 的 `chunk_id/num_chunks`，改变同模板 chunk 的启动波次。

LLM 特征的主要作用是**提出少量结构化候选**。所有候选仍用同一端到端 makespan 估值比较；没有把多个
语义 bonus 生硬相加。

## 四、阶段 0 / R0：DAG 语义审计与特征提取

### 目的

先确定“什么才是一个合法调度”，并找出 LLM DAG 中真正可利用的规律。若语义不统一，后续的最优解、
反例和近似证明都会比较不同的问题。

### 做了什么

建立了统一的事件状态机和 trace 校验：

- FLOW 和 COMPUTE 都是原子、不可抢占节点；
- 正时长 flow 必须对应唯一连续区间；零时长 compute 可在同一时刻完成；
- compute 依赖满足后立即开始，完成时才产生新的 ready 节点；
- 单通道任意两条 flow 不重叠；多资源中重叠 flow 的 route 资源必须互不相交；
- `WAIT` 只跳向下一个合法完成事件，不产生无意义的连续时间分支；
- effective DAG 同时包含数据依赖和 serializer 的计算顺序边。

DAG 审计还发现了一些对算法有用的结构：

1. Ready frontier 通常远小于整个 DAG，适合在局部窗口做精确或半精确搜索；
2. TP/DP/PP collective、micro-batch 和 layer 会重复出现，许多局部状态具有模板相似性；
3. 关键路径会随着任务完成而换分支，因此动态 tail 比只算一次的静态优先级更合理；
4. Fork/join 很多，单看一条链的局部得分可能忽略“最后一个 join 输入”；
5. 不同并行维度并非真正独立：它们通过计算依赖、optimizer barrier 和共享 route 发生耦合。

### 结论

“把 TP、DP、PP 各自求解后直接组合”一般不可行，但维度分解仍有价值：它可以用于识别模板、压缩状态、
生成候选和估计各资源负载。真正的选择仍必须回到统一 DAG 和共享拓扑上评价。

## 五、阶段 1 / R1：Benchmark 与精确 Oracle

### 目的

为 heuristic 建立可信标尺。没有 exact OPT，只能说某方法比另一个方法好，无法知道它离真正最优还有多远。

### 做了什么

实现了两个独立 exact solver：memoized residual DP 和 forward branch-and-bound；二者都分别求
optional-idle OPT 与 work-conserving OPT。安全下界包括：

$$
LB=\max\{\text{剩余通信工作量},\ \text{剩余 DAG 关键路径},\ \text{release-tail 子集下界}\}.
$$

所有最优 action path 都重新回放，检查节点只执行一次、flow 区间连续、依赖满足和资源不冲突。

### 实验结果

- 175 个小 DAG，共完成 700 次 exact 求解；DP 与 B&B 的最优值全部一致；
- 10/175 个图中，optional-idle OPT 严格优于 work-conserving OPT；
- 两个普通 random-join 图的两种 OPT 相同；两个特意保留 release 冲突的图分别为 `17 vs 20`、
  `22 vs 23`，证明等待收益不是 oracle 偶然误差。

### 复杂度结论

问题至少包含经典单机带 release time 和 delivery tail 的问题。把一个 job 写成

```text
initial compute(r_j) -> communication(p_j) -> final compute(q_j)
```

即可保持其 release、机器加工时间、tail 和 makespan。因此目标问题包含强 NP-hard 的
$1|r_j,q_j|C_{\max}$；DP、二分和 B&B 的主要定位应是小窗口 oracle、离线 teacher 和反例搜索器，
而不是任意规模的全图在线算法。

### 本阶段结论

精确标尺可靠，主动等待是模型中不可删除的动作，但只需在有明确未来释放价值时考虑。正式结果位于
`outputs/nonpreemptive_oracle/r1_summary.json`。

## 六、阶段 2 / R2：独立并行链问题

### 目的

先研究最简单但仍有代表性的结构：若干条互相独立的链，每条链由“计算释放—通信—计算—通信……”交替
构成，所有链共享一个不可抢占通信通道。它对应多个 micro-batch 或通信维度暂时只通过瓶颈竞争耦合的情况。

### 主要实验

100 个固定随机实例上的结果如下，所有比值均相对 exact optional-idle OPT：

| 方法 | 最优数 | 平均比值 | 最坏比值 | 平均时间 |
|---|---:|---:|---:|---:|
| Dynamic-tail | 83/100 | 1.014438 | 1.187500 | 约 0.18 ms |
| Rollout-flow-2 | 91/100 | 1.005246 | 1.115385 | 约 1.83 ms |
| Rollout-flow-4 | 91/100 | 1.005246 | 1.115385 | 约 2.74 ms |
| Rollout-WAIT-2 | 95/100 | 1.002338 | 1.055556 | 约 2.55 ms |
| Rollout-WAIT-4 | 95/100 | 1.002338 | 1.055556 | 约 3.37 ms |
| Beam-WAIT-8/32 | 样本中 100/100 | 1.000000 | 1.000000 | 明显更高 |

Beam 的 100/100 不是精确性证明。固定反例上 `OPT=46`，Beam-flow-8、Beam-WAIT-8、
Beam-WAIT-32 都得到 47，只有 Beam-flow-32 得到 46。增加 beam width 或 top-k 也不保证单调解决问题。

在三类专门困难集上：

- idle-hard：7 个，必须识别等待价值；
- ordering-hard：13 个，不等待也要看对完整 flow 顺序；
- combined-hard：3 个，同时需要等待和正确排序。

Top-4 相对 Top-2 没有额外收益；Monte Carlo-64 与 flow rollout 同为 91/100。把 WAIT 加进随机采样还会
稀释有限样本：有时改善，有时反而错过好的工作型顺序。

### 2-近似定理与紧例

对独立链，令

- $P$ 为全部通信时长之和；
- $Q$ 为任意一条链上的最大总计算时长，包括初始 release compute。

任取一个 work-conserving 完整-flow 调度 $H$。由于单通道，完成时间可写为

$$
T_H=P+I_H,
$$

其中 $I_H$ 是通信通道被迫空闲的总时间。取最终完成的那条链：每一段被迫空闲时，所有未完成链都没有
ready flow，因此这条最终链必然正在执行计算；这些互不重叠的空闲段可全部注入该链的计算，故

$$
I_H\le Q.
$$

另一方面，任意允许主动等待的最优解都满足 $OPT\ge P$ 且 $OPT\ge Q$。于是

$$
T_H\le P+Q\le 2OPT.
$$

这个 2 是紧的。构造两条链：

```text
A: communication(M), t=0 ready
B: compute(1) -> communication(1) -> compute(M)
```

任何 work-conserving 策略在 `t=0` 只能先做 A，得到 $2M+1$；允许等待的最优解先等 1，再做 B 的通信，
随后做 A，得到 $M+2$。比值

$$
\frac{2M+1}{M+2}\to 2.
$$

因此 FIFO、SPT、LPT、Dynamic-tail 等所有“有 ready flow 就必须发”的策略族，都无法获得严格小于 2
的一般保证。若 rollout 始终保留 Dynamic-tail 的完整调度作为 incumbent，它在独立链上也继承不超过 2
的保证；但实验上的 1.0556 不能写成新的理论上界。

### 特殊子类

- 每条链只有一条且都在 `t=0` ready 的 flow：Longest-delay 和 Dynamic-tail 在测试中 100% 最优；
- 没有计算间隔：所有基础规则都最优，因为总时间固定为通信总量；
- 等长通信的 729 个枚举实例：Dynamic-tail/LRPT 100% 最优；
- 计算 lag 非增的 576 个实例：Dynamic-tail 93.92% 最优，观察最坏 1.2。

这些是结构性线索，不应在没有证明时推广成定理。

### DP 是否实用

DP 对很小窗口很有价值，但状态随链数、flow 数和离散时长迅速增长：7 条 flow 约 623 个状态，14 条约
19411 个状态，15 条已容易触发 30000 状态或 2 秒限制。可行方向是只对当前小 frontier 做 DP，或用
Dynamic-tail 上界、二分和 dominance rule 强剪枝。

正式结果位于 `outputs/nonpreemptive_parallel_chains/r2_summary.json`。

## 七、阶段 3 / R3：从并行链推广到一般 DAG

### 目的

一般 LLM DAG 有 fork、join、collective completion 和 optimizer gate，不能分成互不影响的链。本阶段
检验并行链方法还能保留多少效果，以及 join 信息应怎样使用。

### 做了什么

在 residual DAG 上计算四类信息：ready 集合、动态关键 tail、未来 release、join 最后阻塞关系。比较
Dynamic-tail、原始 join bonus、flow/WAIT rollout、混合候选、depth-2 和 Beam-WAIT。

关键设计是：join、release 等特征只负责提出候选；候选价值由“执行动作后，再完整调度到终点”的
makespan 统一决定。所有增强方法保留 Dynamic-tail 完整结果作为 incumbent，因此逐实例不会比基线更差。

### 实验结果

111 个 DAG 包含 7 个 ordering 反例、1 个 join-bonus 反例、3 个组合反例和 100 个 random-join 图：

| 方法 | 最优数 | 平均比值 | 最坏比值 | 平均时间 |
|---|---:|---:|---:|---:|
| Dynamic-tail | 97/111 | 1.010168 | 1.176471 | 约 1.14 ms |
| 原始 join bonus | 72/111 | 1.027700 | 1.190476 | 同量级 |
| Rollout-flow-2 | 101/111 | 更优于基线 | — | 数毫秒级 |
| Rollout-WAIT-2 | 107/111 | 接近 1 | — | 数毫秒级 |
| Depth-2 top-2/top-4 | 110/111 | 1.000429 | — | 约 34/40 ms |
| Beam-WAIT-8 | 110/111 | 1.000375 | — | 更高 |

困难集上，WAIT-2 对 ordering、idle、combined 分别改善 7/7、9/10、3/3；Depth-2 分别改善
7/7、10/10、3/3，并把绝大多数修到 exact。

### Join bonus 为什么没起效果

原始 join bonus 想奖励“完成后能让 join 更早满足”的 flow，但它有三个问题：

1. tail 已包含下游关键长度，再加 bonus 容易重复奖励同一件事；
2. 乐观 earliest-finish 没有计入其它 flow 的排队与 route 冲突；
3. 一个局部 join 提前，并不等于全局最后完成时间提前。

实验中，原始 bonus 相对 Dynamic-tail 改善 0 个、相同 86 个、变差 25 个。Join-aware 候选在 24 个图、
38 个事件中确实进入过 top-2，但最终没有超过同预算普通 rollout。这说明不是“join 完全无信息”，而是
当前问题中它的边际信息不足以单独改变端到端结果。

### 理论边界

独立链的 2-近似证明依赖“所有 forced idle 都可注入最终完成链的一条计算路径”。一般 fork/join DAG 中，
不同空闲段可能由互不可比的分支阻塞，无法保证它们都属于同一条原 DAG 路径。因此目前**不能**把 R2 的
紧 2 定理直接推广到任意 DAG。

当前能严格保证的是：若 rollout/Beam 始终保留基线的完整 schedule，则返回结果逐实例不差于基线。
111 图上的 1.176 最坏值只是实验结果，不是一般近似比。

正式结果位于 `outputs/nonpreemptive_general_dag/r3_summary.json`。

## 八、阶段 4 / R4：多资源拓扑扩展

### 目的

单通道能研究顺序，却会把实际可以并行的不同 route 强制串行。本阶段把不可抢占语义扩展到 directed link
和 NIC 资源，研究完整 flow 集合的启动时机。

### 为什么不能只启动最大兼容集合

下面的手算图有两个独立资源：

```text
t=0 ready:
  a: comm(4), route r0
  b: comm(5), route r1

release_c: compute(1) -> c: comm(1), route r1 -> compute(6)
```

若要求 `t=0` 启动最大兼容集 `{a,b}`，则 `c` 在 `t=1` ready 后仍要等 b，最终为 12。最优动作只启动
非最大集合 `{a}`，在 `t=1` 启动 c，之后再做 b，最终为 8。

所以“现在还能多塞一条不冲突 flow，就一定应该塞”是错误的。多资源中的主动保留有两种形式：

- `WAIT`：暂时不启动任何新 flow；
- `START(non-maximal S)`：只保留未来关键 flow 需要的部分资源，其它资源仍工作。

### 实验设计与结果

实现 optional exact DP，枚举所有非空合法子集和 WAIT；work-conserving exact 只枚举 inclusion-maximal
集合。另用 BFS route adapter 构造 single-switch、two-rack、four-rack-core 三个 8-GPU 小拓扑，资源
包括有向链路和端点 NIC。

37 个实例由 4 个机制图、3 个 BFS 拓扑图和 30 个随机 route-conflict DAG 组成：

| 方法 | 最优数 | 平均比值 | 最坏比值 | 平均时间 |
|---|---:|---:|---:|---:|
| Dynamic-pack | 30/37 | 1.027245 | 1.500000 | 0.92 ms |
| Resource-pack | 30/37 | 1.023642 | 1.500000 | 0.97 ms |
| Bottleneck-pack | 31/37 | 1.024092 | 1.500000 | 1.10 ms |
| Rollout-maximal-2 | 33/37 | 1.018861 | 1.500000 | 9.81 ms |
| Rollout-optional-2 | **36/37** | **1.001422** | **1.052632** | 11.69 ms |
| Rollout-optional-4 | **36/37** | **1.001422** | **1.052632** | 12.34 ms |

Optional exact 在 4/37 个图上严格优于 maximal work-conserving exact，最大改善 4。显式全局 WAIT 只出现
2 次，其余收益来自非最大启动集合。Top-4 与 Top-2 没有质量差异。

三个 BFS 图的多资源 OPT 分别为 9、11、11；压成单通道后为 12、12、12。全体 37 图上，单通道相对
多资源最优值平均高估 15.50%，最大高估 57.14%。这说明拓扑大小不是关键，关键是 ready flows 的 route
是否真的共享链路/NIC，以及未来关键 flow 是否会与当前长 flow 冲突。

### 理论结论

安全下界为

$$
LB(s)=\max\left\{L_{DAG}(s),\ \max_r\sum_{v:r\in R_v}p_v(s)\right\}.
$$

多资源问题同时包含 precedence、不可抢占 route reservation、兼容集 packing 和未来 release。
单资源的 $P+Q$ 证明不能直接套用，目前尚未得到 Dynamic/Resource/Bottleneck-pack 或 Optional rollout 的
一般常数近似比。37 图上的最坏 1.0526 仍只是观察值。

正式结果位于 `outputs/nonpreemptive_multiresource/r4_summary.json`。

## 九、阶段 5 / R5：利用 LLM DAG 特殊结构

### 目的

检验 LLM metadata 是否能提出通用 rollout 没覆盖的有价值动作，而不是仅给现有动作换一个更复杂的分数。

### 候选如何利用 LLM 规律

最关键的两个规则是：

1. **Deferred gap-fill。** 设下一条 backbone flow 还需 $\delta_b$ 才 ready。一个 ready 的 deferred
   flow $v$ 只有在

   $$
   p_v\le\delta_b\quad\text{或}\quad R_v\cap R_b=\varnothing
   $$

   时才用于填空。前者保证它在主干释放前完整传完，后者保证即使仍在运行也不会挡住主干。
2. **Chunk/replica wavefront。** LLM collective 在不同 layer、micro-batch、replica 和 Ring chunk 上重复。
   当 tail 看起来接近时，按这些重复坐标生成另一种完整启动波次，让 rollout 判断是否能更早释放后续计算。

此外还实现了 backbone、optimizer deadline、PP/TP/DP/EP 维度候选，但只有在消融实验中产生独立收益的
特征才应进入推荐集合。

### 可手算机制图

两个交换 route 标签的 gap-fill motif 中：

```text
General rollout:      WAIT 或同时启动 long-DP + safe-W  => 10
Semantic gap-fill:    只先启动 safe-W，t=1 启动 PP      =>  9
```

9 是 exact OPT。删除 `deferred_gap_fill` 后结果退回 10，删除其它任一语义特征仍为 9；这证明收益确实来自
gap-fill，而不是候选数量偶然增加。

### 完整 pipeline probe

实验使用真实 pipeline builder、serializer、effective DAG、BFS route 和 directed link/NIC resources；
profile 是统一的结构探针，配置为 `ga=2, layers=2, quantum=25us`：

| 场景 | tasks/flows | Dynamic | General | Semantic | 相对 General 改善 |
|---|---:|---:|---:|---:|---:|
| 1F1B + four-rack-core + TP-cross | 304/160 | 772 | 765 | **762** | **0.39%** |
| Bidirectional + four-rack-core + TP-cross | 400/256 | 786 | 775 | **773** | **0.26%** |
| 1F1B + two-rack + PP-cross | 304/160 | 766 | 766 | 766 | 0 |
| Bidirectional + two-rack + PP-cross | 400/256 | 780 | 780 | 780 | 0 |

两个正收益场景都只使用了一次 general portfolio 之外的 `chunk_wavefront` 动作。去掉该特征后，结果分别
精确退回 765 和 775。另两个场景为 0，说明 metadata 的价值依赖具体 route 冲突，不能写成普遍收益。

### Teacher coverage 和运行开销

在两个高冲突场景各抽取 64 个小 frontier，枚举全部合法子集和 WAIT 作为 one-event teacher：

| 场景 | General coverage | Semantic coverage | Semantic-only 事件 |
|---|---:|---:|---:|
| 1F1B four-rack | 85.94% | **96.88%** | 7 |
| Bidirectional four-rack | 73.44% | **78.13%** | 3 |

命中主要来自 replica/chunk wavefront。但局部 coverage 的提升明显大于最终 makespan 提升，因为许多局部
好动作不在最终关键路径上。

周期缓存只保存压缩 frontier 对应的候选标签，命中后仍在当前状态重新生成合法完整动作。它保持 makespan
不变，并把四个场景的 semantic 时间从约 14.1/23.2/15.6/26.6 秒降至 9.4/17.2/9.9/23.2 秒。
Dynamic greedy 只需约 0.14–0.23 秒，所以 semantic rollout 当前适合作为离线 teacher，不适合作为已经成熟
的在线默认策略。

### 本阶段结论

LLM 规律的实用方式不是假设各并行维度完全独立，也不是做一个巨大的加权分数，而是：

```text
重复结构/语义 sidecar
  -> 少量完整、合法、彼此不同的 START(S) 候选
  -> 同一个端到端 rollout 评价器
  -> 保留通用策略作为 incumbent
```

当前推荐保留 `deferred_gap_fill` 和 `chunk_wavefront` 进入下一阶段；其它语义候选继续作为实验接口，直到有
独立消融收益。正式结果位于 `outputs/nonpreemptive_llm_structured/r5_summary.json`。

## 十、现在算不算闭环

### 已经闭合的部分

- 从统一不可抢占语义到 exact oracle；
- 从反例发现到理论证明；
- 从并行链到一般 fork/join DAG；
- 从单瓶颈到 route/NIC 多资源；
- 从通用 heuristic 到 LLM 结构候选；
- 每个阶段都有固定样本、正式 JSON 输出、trace 回放和定向测试。

R0–R5 联合定向回归为 `45 passed`。完整测试为 `890 passed, 3 skipped, 18 errors`；18 个 error 全部来自
缺失的仓库外 `Spectrum-X_8g_8gps_400Gbps_H100` topology fixture，与本研究实现无关。

### 尚未闭合的部分

- 研究状态机尚未接成独立的生产级不可抢占 executor；
- R5 用的是结构 probe，不是 `inputs/aicb-workload` 中真实 workload 的最终性能扫描；
- 尚未系统加入 profile 误差、release 时间扰动和控制延迟；
- Semantic rollout 的运行时间远高于 greedy，在线预算尚未达标；
- 一般 DAG 和多资源 topology 尚无常数近似保证；
- LLM 特征只在两个高冲突场景出现小幅独立收益，覆盖面仍窄。

所以准确表述是：**算法探索闭环，生产与真实 workload 验证尚未闭环。**

## 十一、现在最值得利用的规律

1. **小 frontier。** 全图很大不代表每次决策很宽，可以只对 ready 集和近期 release 做小窗口精确搜索。
2. **动态关键链。** 关键分支会切换，应在完成事件后重算 tail，而不是使用一次性静态优先级。
3. **等待是稀疏动作。** 大多数状态应立即工作，只在近期将释放高 tail 且 route 冲突的 flow 时考虑等待。
4. **非最大集合比全局等待更常用。** 多资源下经常只需空出一部分 route，而不是让整个网络停止。
5. **重复模板可压缩搜索。** Layer、micro-batch、replica、dimension 和 chunk 可用于缓存候选标签和复用排序。
6. **维度可用于提议，不能独立求解后硬拼。** TP/DP/PP/EP 会在 DAG join、计算序列和物理链路上重新耦合。
7. **端到端评价优先于 bonus。** 一个 flow 看起来更关键，不等于它让最终完成时间更短；少量候选加完整补全
   比不断增加局部权重更可靠。
8. **拓扑冲突强度决定可调度空间。** GPU 或交换机数量大不必然意味着冲突少；应直接看 ready flows 的 route
   交集、NIC 冲突和未来 release。

## 十二、下一步建议

最合理的下一步是 R6：建立独立、可审计的不可抢占执行闭环，而不是继续堆更多 heuristic 名字。

1. **接入真实 AICB。** 从 `inputs/aicb-workload` 选择不同模型大小、TP/DP/PP/GA 配置，使用真实 builder、
   serializer、topology 和 route 生成实验。
2. **实现隔离 executor/policy。** 保持通用 schema 和现有 baseline 不变；只在研究路径中实现 whole-flow start、
   route reservation、completion-event replanning 和 optional idle。
3. **分层比较。** 至少比较 Dynamic-pack、General Optional Rollout-2、加入 gap-fill/chunk 的 Semantic、以及小
   frontier exact teacher；同时报告相对默认顺序的收益。
4. **不只看 makespan。** 记录 ready queue delay、关键 flow unlock delay、WAIT/非最大启动次数、route overlap、
   候选数、每次决策时间和总控制开销。
5. **做反事实消融。** 分别关闭 gap-fill、chunk、replica 和缓存，确认收益来源；对没有收益的特征不进入默认集。
6. **做鲁棒性实验。** 扰动通信时长、计算 release、路由和 profile，检查计划误差是否会让主动等待适得其反。
7. **压缩在线预算。** 用 exact/semantic rollout 生成 teacher 数据，再蒸馏成阈值规则、决策树或小型 value model；
   在线仅评估 2–4 个高价值候选。
8. **继续理论工作。** 对 bounded frontier、series-parallel DAG、等长 collective chunk、固定 route 数等 LLM
   子类寻找严格界；一般多资源问题则先寻找反例，不应从小样本直接猜常数比。

## 十三、理论与参考文献

### 本研究中已经证明的内容

- 独立并行链上，任意 work-conserving 不可抢占完整-flow 调度是 2-近似；
- 两链参数族证明该 2 对整个 work-conserving 策略族是紧的；
- 保留完整 baseline incumbent 的 rollout/Beam 逐实例不差于 baseline；
- 多资源中“只枚举最大兼容集”不具支配性，`12 vs 8` 反例已经给出；
- 一般 DAG 与多资源情况下尚无新的常数近似证明。

这些证明都针对本文明确写出的模型，不能自动外推到允许流共享带宽、可拆分 flow 或不同资源语义的问题。

### 相关文献

1. Hall, L. A. and Shmoys, D. B., *Jackson's Rule for Single-Machine Scheduling*, Mathematical
   Operations Research, 1992. 该文讨论 Jackson rule、无 precedence 情形的 PTAS，以及带 precedence
   情形的近似结果：[DOI](https://doi.org/10.1287/moor.17.1.22)。
2. Vakhania, N., *Single-Machine Scheduling with Release Times and Tails*, Annals of Operations
   Research. 该文系统讨论 release-time/tail 模型及若干特殊可解情形：
   [DOI](https://doi.org/10.1023/B:ANOR.0000030692.69147.e2)。

本文使用这些文献来定位 $1|r_j,q_j|C_{\max}$ 的复杂性和经典排序背景；R2 的紧 2 证明、多资源非最大集合
反例以及 R3–R5 的实验结论均为项目当前模型下的独立推导与验证。

## 十四、代码、测试与正式结果入口

### 核心实现

- `scripts/nonpreemptive_dag_model.py`：统一不可抢占 DAG 状态机；
- `scripts/nonpreemptive_dag_oracle.py`：DP、B&B、下界和 trace 回放；
- `scripts/study_nonpreemptive_oracle.py`：R1 benchmark；
- `scripts/study_nonpreemptive_parallel_chains.py`：R2 并行链算法、反例和理论实验；
- `scripts/study_nonpreemptive_general_dag.py`：R3 一般 DAG；
- `scripts/study_nonpreemptive_multiresource_dag.py`：R4 route-resource 拓扑；
- `scripts/study_nonpreemptive_llm_structured.py`：R5 LLM 结构候选与 pipeline probe。

### 正式结果

- `outputs/nonpreemptive_oracle/r1_summary.json`
- `outputs/nonpreemptive_parallel_chains/r2_summary.json`
- `outputs/nonpreemptive_general_dag/r3_summary.json`
- `outputs/nonpreemptive_multiresource/r4_summary.json`
- `outputs/nonpreemptive_llm_structured/r5_summary.json`

更详细的阶段过程、固定反例和测试命令见 `docs/heuristic进度.md`；后续任务与退出条件见
`docs/heuristic规划.md`。
