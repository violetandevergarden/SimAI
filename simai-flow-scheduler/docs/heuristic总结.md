# LLM 训练通信调度 heuristic 研究总结

本文把整个研究过程按“先弄清问题，再建立精确标尺，然后逐步增加 DAG、拓扑和真实执行语义”的顺序总结。目标不是罗列脚本和算法名字，而是回答五个问题：每个阶段为什么做、具体做了什么、实验说明了什么、现在能下什么结论、下一步还缺什么。

## 一、先说最终结论

这项工作已经形成了一个**模拟器内部的实验闭环**：

```text
真实 AICB profile
  → 真实 workload builder / pipeline serializer
  → effective DAG
  → 真实 topology 与 BFS route
  → 按 DAG tail 分层的 progressive-filling max-min allocator
  → AnalyticalExecutor
  → makespan、冲突、重分配开销和 profile 扰动结果
```

最重要的结论可以概括成六点。

1. **理论安全底座已经明确。** 在“所有通信共享一个瓶颈、通信可抢占、计算可并行且依赖正确”的模型中，任何不故意让网络空闲的调度都有 2-近似保证。这个 2 对任意 work-conserving 优先级族是紧的，但尚不能证明 Longest-tail 等具体算法的最坏比就是 2。
2. **最可靠的低成本基线是 Residual Dynamic-tail。** 它总是优先处理“完成后还有最长关键后续工作”的 ready flow，并在事件发生后按剩余 DAG 重算。它比 FIFO、SPT、LPT、静态 tail、LRPT 和简单 join bonus 更稳定。
3. **有限前瞻有效，但应看完整端到端后果。** 最有效的增强不是把各种特征线性相加，而是提出少量候选动作，分别模拟到下一个事件，再用基线补全并比较最终 makespan。困难一般 DAG 上，这种 next-event counterfactual rollout 修复了 20 个 Dynamic-tail 失败实例中的 19 个，平均关闭 95% 的最优差距。
4. **真实拓扑改变了问题本身。** 单 channel 会把本可并行的 flow 强制串行。在真实 route 窗口中，它平均可高估 makespan 83.33%。拓扑下的动作应是“可同时推进的 flow 集合”或“带宽分配”，而不只是给所有 flow 排一个全序。
5. **LLM 结构有用，但主要用于少量候选生成和状态压缩。** DP replica 顺序、Ring chunk、通信维度和 pipeline 周期能够补足纯 tail 看不到的对称性。最终真实 executor 中，真正观察到独立收益的是带 placement guard 的 replica wavefront；许多看似合理的 backbone、optimizer、chunk 特征尚无独立收益证据。
6. **真实收益存在，但不是所有 PP 策略都有。** 在真实 AICB + max-min executor 中，strict Dynamic-tail 对 Bidirectional/Chimera 在 GPT-7B/13B/22B、多组 GA、DP=2/4、两种 placement 下均有正收益，典型为 1.21%--15.55%；普通 1F1B 基本没有收益。因此目前可以宣称“研究实验闭环”，不能宣称“通用上线算法闭环”或“多资源常数近似已经解决”。

## 二、问题到底是什么

### 2.1 effective DAG，而不只是 workload DAG

模拟器中的 workload DAG 只表示显式数据依赖，但 GPU 上的计算还必须遵守 serializer 给出的 `compute_order`。因此真正参与调度分析的图是

$$
G_{\mathrm{eff}}
=G_{\mathrm{data}}
\cup E_{\mathrm{compute\ order}}.
$$

集合通信的多条 P2P flow、collective completion join 和显式 optimizer gating 已经由 workload 依赖表达，不应再根据任务名字猜边。调度器只控制通信；计算一旦依赖满足就按既定设备顺序执行。

### 2.2 单瓶颈研究模型

基础理论把所有通信看成共享一个容量为 1 的 channel。通信可在离散时间边界抢占，ready compute 自动执行且不同链上的计算可以并行。目标是最小化整个 DAG 的完成时间（makespan）。

记

- $P$：全部通信工作量；
- $Q$：任意一条依赖链上的最大纯计算量；
- $L$：忽略通信争用后的 weighted critical path。

这个模型有意简单：它适合研究“当前先推进哪条通信”，并能获得精确解和近似证明；它不表达不相交 route 的并发、链路容量差异或 max-min 带宽共享。

### 2.3 多资源拓扑模型

拓扑阶段为每条 flow $v$ 赋予 route resource set $R_v$。在独占小窗口模型中，一组 ready flows 可以同时推进，当且仅当任意两条 flow 的资源集合不相交：

$$
R_u\cap R_v=\varnothing.
$$

资源包括有向 route link，并可加入源端 NIC-TX 和目的端 NIC-RX。这个模型可以精确研究“哪些 flow 可以一起跑”，但仍是 max-min fluid allocator 之前的中间模型。最终 executor 实验才允许 flow 共享链路，并用 progressive filling 分配带宽。

### 2.4 三种数字不能混淆

- `makespan/OPT` 才是某个有限实例的真实最优比；
- `makespan/LB` 只是相对下界的 gap，不能称为 approximation ratio；
- 固定样本上的 `observed max` 只是发现的最差值，不能代替一般最坏情况证明。

## 三、研究中比较了哪些方法

下面先用直观语言解释主要方法，后文在各阶段说明它们为什么有效或失败。

### 3.1 简单优先级

| 方法 | 实际在做什么 | 直觉与主要问题 |
|---|---|---|
| FIFO | 谁先 ready 就先传谁 | 几乎不看未来，适合作为最朴素对照 |
| SPT | 优先传剩余通信量最小的 flow | 能快速完成小任务，但可能推迟一个能释放长计算的稍大 flow |
| LPT | 优先传剩余通信量最大的 flow | 希望先清大流，但容易把长计算链饿死；已有趋近 2 的反例 |
| Longest-delay | 优先选择紧跟其后的计算等待最长者 | 只看下一段计算，不看更深的 DAG |
| Longest-tail | 优先选择从该 flow 之后到终点的最长剩余依赖尾 | 能尽早启动长计算链，是最稳定的简单基线 |
| LRPT | 把当前 flow 剩余量和下游 tail 一起计入 | 比 tail 更偏向大 flow，可能重复强调当前工作量 |
| Earliest-slack | 优先做离预计截止时间最近的 flow | 在基础独立链模型里与 LRPT 排序等价；有真实 optimizer deadline 后才可能不同 |
| TicTac-style | 对两条 ready flow 分别估算 `A→B` 与 `B→A` 的局部结果，再两两比较 | 能看一小步交换效果，但这里只是受 TIC/TAC 思路启发的 comparator，并非复现某篇完整算法 |

Static Longest-tail 只在初始图上算一次尾长；**Residual Dynamic-tail** 会删除已完成工作、使用 active task 的剩余时长，并在每个事件后重算，所以关键分支变化时优先级也会变化。

### 3.2 Join/Gate-aware

Join 是“多个前驱全部完成后，后继才能开始”的汇合点。最初的 gate bonus 试图奖励 join 的最后阻塞者：如果 flow $v$ 比同一 join 的其它输入晚到，则

$$
g(v,x)=\max\left(0,EF(v)-\max_{u\in pred(x),u\ne v}EF(u)\right).
$$

问题是 `tail + g` 可能把“这条 flow 很长/很晚”奖励两遍，而且 optimistic earliest-finish 没有计入网络排队。固定反例中 Dynamic-tail 和 OPT 都为 20，而加 raw gate bonus 后变成 21。因此 join 信息后来只用于提出候选，不再直接叠加成最终分数。

### 3.3 Rollout、Monte Carlo、Beam 与 exact DP

- **Top-k rollout**：先用便宜的规则筛出 $k$ 个候选；假设当前选择其中一个，然后用基线把剩余调度完整跑完，比较预测 makespan。它相当于“现在尝试几种走法，后面都按老办法做，看看哪种结局最好”。
- **Next-event counterfactual rollout**：不只推进一个 tick，也不强迫把长 flow 做完，而是推进到 flow 完成、compute 完成或新 flow ready 中最早发生的下一个事件。这样既能看到连续投入产生的解锁收益，又能在新信息出现时重新决策。
- **Monte Carlo-64**：随机采样 64 个合法、work-conserving 的完整调度，70% 偏向高 tail 候选、30% 探索其它 flow，返回样本中最好者。它不是 MCTS，没有 UCB 或搜索树统计，优点是简单、可并行、随时可停止。
- **Beam-8/32**：每一搜索层扩展多个后继状态，但只保留评分最好的 8 或 32 个。评分结合 residual lower bound 和用 Longest-tail 补全得到的 upper bound。它比单路贪心看得更宽，但固定宽度仍会永久裁掉好分支。
- **伪多项式 exact DP**：状态记录每条链下一条通信、当前剩余量和 compute cooldown，枚举 ready flow 的下一单位执行并缓存重复 residual state。它依赖数值化 duration，总状态会随通信量、链数和深度爆炸，所以是小实例 oracle，不是一般多项式在线算法。
- **二分 makespan + feasibility DP**：在 lower bound 与已有可行 upper bound 之间二分目标时间 $H$，每次用 memoized DFS 判断是否存在 $\le H$ 的调度。它适合 deadline 判定和紧上下界，但可能重复搜索多个 $H$，不保证总比直接 DP 快。
- **Branch-and-bound**：用当前最好 heuristic 解作 incumbent，DFS 枚举决策，用 residual lower bound 剪掉不可能更好的分支。它与 DP 相互校验精确值。

### 3.4 拓扑和 LLM 特化方法

- **Dynamic-tail pack**：按 Dynamic-tail 排 ready flow，再贪心装入互不冲突的 maximal compatible set。
- **Resource-tail pack**：仍以 DAG tail 为主，只在接近的选择中偏向经过剩余负载较高资源的 flow。
- **Bottleneck-first**：优先清理所经 route 上负载最重的 flow。它能释放热点资源，但可能牺牲真正的关键 DAG 解锁。
- **Resource-complement**：主动寻找与当前关键 flow 资源互补、可以同时跑的 flow，以填满闲置链路。
- **Set rollout**：候选动作从一条 flow 变成一个 compatible set，对每个集合做 next-event 端到端评价。
- **Backbone-first**：偏向 PP/TP 的 forward/backward-input 主干通信。
- **Deferred gap-fill**：当主干暂时不紧迫时，用 backward-weight/DP 等可推迟工作填通信空隙。
- **Optimizer-deadline**：按估计 latest-start slack 提升临近 optimizer barrier 的 W/DP 流量；当前版本的 slack 仍是近似量。
- **Dimension round-robin**：给 PP/TP/DP 等通信维度提供不同候选，避免单一维度长期占据共享资源。
- **Replica wavefront**：在 tail 和 route 看起来相同的 DP replica 中保持与物理 placement 相符的推进顺序。
- **Ring chunk wavefront**：利用 collective 的 `chunk_id/num_chunks`，为 Ring 内部不同 chunk 生成替代候选。
- **Default fair-share**：真实 executor 中所有 active flow 在冲突链路上普通 max-min 公平共享。
- **Strict Dynamic-tail max-min**：先按 downstream tail 划优先 tier；高 tier 先做 progressive filling，同一 tier 内 max-min 公平，低 tier 只能使用剩余容量。

LLM 特征最终不组成一个大线性 score，而是生成少量语义不同的候选；端到端 rollout 或受限 tie-break 再决定是否采用。

## 四、阶段 0：先确认 DAG 语义和可利用结构

### 目的

如果 DAG 本身缺边、错边，或分析图与 executor 真正遵守的顺序不同，后续任何 heuristic 收益都没有意义。本阶段的任务是构造并验证 effective DAG，同时寻找真实 LLM 图中可用于压缩搜索的规律。

### 做了什么

构造了 `pp=2,tp=2,dp=2,ga=4` 的混合探针和纯 PP 小图，覆盖 1F1B、Interleaved、当前 Zero Bubble、Bidirectional 和 DualPipe。审计器加入相邻 `compute_order` 边，检查无环性、critical path、frontier、join、slack、模板重复和 warmup/steady/cooldown 周期，并用 `AnalyticalExecutor` 逐边验证 `end(source) <= start(target)`。

### 实验结果与分析

- 混合图约 992--1088 个节点；每种模式都多出 392 条原 workload DAG 没有的设备串行边。忽略这些边会虚构 GPU 并行性。
- pure-PP 图的 exact width 只有 3--6，混合图 ready frontier 约 10--18，说明在线状态通常不宽，适合 top-k/beam/frontier DP。
- 所有任务都落在重复模板中，去掉 microbatch 后只有 76--114 种局部模板，说明 pipeline 周期和缓存有价值。
- 图中 join 很密，且 critical backbone 会跨 stage、microbatch 和 phase 切换；没有足够多可独立拼接的全局 SESE 区域。静态 chain decomposition 或按并行维度分别求解不可取。
- TP 中大量 flow 是零 slack；DP 通常有较大 slack，支持把 DP/W 当作 deferred side work，但必须保留 optimizer barrier。
- 默认 ZB builder 仍有 `B→W` 依赖，DAG 层面并未真正解耦 B 和 W。serializer 只是改变执行顺序，不能替代因果结构。
- 普通 1F1B/Interleaved 中 optimizer gating 和 activation memory 语义不完整，因此当前研究只适用于固定单 iteration、忽略 activation 容量的调度。
- 992-task 1F1B timeline 完成全部任务，2168 条 effective edge 零违例，满足阶段退出条件。

### 阶段结论

后续应在 residual effective DAG 上动态算 tail；利用小 frontier 和周期模板压缩搜索；把 collective completion 而非每个 Ring hop 当成 join 信号；不能把 PP、TP、DP 当作完全独立子问题直接组合。

### 可继续深挖

补齐显式 optimizer barrier、跨迭代 overlap 和 activation memory；正式修正 ZB 的 B/W 因果语义；对非 homogeneous pipeline stage 建模真实层数和时间。

## 五、阶段 1：建立 Benchmark 与精确 Oracle

### 目的

普通 LLM motif 经常对通信顺序不敏感，只在几个“正常图”上比较会让所有算法看起来都很好。本阶段建立能主动攻击错误策略的 benchmark，并用精确最优解判断 heuristic 到底差多少。

### 做了什么

构造了七类对抗图、七类参数化 LLM motif，以及从 992-node effective DAG 缩减出的真实窗口。实现 memoized discrete-time DP 和 branch-and-bound 两个独立 oracle，并统一评价 FIFO、SPT、LPT、Longest-tail、LRPT 和简单 Gate-aware。

统一下界为

$$
LB=\max(P,Q,L,LB_{window},LB_{cut}).
$$

其中 `window bound` 用 precedence-only earliest release 和 downstream tail 推导每条 flow 的必要执行窗口，再检查任意 release/deadline 区间内的通信需求是否超过可用时间；`cut bound` 为同一标记 cut 上的负载，在单 channel 中不强于 $P$，主要为多资源扩展预留。

### 实验结果与分析

200 个精确可解的随机链/一般 DAG 混合样本中：FIFO、SPT、LPT 的 observed max 分别为 1.6875、1.5455、1.6875；Longest-tail 和 LRPT 的 mean ratio 为 1.0035、observed max 为 1.1176；简单 Gate-aware 的 mean ratio 为 1.0144、observed max 为 1.2。

这证明 benchmark 能识别坏算法，也说明“立即解锁了几个 compute”不是可靠 bonus。14 个合成图的 combined LB 恰等于 OPT；真实缩减图为 `LB=144, OPT=145`，说明下界很强但仍不能完全代替 oracle。很多单一 PP wave、optimizer DP burst 上所有 work-conserving 方法都最优，真正有区分力的实例必须让 backbone、join 和 deferred work同时竞争。

### 阶段结论

Longest-tail 成为首选基础策略，但没有一个简单静态规则逐实例支配其它方法。后续算法必须保留反例回归、随机图、LLM motif 和真实窗口四类评测，不能只报告容易实例的总体最优率。

### 可继续深挖

真实窗口应从“策略发生分歧的 endogenous 状态”提取，而不是只选 flow 最密集时间桶；中型 oracle 可进一步加入可选 CP-SAT/MILP backend 和 gap certificate。

## 六、阶段 2：并行链问题与近似理论

### 目的

先在最清楚的模型里回答两个问题：有没有可靠的常数保证；比简单贪心更贵的 DP、二分、Monte Carlo 和 Beam 在实际小 frontier 中是否值得。

### 建模

有 $h$ 条独立链：

$$
C_{i,1}\to L_{i,1}\to C_{i,2}\to L_{i,2}\to\cdots,
$$

$C$ 是共享 channel 上的通信，$L$ 是通信完成后在 channel 外并行执行的 compute lag。令

$$
P=\sum_{i,j}C_{i,j},\qquad Q=\max_i\sum_j L_{i,j}.
$$

### 2-近似定理与证明

**定理。** 任意 work-conserving schedule $H$ 满足

$$
T_H\le P+Q\le 2OPT.
$$

**证明。** 把调度时间分成网络 busy 和 idle 两类。busy 总时长不超过所有通信工作量 $P$。若网络 idle，由于策略不主动空闲，此时一定没有 ready communication；沿最后完成任务反向追踪一条 blocking chain，这些互不重叠的 idle 区间只能对应这条因果链正在执行的 compute lag，所以 idle 总量不超过 $Q$。于是 $T_H\le P+Q$。任何调度都必须完成全部通信且不能短于最长纯计算链，因此 $OPT\ge P$ 且 $OPT\ge Q$：

$$
\frac{T_H}{OPT}
\le\frac{P+Q}{\max(P,Q)}\le2.\quad\square
$$

条件是单瓶颈、静态有限 DAG、计算 ready 后自动执行、依赖已经正确编码，以及存在 ready flow 时不主动 idle。整数时间和抢占主要服务 exact DP，并非 charging proof 的核心。该证明不能直接搬到多 route 资源模型。

**2 为什么对任意 work-conserving priority 是紧的。** 构造

```text
A: comm(M)
B: comm(1) → compute(M) → comm(1)
```

错误策略若先完整传 A，则 $T_H=2M+2$，而先启动 B 的长计算可得 $OPT=M+2$，比值随 $M$ 增大趋近 2。这里证明的是“任意 work-conserving 策略族不能仅凭 work-conserving 性质获得更好界”，并没有证明每个具体策略都达到 2。

### Longest-tail、Rollout 和 Beam 的理论边界

早期得到 Longest-tail/LRPT 的 $9/8$ 反例；随后构造了四种方法共享的渐近 $5/4$ 反例：

```text
A: C(2k) → L(3k+1) → C(2k)
B: C(k)  → L(2k)   → C(3k)
```

最优值为 $8k+1$。A 的初始 tail 比 B 大 1，Longest-tail 先做 A，随后产生约 $2k$ 的网络空闲，得到 $10k$，所以

$$
\frac{T_{LT}}{OPT}=\frac{10k}{8k+1}\to\frac54.
$$

Rollout-2 只试一个单位后又切回 Longest-tail，看不到“必须连续做完 B 的首段”才产生的收益，所以也继承该反例。固定 Beam-$B$ 必须连续保留推进 B 的分支 $k$ 层；取 $k>B$，好分支会被裁掉。因此当前严谨结论是：

| 算法 | 已证明上界 | 已构造下界 | 能否宣称小于 2 |
|---|---:|---:|---|
| Longest-tail | 2 | 渐近 $5/4$ | 不能 |
| Rollout-2 | 2 | 渐近 $5/4$ | 不能 |
| Beam-8/32 | 2 | 渐近 $5/4$ | 不能 |

Rollout-2 还有一个严格正结果：候选集总包含 Longest-tail 的基础动作，而每个候选都用 Longest-tail 补全并取最好者，所以逐状态有

$$
1+J_{LT}(s')\le J_{LT}(s),
$$

沿 rollout 轨迹归纳可得 $T_{rollout2}\le T_{LT}$。这叫 policy improvement，不等于改善最坏近似因子。

### 实验结果

100 个 2--5 链、每链 1--3 个 flow 的精确随机实例中：

| 方法 | mean ratio | observed max | 平均 Python 时间 |
|---|---:|---:|---:|
| FIFO | 1.1918 | 1.5217 | 0.38 ms |
| Longest-delay | 1.0688 | 1.3077 | 0.39 ms |
| Longest-tail | 1.0037 | 1.1176 | 0.45 ms |
| Rollout-2/4 | 1.0008 | 1.0769 | 9.39/15.40 ms |
| Monte Carlo-64 | 1.0004 | 1.0400 | 32.08 ms |
| Beam-8 | 1.0004 | 1.0370 | 77.48 ms |
| Beam-32 | 1.0000 | 1.0000 | 215.70 ms |
| exact DP | 1.0000 | 1.0000 | 平均 902.94 ms |

Beam-32 在这 100 个实例上全优只是一项实验现象。Exact DP 平均探索 33,116 个状态，P95 达 204,697；6 条链就可能超过 30,000 状态预算。非多项式算法可用于很小窗口或离线 teacher，但必须同时限制状态数和 wall-clock。

Longest-tail 通过更早启动长 compute，把平均 network idle fraction 从 FIFO 的 0.2617 降到 0.1303；Rollout-4 进一步到 0.1281。这说明收益来源不是提高物理带宽，而是减少“所有链都在算、网络被迫等待”的空洞。

### 可严格求解的特殊子类

1. 每条链只有一个 flow 时，问题等价于单机加工后带 delivery tail 的 makespan 问题。按 tail 非增顺序最优：若相邻 $q_a<q_b$ 却先排 $a$，交换为 $b,a$ 不会增大两者的最大 $C_i+q_i$。这是 Jackson rule 的相邻交换证明。
2. 所有 compute lag 为 0 时，makespan 恒为 $P$，任意 work-conserving 调度最优。
3. 所有 flow 等长时，小参数穷举中 Longest-tail 全优，但尚无一般证明。
4. 重复链可以按对称状态 canonicalization 合并，这对重复 microbatch/replica 比发明新贪心分数更有价值。

### 阶段结论与后续

实际算法应是 anytime cascade：立即给出 Longest-tail；有约 10--20 ms 小图预算时做 rollout；更宽离线预算做 Monte Carlo/Beam；极小窗口才做 exact DP。理论上最值得继续的是证明 2-chain/2-flow 子类是否恰为 $5/4$，或对一般链争取 $3/2$ 上界；LLM 场景则更适合研究按 frontier 宽度、flow 深度和量化时长参数化的 exact 保证。

## 七、阶段 3：从并行链推广到一般 DAG

### 目的

真实训练图包含 fork、join、TP collective 和不断切换的关键分支，不能永久分成独立链。本阶段把 tail 和有限前瞻改为 residual DAG 上的事件驱动算法。

### 做了什么

对当前状态 $s$ 动态计算

$$
q_s(v)=\max_{(v,x)\in E}\left(d_s(x)+q_s(x)\right),
$$

已完成节点删除，active 节点使用 remaining duration。实现 raw last-blocker gate、Event Rollout-2/4/8 和看未来 3 个事件层的 Local Beam-8。所有增强方法保留完整 Dynamic-tail incumbent，在确定性模型中如果预测解更差就退回基线。

### 普通集结果

73 个 exact 实例由 8 个对抗图、7 个 LLM motif、8 个真实缩减窗口和 50 个随机一般 DAG 组成：Static tail、Dynamic-tail 的 mean ratio 分别为 1.00361、1.00190；raw gate 变差到 1.00258；Event Rollout-2/4/8 和 Local Beam-8 在本组全部命中 OPT。Rollout-2 平均约 28 ms，Beam-8 约 291 ms，因此当前没有理由在线使用更贵的 Beam。

但 Dynamic-tail 本来就有 97.26% 最优率，普通集过于容易。为避免高基础优秀率掩盖差异，阶段 3.5 专门筛选 `Dynamic-tail > OPT` 的困难实例：尝试 513 个图只找到 20 个，接受率 3.90%。

### 端到端 Counterfactual Bonus

对候选动作 $v$ 执行到指定 horizon 后得到状态 $s_v$，再由 Dynamic-tail 补全：

$$
\widehat C(v\mid s)=\Delta_v+\widehat J_{DT}(s_v).
$$

相对基础动作 $a_0$ 的 bonus 为

$$
B(v\mid s)=\widehat C(a_0\mid s)-\widehat C(v\mid s).
$$

最终直接选择 $\widehat C$ 最小者，而不是把 $B$ 再加回 tail。这样 join 解锁、未来计算、推迟其它通信的机会成本都只在完整预测中计算一次，解决了 raw bonus 的重复奖励。

20 个困难实例上：one-tick horizon 平均关闭 70% gap；强制 flow-complete 只关闭 60%；**next-event 关闭 95%，修复 19/20**。Tick 看得太短，flow-complete 又承诺过度；next-event 是信息量和可撤销性的最好平衡。

Join-only、Dynamic-tail-only 和 Hybrid 候选的 next-event 结果逐实例相同。Join top-2 真正替换 Dynamic top-2 的状态很少，即使专项筛选 join-sensitive 实例，也没有观察到独立收益。因此有效的是端到端 counterfactual 评价，不是当前 `EF last-blocker` 特征。

### 真实窗口的负结果

沿真实量化 1F1B 调度发现 422 个不同策略动作分歧 tick，但只有 5 个窗口能在状态预算内精确求解，而且各方法最终 makespan 仍相同。这说明“动作不同”也不等于“端到端性能不同”；粗时间量化和 boundary tail 截断会抹掉 iteration 末端影响。

### 阶段结论与后续

推荐一般 DAG 原型收敛为：

```text
Residual Dynamic-tail
  + Dynamic-tail top-2 candidates
  + next-event end-to-end counterfactual
  + complete baseline incumbent
```

Join 若继续研究，应改为 optimizer latest-start、join 后 resource-aware critical tail 或真实 delay sensitivity。窗口应保留原始微秒时长、多事件因果和未来资源 demand profile。

## 八、阶段 4a--4c：从单 channel 扩展到真实路径冲突

### 阶段 4a：多资源小图 Oracle

#### 目的与做法

验证单 channel 是否虚构冲突。为 flow 加 route resource set，枚举 inclusion-maximal compatible sets，并用 exact DP 求小图最优。合法下界改为

$$
LB=\max\left(L,\max_r P_r\right),
$$

$P_r$ 是所有使用资源 $r$ 的剩余工作量。总通信量 $P$ 不再是合法下界，因为不同 route 可以并行。

#### 结果

53 个 exact 小图中，Dynamic-tail pack mean ratio 1.01749，Bottleneck-first 1.02947，Set Rollout-2 为 1.00463。13 个 Dynamic-tail 困难实例上，Resource-tail 只关闭 10.26% gap，Bottleneck-first 为 -15.38%，Set Rollout-2 关闭 69.23% 并修复 9/13。

同一批 DAG 压成 single channel 后，连 exact OPT 都平均高 14.88%、最大高 57.14%。因此拓扑不是一个可后补的小修正：它决定哪些通信选择真实存在。

### 阶段 4b：真实 builder + AlibabaHPN route

连接五种 pipeline 的真实 builder、serializer、effective DAG、16-GPU AlibabaHPN 和 BFS route。`pp=2,tp=2,dp=2,ga=4` 探针的平均 ready width 为 4--8，但 ready-pair conflict 大多为 0；只有 Bidirectional 为 3.407%，1F1B 为 0.810%。Interleaved、当前 ZB、DualPipe 没有离散 route 选择。

共得到 5 个 exact 窗口，除 LPT 在两个窗口上由 6 变 7 外，Dynamic/Resource/Bottleneck/Set rollout 都最优。4 个可解 single-channel 对照比多资源 OPT 平均高 83.33%。这不是 topology heuristic 失败，而是默认大拓扑、单 job、规则 placement 下几乎没有可调度冲突。

### 阶段 4c：手工小拓扑制造可控冲突

构造 single-switch、two-rack shared uplink 和 four-rack shared core，并用 PP-cross、DP-cross、TP-cross placement 控制哪个并行维度跨瓶颈。18 个真实 builder/effective-DAG 场景的平均冲突率升到 15.15%，15/18 出现策略分歧。

资源优先出现正反两面：four-rack TP-cross 中 Bottleneck-first 有收益；two-rack PP-cross 1F1B 中 Dynamic-tail=766，而 Resource/Bottleneck=768。原因是清热点链路和解锁关键 compute 是两个不同目标，必须看端到端结果。

最强 `four-rack-core + TP-cross` 上：

| pipeline | LB | Dynamic | Bottleneck | Rollout-2 | Rollout-4 |
|---|---:|---:|---:|---:|---:|
| 1F1B | 758 | 765 | 764 | 765 | **762** |
| Bidirectional | 768 | 782 | 777 | 779 | **775** |

Rollout-4 分别改善 0.39% 和 0.90%，但需约 36--42 秒，比快速 heuristic 慢两个数量级，暂时只能当离线 teacher。43 个 depth-0 exact 窗口不足以解释完整图差异，因为 1--7 tick 的收益由多个 collective wave 的选择累积产生。

### 多资源理论结论

单 channel 的 2-近似证明在这里失效：不同资源 busy 时间会重叠，一个 maximal set 也可能同时占住多个关键资源，不能把时间简单 charge 到 $P$ 或各 $P_r$ 之和。目前只有 `critical path + per-resource load/window` 下界和小窗口 exact oracle，没有 Dynamic-tail pack 或 Set rollout 的一般常数近似保证。

如果继续做理论，合理方向是限制 route 长度、每条 flow 占用资源数、树路径冲突图结构或拓扑 oversubscription，而不是继续套用单机证明。

## 九、阶段 4：利用 LLM DAG 的特殊结构

### 目的

在拓扑冲突真实存在后，再问 PP/TP/DP、replica、Ring chunk、B/W deferred work 和 pipeline 周期能否减少候选数并改善调度。

### 做了什么

建立 analyzer-owned sidecar，记录 dimension、phase、microbatch、stage、layer、item、chunk、逻辑 PP/DP/TP 坐标、物理端点和 route，不修改通用 `Task`。每个冲突事件用这些语义生成 Dynamic、Bottleneck、Resource-complement、Backbone、Deferred、Optimizer、Dimension、Replica、Chunk 等少量 candidate sets，再做统一 next-event counterfactual。

为了衡量候选生成是否漏掉好动作，还枚举每个事件全部 maximal compatible sets，找 one-event teacher 的最佳集合。Teacher 不是完整 DAG OPT，但可以回答“语义候选是否覆盖了这一步最好的反事实动作”。

### Teacher 与完整图结果

高冲突拓扑中，1F1B 每个决策平均有 4.77 个 exhaustive sets，语义候选只有 1.81 个；Bidirectional 从 6.70 降到 2.10，分别减少约 62% 和 69%，同时 value/action coverage 都为 100%。补足覆盖的关键正是 DP replica 坐标和 Ring chunk id。

完整图结果为：

| pipeline | LB | Dynamic | topology-only | semantic rollout | 先前 Rollout-4 |
|---|---:|---:|---:|---:|---:|
| 1F1B | 758 | 765 | 764 | **762** | 762 |
| Bidirectional | 768 | 782 | 776 | **773** | 775 |

LLM 语义相对 Dynamic 分别改善 0.39% 和 1.15%，相对相同 counterfactual 框架的 topology-only 再改善 2/3 tick。绝大多数事件仍沿用 Dynamic-tail，语义只在少数 replica/chunk/跨维度冲突点介入。

### 特征消融：被选择不等于有独立贡献

逐个删除候选族后：1F1B 只有 replica wavefront 带来独立 2 tick；Bidirectional 中 replica wavefront 独立贡献 3 tick，dimension round-robin 贡献 2 tick。Backbone、optimizer deadline、deferred gap-fill 和 chunk wavefront 在这个场景中移除后不改变最终 makespan。某候选在轨迹中被选择，只说明它是一个可行等价路径，不能自动算作独立收益。

### 周期缓存

同一粗模板动作一致率为 1F1B 82.86%、Bidirectional 68.75%。只用粗 `(template, replica, TP position, chunk, route width)` 缓存候选标签，1F1B 命中 30/77，保持 makespan 762 并把原型时间从约 13.17 秒降到 8.79 秒；Bidirectional 命中 10/96，时间从 18.58 秒降到 17.04 秒。缓存复用的是候选标签，每次仍重建合法集合并保留 Dynamic incumbent，因此不会因 key alias 直接返回非法旧动作。

### 安全分区和 ZB 对照

只把“与图中所有其它节点都有明确先后关系”的全局 barrier 当 strict-series 切口。38 个完整参数点全部没有非平凡安全切分，进一步否定“切成很多小 DAG、分别最优再拼起来”。Dominator/SESE 可用于压缩，但边界必须携带完成时间、未来资源 demand、输出 ready time和未完成状态等 Pareto 信息。

隔离研究路径删除当前 ZB 中 W 对同层 B/IG completion 的依赖，并加入 `F→W`，不改生产 builder。48 个 W 共删除 80 条旧边、加入 48 条 F 边。LB 从 730 降到 726，Dynamic 从 733 降到 729，说明 `B→W` 确实压缩了真实 ZB 的可调度空间；但 deferred-W 规则仍未展示相对一般策略的独立收益。

### 更强多资源下界

对每个 route resource 单独构造 release/deadline demand window，若区间内所有必须使用该资源的工作量超过区间长度，则该 horizon 不可行。一个测试中原 `critical path=11, max load=6`，window bound 直接收紧到 `OPT=14`；高冲突 ZB 中 combined LB 从 726/730 一类的松界进一步缩小剩余认证 gap。这个下界适合作离线证书和搜索剪枝。

### 阶段结论与后续

LLM 规律的实用方式不是硬切 DAG，也不是把十几个 bonus 相加，而是：压缩 frontier 状态、生成少量互补候选、只在冲突事件做端到端评价。下一步应扩大 replica placement 规则验证，补正式 ZB B/W 语义，研究 optimizer deadline 的严格 latest-start，并把 continuation value 缓存做成增量实现。

## 十、阶段 5：统一评测与真实 AICB/executor 闭环

### 10.1 统一评测先发现了什么

统一入口汇总随机并行链、一般小 DAG、38 个完整 route-aware 参数点、五种 pipeline、语义消融和真实 GPT profile。

53 个一般 DAG 上，Dynamic-tail mean ratio 1.00176、observed max 1.04762，优于静态 tail；raw gate 再次略微退化；Rollout-2/4/8 全部命中 OPT，但平均约 12--14 ms，仍没有小于 2 的一般证明。

按 $P=\max_r P_r$ 与纯计算 critical load $Q$ 分层后，Resource-tail 最稳定；Bottleneck-first 在 compute-dominated 点最好，却在 balanced/communication-dominated 点显著退化；LPT 在通信主导点相对 LB 达 2.28723。说明 heuristic 最有价值、也最难的区域是 $P\approx Q$，资源优先不能跨负载区间无条件使用。

首个真实 GPT-13B AICB 为 TP=8、PP=2、DP=1，48,416 tasks、39,776 flows，其中 TP 占 99.7%。三个缩减窗口均由“可行解=resource-window LB=56”认证最优，各算法无差异。这完成了真实 profile 的结构/route/certificate 通路，却不能测试多维 DP overlap。

### 10.2 真实多维 AICB 是怎样得到的

扫描 `inputs/aicb-workload` 的 936 份文件后发现它们都满足 header `all_gpus=TP×PP`，即原文件 DP=1。为保持语义可审计，实验沿用仓库已有做法：保留每 rank 的 AICB compute/communication profile，通过 `dp_override` 扩大 `Job.parallelism.dp` 和 assigned nodes，让真实 builder 展开 DP collective。报告同时记录 header DP 与 expanded DP。

主实验选择 GPT-7B/13B/22B、TP=4、PP=2、DP override=2、GA=8、AlibabaHPN 16-GPU，并比较 contiguous 与 `cyclic_pp_dp`。扩展实验为 GPT-13B、DP=4、GA=4、Hermod 32-GPU。

### 10.3 真实 executor heuristic

研究 policy 复用真实 parser、builder、serializer、BFS route、链路容量和 `AnalyticalExecutor`。与 Default 的区别仅在带宽分配：

- Default 把所有 active flow 放在同一个普通 max-min fair-share 中；
- Strict Dynamic-tail 先按 effective-DAG downstream tail 划严格优先级层；
- 每层内部使用 progressive filling 得到 max-min 公平份额；
- 低层只能使用高层分完后的剩余链路容量。

因此这不是独占 route 小模型，而是真实链路上的 fluid bandwidth sharing。实验还记录 conflict allocation calls、真正从正带宽降为零且仍 active 的 preemption、unlock delay 和 Python allocator 时间。

### 10.4 跨模型与 PP 策略结果

Bidirectional、DP=2、GA=8：

| model | contiguous improvement | cyclic improvement |
|---|---:|---:|
| GPT-7B | 4.30% | 1.21% |
| GPT-13B | 4.96% | 1.29% |
| GPT-22B | 6.70% | 1.65% |

六个场景全部为正。收益来自严格保护关键 tail，而不是 Resource/LLM 特征：Dynamic、Resource、Bottleneck 和完整 LLM tie 在 DP=2 中 makespan 相同。

1F1B 是重要负对照：三模型 contiguous 完全不变，cyclic 变化在 -0.0136% 到 +0.0058% 之间，可视为无收益。1F1B 只有 32 条 DP flow，而 Bidirectional 有 224 条 replica gradient sync flow；后者才形成足够长的共享链路冲突窗口。

GPT-13B Bidirectional 的 GA 敏感性为：GA=2 时 contiguous/cyclic 改善 15.55%/4.12%，GA=4 为 7.84%/2.03%，GA=8 为 4.96%/1.29%。GA 越大，固定同步尾部占 iteration 比例越小，所以相对收益下降。

### 10.5 DP=4 和 guarded replica

GPT-13B、TP4×DP4×PP2、GA=4 有 1088 条 DP flow。Dynamic-tail 相对 Default 在 contiguous/cyclic 分别改善 6.73%/9.29%。

Replica tie 在 contiguous 把 makespan 从 Dynamic 的 7,712,679 us 降到 7,566,512 us，相对 Default 总改善 8.49%；但 cyclic 中反而比 Dynamic 慢 19,803 us。原因是物理 server 上 replica 顺序不同：

```text
contiguous: stage0 [0,0,1,1], stage1 [2,2,3,3]  → 单调
cyclic:     stage0 [0,1,2,3], stage1 [1,2,3,0]  → 非单调
```

于是得到 `guarded_replica_tie`：只有每个 PP stage 的 replica server 序列都单调时启用 replica wavefront，否则退回 Dynamic-tail。它在 contiguous 保留 7,566,512 us，在 cyclic 精确退回 13,874,545 us。这是首个在真实 AICB、route 和 max-min executor 中展示独立收益的 LLM 特征，但目前只是一条经一个模型/DP=4/拓扑组合验证的 restricted placement rule。

### 10.6 Profile 误差与调度开销

只扰动调度器估计，executor 使用原 AICB 时长。把全部 compute 或 communication 统一缩放到 80%/120% 不改变决策。逐 task 加三组独立 ±20% 噪声后，contiguous 仍改善 8.08%--8.17%，cyclic 仍改善 9.23%--9.55%，说明 makespan 收益对这种误差较稳健。

问题出在控制开销：nominal 约 1012/1392 次 allocation、0.68/0.87 秒 allocator CPU；噪声后增至 5500--6812 次、4.4--5.2 秒。噪声把相近 tail 拆成大量唯一 tier，引发频繁暂停和重分配。

1 ms 固定 tail bucket 没有减少调用；100 ms bucket 虽减少部分调用，却把 nominal contiguous/cyclic 收益从 6.73%/9.29% 降到 2.84%/4.21%。固定分桶应当否定。更合适的是只在冲突集合或关键优先关系真正变化时重算，并加入 priority hysteresis/minimum residency。

单次 allocator Python 原型成本仍较小：DP=2 约 0.23--0.28 ms，DP=4 nominal 约 0.62--0.74 ms；guarded replica 约 0.8 ms/调用。主要问题是调用次数，而不是单次 score 计算。

### 阶段结论

目前可以严谨宣称：

- 已经完成“真实 AICB → effective DAG → route → max-min executor → 指标”的模拟器内部实验闭环；
- Strict Dynamic-tail 对 Bidirectional 在多模型、多 GA、DP=2/4 和两种 placement 上稳定优于 Default；
- guarded replica 在受限单调 placement 上有独立额外收益；
- 1F1B 无可测收益，Resource-tail 无独立收益，固定 tail bucket 无效；
- 尚无多资源常数近似比，也尚未达到至少两种 PP 策略稳定改善、生产在线开销验证和跨动态 job/多 iteration 泛化。

## 十一、整体上学到了哪些可利用的 LLM 规律

1. **小 frontier。** ready 候选通常不多，允许 top-k、短 beam 或受预算 DP；但 duration、深度和重复状态同样决定复杂度，不能只看 ready 数。
2. **强模板重复。** microbatch、replica、Ring chunk 反复出现相同局部结构，适合 canonicalization、候选标签缓存和短周期策略。
3. **关键链动态切换。** stage、phase、microbatch 都会切换，静态链分解不可靠，residual tail 更自然。
4. **TP 常在关键 join，DP 常有 slack。** 可以把 W/DP 当 deferred work，但不能删除它们到 optimizer barrier 的全局关系。
5. **真正的机会来自 DAG 紧迫性和 route 冲突同时出现。** 只有 tail 没有冲突，无需调度；只有热点没有关键解锁，Bottleneck-first 可能伤害性能。
6. **并行维度不是可独立求解的笛卡尔积。** PP、TP、DP 的子图有规律，但它们通过 NIC、uplink、collective join 和 optimizer barrier 耦合。维度结构适合生成候选和压缩状态，不适合分别求局部最优后直接拼接。
7. **少数对称选择很关键。** replica/chunk/维度通常只在少量事件改变动作，却可能贡献全部额外收益，所以总体命中率不是好指标，应报告 hard-subset gap closed 和 leave-one-feature-out。

## 十二、下一步最值得做什么

按收益与风险排序，建议继续推进以下工作。

1. **把研究 policy 做成隔离的 executor 策略原型。** 仍不注册为默认 baseline，先实现 conflict-event trigger：只有共享关键链路的 active set 或优先关系变化时才重分配。
2. **加入 priority hysteresis/minimum residency。** 限制小 profile 噪声造成的 tier 抖动；保留原始 tail 精度，不再使用已失败的固定大 bucket。
3. **以 Dynamic-tail 为默认，guarded replica 为受审计增强。** Guard 必须继续在更多模型、DP、拓扑、placement 上做正反例验证；不满足条件时自动退回基线。
4. **验证多 iteration、多 job 和动态到达。** 观察 job 间相位错开、跨 iteration optimizer overlap 是否产生新的冲突，并测量真实 event frequency 与控制开销。
5. **补全语义缺口。** 正式建模 optimizer barrier、activation memory 和真实 ZB B/W 解耦后，重新评价 deferred-W/optimizer deadline。
6. **继续理论研究时收缩问题。** 单瓶颈优先研究 2-chain/2-flow 的 $5/4$ 猜想和 frontier 参数化 exact 算法；多资源优先研究树路径、有限 route 长度和有限资源占用数的 restricted guarantee。不要在没有新结构条件时声称一般多资源常数比。
7. **用论文式实验口径报告。** 同时给出 absolute makespan、相对 Default 改善、`makespan/OPT` 或明确标注的 `makespan/LB`、冲突机会数、hard-subset gap closed、allocator 调用次数和 profile 扰动结果。

## 十三、参考文献与适用边界

下面文献用于说明相邻调度问题的算法和复杂性边界。它们的模型通常包含不可抢占机器、exact/minimum delay 或 coupled tasks，与本文“通信占共享资源、计算在资源外并行、通信可抢占”的模型并不完全相同，因此不能直接搬用其 hardness 或近似比。

1. Lucian Finta and Zhen Liu. **Single Machine Scheduling Subject to Precedence Delays.** *Discrete Applied Mathematics*, 1996. 讨论 unit processing、precedence delay 下的强 NP-hard 性及部分可解特例。[DOI: 10.1016/0166-218X(96)00110-2](https://doi.org/10.1016/0166-218X(96)00110-2)
2. Jacek Błażewicz, Grzegorz Pawlak, Michał Tanaś, and Wojciech Wojciechowicz. **New Algorithms for Coupled Tasks Scheduling – A Survey.** *RAIRO Operations Research* 46(4):335--353, 2012. 总结 exact/minimum gap、chain precedence 与若干多项式/困难子类。[DOI: 10.1051/ro/2012020](https://doi.org/10.1051/ro/2012020)
3. Maher Mallem, Claire Hanen, and Alix Munier-Kordon. **Single Machine Scheduling with Precedence Constraints and Bounded Maximum Delay Value.** *Journal of Combinatorial Optimization* 51(3), 2026. 说明 bounded delay/width 参数下仍可能存在较强参数化困难。[DOI: 10.1007/s10878-026-01411-w](https://doi.org/10.1007/s10878-026-01411-w)
4. Anis Gharbi and Mohamed Labidi. **Jackson's Semi-Preemptive Scheduling on a Single Machine.** *Computers & Operations Research* 37(12):2082--2088, 2010. 回顾单机 heads/tails 场景中的 Jackson schedule；本文“每链一个 flow”最优性与按 non-increasing delivery tail 的交换规则对应。[DOI: 10.1016/j.cor.2010.02.008](https://doi.org/10.1016/j.cor.2010.02.008)
5. Alexander A. Ageev and Mikhail Ivanov. **Approximating Coupled-Task Scheduling Problems with Equal Exact Delays.** *Lecture Notes in Computer Science* 9869:259--271, 2016. 给出相邻 exact-delay coupled-task 模型的近似与困难结果；不能直接当作本文 $5/4$ 界。[DOI: 10.1007/978-3-319-44914-2_21](https://doi.org/10.1007/978-3-319-44914-2_21)
6. Hans L. Bodlaender and Marieke van der Wegen. **Parameterized Complexity of Scheduling Chains of Jobs with Delays.** IPEC 2020. 研究带 exact/minimum delay 的 chain scheduling 参数化复杂性，为“按 frontier/链宽做参数化算法”提供相邻问题背景。[LIPIcs.IPEC.2020.4](https://doi.org/10.4230/LIPIcs.IPEC.2020.4)

本文中的 $T_H\le P+Q\le2OPT$、Rollout-2 对 Longest-tail 的逐实例支配、$9/8$ 与渐近 $5/4$ 反例、多资源 window lower bound，均是在本项目明确模型下给出的推导或构造；它们不是从上述不同模型的文献直接移植而来。

## 十四、代码与结果入口

主要实现和可复现实验包括：

- DAG 审计：`scripts/audit_pipeline_dags.py`
- Benchmark/exact oracle：`scripts/benchmark_dag_oracle.py`
- 并行链算法：`scripts/study_parallel_chains.py`
- 一般 DAG rollout：`scripts/study_general_dag_heuristics.py`
- Counterfactual bonus：`scripts/study_counterfactual_bonus.py`
- 多资源 DAG：`scripts/study_multiresource_dag.py`
- 真实 route 窗口：`scripts/study_llm_route_windows.py`
- 手工小拓扑：`scripts/study_small_topology_sensitivity.py`
- LLM 语义候选：`scripts/study_llm_structured_candidates.py`
- 统一评测：`scripts/study_heuristic_plan_completion.py`
- 真实 AICB executor：`scripts/study_real_aicb_executor_heuristics.py`

完整实验数字和逐轮复现信息保留在 `docs/heuristic进度.md`；本文负责解释研究主线、方法含义和证据边界。
