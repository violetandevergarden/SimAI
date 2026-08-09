建议将研究主线定为：

  > 以单瓶颈模型下的 2-近似为理论安全底座，通过真实 DAG 结构分析，逐步发展 Gate-Aware Dynamic Tail
  > Rollout，并重点争取在 LLM 特有 DAG 子类上获得更强保证。

  整体不再采用“先切成独立小 DAG、分别求解、最后拼接”的方案，而采用“安全分区 + 动态关键链投影 + 有限前
  瞻”。

  flowchart LR
      A[DAG 语义审计] --> B[Benchmark 与最优 Oracle]
      B --> C[并行链理论与 Heuristic]
      C --> D[一般 DAG: Gate-aware Rollout]
      D --> E[LLM Motif 与周期结构]
      E --> F[真实拓扑与 Runtime-adaptive]
      E -.发现新结构.-> C
      D -.反例反馈.-> B

  ## 一、研究目标

  ### 理论目标

  1. 严格证明单瓶颈一般 DAG 下 work-conserving list scheduling 的 2-近似。
  2. 分析 longest-tail、LRPT 等具体策略的最坏情况。
  3. 判断一般并行链问题能否获得 (2-\varepsilon) 近似：
      - 如果可以，给出算法和证明；
      - 如果不行，寻找 hardness 或趋近 2 的反例。

  4. 从真实 LLM DAG 中提取 restricted class，争取在这些子类上得到更好的近似比或最优算法。

  ### 算法目标

  形成一条逐级增强的算法链：

  FIFO
  → Longest-Tail
  → Gate-Aware Longest-Tail
  → Top-k Event Rollout
  → LLM-Specific Motif Scheduler

  所有算法在单瓶颈模型下都保持 work-conserving，从而保留 2-近似安全网。

  ### 系统目标

  最终形成一个可动态重计算的调度器：

  - 输入 residual effective DAG；
  - 输出 ready flows 的优先级；
  - 在 flow ready/completion、compute completion 等事件发生后增量更新；
  - 可接入当前 SimAI executor；
  - 后续能够迁移到真实训练框架。

  ———

  ## 二、阶段 0：DAG 语义审计与特征提取

  这是整个研究的前置门槛。

  ### 0.1 构造 effective DAG

  需要区分：

  workload DAG
      = 数据依赖

  effective DAG
      = 数据依赖
      + compute_order 设备串行边
      + collective completion/join
      + optimizer barrier

  重点核实：

  - PP stage 是否真正只包含所属模型层；
  - 每个 rank 是否错误物化完整模型；
  - B/W、PP gradient、DP、optimizer 的依赖是否正确；
  - collective 多条 flow 如何表示整体完成；
  - compute order 是否完整进入 DAG；
  - activation memory 是否暂时忽略，以及忽略的影响。

  ### 0.2 提取真实结构指标

  对 1F1B、ZB、Interleaved、DualPipe 等模式统计：

  - DAG 节点数、边数、宽度和 critical path；
  - 每个时间切面的 frontier width；
  - fork/join 数量和 join 入度；
  - dominator/post-dominator；
  - single-entry/single-exit region；
  - PP、TP、DP flow 的 slack 分布；
  - flow 成为 join 最后阻塞者的比例；
  - (stage, phase, layer, microbatch) 模板重复率；
  - warmup、steady、cooldown 的周期性；
  - critical backbone 的切换频率。

  ### 阶段产出

  - 一份 DAG 语义说明；
  - validated effective DAG exporter；
  - LLM DAG 特征统计报告；
  - 后续 restricted problem 的选择依据。

  ### 退出条件

  在 timeline、DAG 依赖和 simulator 实际执行之间完成至少一个小 workload 的逐任务对照。

  ———

  ## 三、阶段 1：Benchmark 与精确 Oracle

  先建立可靠的评价体系，避免只凭少数示例判断 heuristic。

  ### 1.1 三类小 DAG

  #### 对抗性 DAG

  专门攻击某类策略：

  - 大 flow 与长 compute tail；
  - longest-tail 反例；
  - LRPT 重复计权；
  - join 假关键路径；
  - fork 多分支解锁；
  - 多个 deferred W 竞争；
  - optimizer 前集中 DP。

  #### 参数化 LLM motif

  至少包括：

  - PP forward wave；
  - PP backward wave；
  - 1F1B microbatch；
  - ZB 的 B/W 分叉；
  - W/DP/optimizer join；
  - TP collective 加 PP；
  - warmup/steady/cooldown。

  #### 真实 DAG 缩减

  从完整 workload 中截取某个冲突窗口，然后：

  - 删除与调度选择无关的节点；
  - 合并固定串行 compute；
  - 保留 flow、fork、join 和边界时间；
  - 缩减到精确算法可以求解。

  ### 1.2 精确解与 lower bound

  小规模使用：

  - 离散时间动态规划；
  - branch-and-bound；
  - CP-SAT/MILP；
  - 必要时枚举 ready-flow 决策序列。

  统一 lower bound：

  [
  LB=\max
  \left(
  P,,
  Q,,
  L,,
  LB_{\mathrm{window}},,
  LB_{\mathrm{cut}}
  \right)
  ]

  其中：

  - (P)：总通信工作量；
  - (Q)：路径上的最大纯计算量；
  - (L)：无争用 weighted critical path；
  - (LB_{\mathrm{window}})：时间窗口 demand bound；
  - (LB_{\mathrm{cut}})：关键 cut 上的通信负载。

  ### 阶段产出

  - 可复现的 benchmark generator；
  - exact oracle；
  - worst-instance 搜索器；
  - 统一的算法评价接口。

  ———

  ## 四、阶段 2：并行链问题

  ### 2.1 理论基线

  首先完整形式化：

  [
  T_H\le P+Q\le2OPT
  ]

  明确证明所需条件：

  - 单瓶颈 channel；
  - work-conserving；
  - 计算 ready 后立即执行；
  - compute resource order 已写入 DAG；
  - 静态有限 workload；
  - 通信抢占语义。

  ### 2.2 Baseline heuristic

  系统比较：

  - FIFO；
  - SPT/LPT；
  - longest-delay；
  - longest-tail；
  - LRPT；
  - earliest-slack；
  - TicTac-style comparator；
  - top-(k) rollout。

  报告：

  - mean、P50、P95、max approximation ratio；
  - worst discovered instance；
  - network idle；
  - compute idle；
  - overlap coefficient；
  - 调度开销和抢占次数。

  ### 2.3 寻找优于 2 的结果

  分三步进行：

  1. 为 longest-tail、LRPT 构造最坏情况族；
  2. 调研一般 minimum-lag parallel-chain 的 approximability；
  3. 研究 LLM 相关 restricted cases：
      - 通信大小相同；
      - compute lag 单调；
      - 每条链至多两个或三个 flow；
      - 重复链；
      - 链深或链数有界；
      - backbone 加共同 deadline side jobs。

  不要把“一般问题优于 2”作为必须完成的目标。更实际的成果可能是：

  > 一般问题保留 2-近似，但对满足某些 LLM 结构条件的子类得到 (3/2)、(4/3) 或最优结果。

  ———

  ## 五、阶段 3：从并行链推广到一般 DAG

  主算法建议为：

  ## Gate-Aware Dynamic Tail Rollout

  ### 3.1 Dynamic tail

  在 residual DAG 上计算：

  [
  q(v)=\max_{\pi:v\leadsto sink}
  \sum_{u\in\pi,u\neq v}d(u)
  ]

  每个 ready flow 被视为一条动态关键链的头部。DAG 更新后重新选择其关键后继，不做永久 chain
  decomposition。

  ### 3.2 Join gating

  识别 flow 是否为某个 join 的最后阻塞前驱。

  对于 (v\to x)，估计：

  [
  g(v,x)=
  \max\left(
  0,,
  EF(v)-\max_{u\in pred(x),u\neq v}EF(u)
  \right)
  ]

  如果其它前驱明显更晚，提前完成 v 的即时价值较低；如果 v 是最后阻塞者，则提高其优先级。

  ### 3.3 Top-k rollout

  每个决策事件：

  1. 用 tail、gating、slack 筛选 top-(k) ready flows；
  2. 假设分别执行每个候选；
  3. 模拟至下一个 flow/compute 事件；
  4. 增量更新 residual DAG；
  5. 计算：
     [
     H(v)=\Delta_v+LB(G_v)
     ]

  6. 选择 (H(v)) 最小的候选。

  建议从 (k=4) 或 (k=8) 开始。

  ### 3.4 理论性质

  算法必须满足：

  - 只调度 ready flow；
  - 有 ready flow 时不主动 idle；
  - profile 不可信时退化到 longest-tail；
  - rollout 超时后仍能立即给出 baseline 决策。

  这样单瓶颈模型下仍继承 2-近似。

  ———

  ## 六、阶段 4：利用 LLM DAG 的特殊结构

  ### 4.1 Critical backbone + deferred side work

  把任务角色分为：

  backbone:
  F / PP_ACT / B / PP_GRAD

  deferred:
  W / DP / optimizer preparation

  但不将它们切成两个独立问题。

  策略：

  - backbone flow 使用 tail 和 compute-unlock urgency；
  - W/DP 使用 optimizer deadline 和 slack；
  - backbone 暂时不紧迫时，用 W/DP 填补通信空隙；
  - W/DP 接近 latest-start time 时自动晋升。

  ### 4.2 周期性调度

  将完整 iteration 分成：

  warmup
  steady-state period
  cooldown

  重点搜索 steady-state 的短周期策略，而不是所有 task 的全排列。

  搜索单位从裸 task 变为：

  (stage, phase, microbatch offset, chunk)

  得到周期后，在相同 pipeline 相位复用。

  ### 4.3 Frontier-state 搜索

  将调度状态压缩为：

  [
  S=
  {f_s,b_s,w_s}{s=0}^{pp-1}
  +
  R{\mathrm{flow}}
  +
  J_{\mathrm{collective}}
  +
  M_{\mathrm{activation}}
  ]

  其中：

  - (f_s,b_s,w_s)：stage (s) 已推进的 microbatch 数；
  - (R_{\mathrm{flow}})：当前 ready flows；
  - (J_{\mathrm{collective}})：join 完成计数；
  - (M_{\mathrm{activation}})：可选的 activation memory 状态。

  如果实际 frontier width 很小，可以尝试：

  - beam search；
  - A*；
  - parameterized DP；
  - 周期状态合并。

  ### 4.4 安全的 DAG 分区

  只在以下情况进行精确组合：

  - iteration/optimizer barrier；
  - 明确的 dominator–post-dominator region；
  - single-entry/single-exit 且没有跨区域 flow；
  - 严格 series composition。

  其它区域不输出单一“局部最优 schedule”，而输出边界 Pareto 状态：

  完成时间
  网络 demand profile
  输出 ready time
  内存状态
  边界未完成任务

  由全局层继续协调。

  ———

  ## 七、阶段 5：完整评测

  ### 算法组

  - FIFO；
  - SPT/LPT；
  - Longest-delay；
  - Longest-tail；
  - LRPT；
  - TicTac-like；
  - Gate-aware tail；
  - Tail + rollout；
  - Gate-aware tail + rollout；
  - LLM-specific scheduler；
  - exact/CP-SAT oracle。

  ### Workload 组

  - 对抗性小 DAG；
  - 随机平行链；
  - 参数化 LLM motif；
  - 缩减真实 DAG；
  - 完整 1F1B/ZB/Interleaved/DualPipe；
  - 不同 PP、GA、TP、DP、flow size 和 compute/communication ratio。

  ### 指标

  - (T_H/OPT)；
  - (T_H/LB)；
  - makespan；
  - network idle；
  - GPU idle；
  - overlap coefficient；
  - critical compute unlock delay；
  - W/DP deadline violation；
  - 调度器运行时间；
  - 抢占次数；
  - profile 误差敏感性。

  需要把 compute/communication ratio 分层报告，因为理论上 heuristic 最有价值的区域是：

  [
  P\approx Q
  ]

  当一方远大于另一方时，不同 work-conserving 策略本来就会接近。

  ———

  ## 八、建议的阶段退出条件

   阶段        退出条件
  ━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   DAG 审计    effective DAG 与小 workload timeline 一致
  ──────────  ──────────────────────────────────────────────────────────
   Oracle      motivating example、随机小 DAG、LLM motif 均能给出精确解
  ──────────  ──────────────────────────────────────────────────────────
   并行链      完成 2-近似正式证明和主要 heuristic 的反例/实验
  ──────────  ──────────────────────────────────────────────────────────
   一般 DAG    gate-aware rollout 在小 DAG 上稳定优于 longest-tail
  ──────────  ──────────────────────────────────────────────────────────
   LLM 特化    至少在两种 PP 策略、多组参数下稳定改进
  ──────────  ──────────────────────────────────────────────────────────
   系统验证    调度收益大于运行时开销，并通过 profile 扰动测试

  ## 最终预期成果

  理想情况下形成三层贡献：

  1. 理论层：单瓶颈一般 DAG 的 2-近似，以及 LLM restricted class 的更强结果。
  2. 算法层：Gate-Aware Dynamic Tail Rollout。
  3. 系统层：利用 PP 周期、B/W slack 和 optimizer deadline 的 runtime-adaptive flow scheduler。

  这条路线的关键是：并行链研究负责提供理论和局部决策直觉；一般 DAG 通过动态关键链、join gating 和
  rollout 扩展；LLM 规律则负责压缩状态空间并改善实际效果，而不是把 DAG 强行切开。

  ———

  ## 九、模型修订：不可抢占通信与主动等待（2026-08-09）

  前述规划及其阶段 1--5 实验曾采用“通信可在整数 tick/event 边界切换，存在 ready flow 时不主动 idle”的研究模型。现在明确最终目标模型为：

  > DAG 中的通信和计算节点一旦开始就必须连续执行到完成；所有通信使用单 channel 的完整带宽；调度器决定完整通信节点的开始顺序，并允许主动等待未来节点完成或新 flow 释放。节点完成后，可以在 residual DAG 上重新规划尚未开始的任务。

  新语义不是给旧算法增加一个 `preemptive=False` 参数，而是改变了动作空间、精确最优解和理论边界。后续规划以本节为准；前述规划中与本节冲突的 tick-level rollout、可抢占 DP、max-min sharing 和 forced work-conserving 结果只作为 legacy 研究记录，不再作为目标模型的性能结论。

  ### 9.1 正式执行模型

  对通信节点 $i$，令完整 channel 容量为 $C$，通信量为 $b_i$，则执行时间为：

  $$
  p_i^{comm}=\frac{b_i}{C}.
  $$

  如果节点在 $s_i$ 开始，则：

  $$
  r_i(t)=
  \begin{cases}
  C,&s_i\le t<s_i+p_i^{comm},\\
  0,&\text{otherwise}.
  \end{cases}
  $$

  区间 $[s_i,s_i+p_i^{comm})$ 必须连续。禁止暂停、恢复、时间片交替和带宽平分。计算节点满足依赖后立即开始，以固定时长连续运行；不同 compute 可以并行，compute 与通信可以重叠，GPU 上固定顺序继续由 effective-DAG edge 表达。

  当 channel 空闲时，状态 $s$ 的合法动作是：

  $$
  A(s)=R(s)\cup\{\mathrm{WAIT}\},
  $$

  其中 $R(s)$ 是当前 ready communications：

  - `FLOW(i)`：选择 $i\in R(s)$，用全部带宽执行到 $i$ 完成；
  - `WAIT`：主动等待到下一个正在运行的 compute 完成事件；
  - 若没有 active compute，等待不会释放新任务，因此 `WAIT` 无意义且不应枚举；
  - flow 执行期间若 compute 完成并释放新 flow，新 flow 进入等待集合，但不能抢占当前 flow。

  只需要把 WAIT 终点放在下一节点完成事件：如果一段开放区间内没有任何 DAG 状态变化，在区间中间结束等待不会产生新决策信息。连续多次 WAIT 可以表达等待多个事件。

  ### 9.2 “极化”与“不可抢占”的关系

  原文的极化结论说明带宽共享不是必要的研究变量，但“每个时刻只有一条 flow 获得全部带宽”并不自动推出“同一 flow 必须连续执行到完成”。新规划把连续执行作为明确的问题约束，而不是继续依赖多面体极点直觉。

  正式理论应分别证明或声明：

  1. 极化：单 channel 上不研究 weighted fair sharing；
  2. 不可抢占：每个通信节点只对应一个连续执行区间；
  3. 可主动等待：即使有 ready flow，调度器也可以等待已知的下一 release event；
  4. 重规划：只改变尚未开始的节点，不能中断 active 节点。

  ### 9.3 Legacy 结果的处理

  下列旧结果需要统一标记为：

  ```text
  Legacy model: preemptive, tick-level, forced work-conserving.
  ```

  包括：

  - 原单 channel exact DP / binary feasibility DP 的最优值；
  - Dynamic-tail 的 96%--97% 经验最优率；
  - tick/event/flow-complete rollout 的比较；
  - Beam、Monte Carlo 和 hard-subset gap-closed 数字；
  - 多资源每 tick compatible-set Oracle；
  - strict-priority max-min executor 的 makespan 收益和 profile 扰动结果。

  DAG 审计、effective dependency、route、模板重复、候选 sidecar、ZB 语义诊断和 lower-bound 构造仍可复用。旧数据不删除，但不得与新模型结果混表或用于新模型的算法结论。

  ———

  ## 十、修订阶段 R0：模型规范与语义回归

  ### 目标

  在重新做性能实验前，先保证 Oracle、heuristic 和时间线遵守同一个不可抢占语义。

  ### 任务

  1. 新增独立研究模型，不修改通用 `Task`/schema 或 baseline executor；
  2. 明确状态中的 pending/running/completed、active compute remaining、ready flow 和 channel 状态；
  3. `FLOW(i)` 必须一次推进到 flow 完成，中途只更新 compute/release 状态；
  4. `WAIT` 只推进到下一 compute completion；
  5. 增加以下最小语义测试：
     - flow 开始后不能被新释放 flow 抢占；
     - compute 开始后不能中断；
     - active flow 执行期间 compute 可以完成；
     - 新 ready flow 等待当前 flow 完成；
     - 有 active compute 时允许 WAIT；
     - 无未来事件时禁止无意义 WAIT；
     - 连续 WAIT 能表达等待多个 release；
     - 任务 ID、依赖和 DAG 无环性保持不变。

  ### 必备反例

  构造主动等待严格必要的实例：

  ```text
  A：t=0 ready，comm(M)，无后续计算
  B：t=1 ready，comm(1) → compute(M)
  ```

  Work-conserving Dynamic-tail 只能先执行 A：

  $$
  T_{DT}=2M+1.
  $$

  最优解等待到 B ready，先执行 B，再让 A 与 B 的 compute 重叠：

  $$
  OPT=M+2,
  \qquad
  \frac{T_{DT}}{OPT}\to2.
  $$

  这个实例应成为永久回归，用于防止未来实现再次隐式禁用 WAIT。

  ### 退出条件

  对至少三个手算 DAG，逐事件核对状态、合法动作、时间线和 makespan；任何输出中不得出现同一 flow 的两个不连续执行区间。

  ———

  ## 十一、修订阶段 R1：Non-preemptive + Optional-idle Exact Oracle

  ### 11.1 精确递推

  新 Oracle 在 channel 空闲状态上递推：

  $$
  F(s)=\min\left\{
  \min_{i\in R(s)}
  \left[p_i+F(\operatorname{finish}(s,i))\right],
  \delta(s)+F(\operatorname{wait}(s,\delta))
  \right\}.
  $$

  - `finish(s,i)` 完整执行 $i$，同时推进所有 active compute；
  - $\delta(s)$ 是距下一 compute completion 的时间；
  - 若没有合法 WAIT，递推只枚举 ready flows；
  - memoization key 必须包含所有未完成 compute 的 remaining time，不能只记录通信顺序前缀。

  ### 11.2 两套最优值

  为分离“排序错误”和“缺少主动等待”，Oracle 同时输出：

  - `OPT_optional_idle`：允许 `WAIT` 的真正目标最优值；
  - `OPT_work_conserving`：有 ready flow 时禁止主动等待的受限最优值。

  对任意 heuristic $H$，把 regret 分解为：

  $$
  T_H-OPT_{idle}
  =
  \left(T_H-OPT_{wc}\right)
  +
  \left(OPT_{wc}-OPT_{idle}\right).
  $$

  第一项表示 ready-flow 排序损失，第二项表示不允许等待的损失。后续不能再把两者混在一个“tail 选错”结论中。

  ### 11.3 Lower bound 与剪枝

  保留：

  $$
  LB=\max(P,Q,L,LB_{window},LB_{cut}).
  $$

  其中 preemptive window-demand bound 是不可抢占问题的松弛，仍是合法必要下界，但不再期待其充分。新增：

  - non-preemptive release/tail bound；
  - 当前 active compute 的下一 release event bound；
  - 相同 chain/microbatch 的对称状态合并；
  - heuristic incumbent；
  - state-count 与 wall-clock hard limit；
  - DP 与独立 branch-and-bound 交叉验证。

  ### 11.4 Benchmark 重跑顺序

  1. `260804组会.md` motivating example；
  2. 主动等待趋近 2 的实例；
  3. 原 `9/8`、`5/4` tail 反例；
  4. `random_join_30/40`；
  5. 七类对抗图和七类 LLM motif；
  6. 固定 seed 随机并行链；
  7. 固定 seed 随机 fork/join DAG；
  8. 原始微秒 duration 的真实缩减窗口。

  ### 退出条件

  两个独立 Oracle 在全部正式小图上得到相同的 `OPT_optional_idle`，并自动验证每条 flow 只有一个连续执行区间。

  ———

  ## 十二、修订阶段 R2：并行链算法与理论

  ### 12.1 新 baseline 组

  所有 priority heuristic 都改成 operation-level：一旦选中就完整执行。

  - Non-preemptive FIFO；
  - SPT/LPT；
  - Longest-delay；
  - Non-preemptive Dynamic-tail；
  - LRPT/earliest-slack；
  - TicTac-style pairwise order；
  - `Idle-unaware` 与 `Idle-aware` 版本分别报告。

  Non-preemptive Dynamic-tail 仍作为立即可用的 work-conserving incumbent，但不再假设它能判断是否应该等待。

  ### 12.2 Full-flow + WAIT Counterfactual Rollout

  每次 channel 空闲时，候选为：

  ```text
  FLOW(i), for i in selected ready-flow candidates
  WAIT_TO_NEXT_RELEASE, if an active compute exists
  ```

  对 flow 候选：

  $$
  \widehat C(\operatorname{FLOW}(i)\mid s)
  =p_i+\widehat J(s_i).
  $$

  对等待候选：

  $$
  \widehat C(\operatorname{WAIT}\mid s)
  =\delta+\widehat J(s_{wait}).
  $$

  补全策略首先使用 Non-preemptive Dynamic-tail；之后再比较 Dynamic/Resource/LLM portfolio。完整 work-conserving baseline 始终作为 incumbent，增强搜索超时或预测失败时立即返回。

  ### 12.3 Beam、Monte Carlo 与伪多项式 DP

  - Beam 的一层是一条完整 flow 或一次 WAIT，不再是一 tick；
  - Monte Carlo 采样完整 flow 顺序和有意义的 WAIT event；
  - DP 状态记录每条链下一通信位置和 compute cooldown；
  - 二分 feasibility 必须允许主动 idle，并使用新的 operation-level transition；
  - 重复链继续做 canonicalization；
  - 报告 state budget、wall-clock budget和 timeout fallback。

  ### 12.4 理论任务

  1. 对独立并行链重写 work-conserving non-preemptive 的：

     $$
     T_H\le P+Q\le2OPT_{idle}.
     $$

  2. 使用主动等待实例证明 Dynamic-tail 的 2 下界 tight；
  3. 重新检查一般 fork/join DAG 的 blocking-chain charging 是否仍成立；
  4. 检查原 NP-hard reduction 的源问题是否不可抢占、允许 idle，以及目标值是否双向保持；
  5. 重新研究带 WAIT rollout 的近似界，而不是继续尝试证明普通 Dynamic-tail 小于 2；
  6. 保留 restricted results：每链单 flow且全部初始 ready、零 lag、等长 flow、有界链数/深度。

  ### 12.5 先验审计结果（只作实现校验目标）

  临时只读原型在原固定 seed 的 100 个随机并行链上得到：

  | method | exact optimal | mean ratio | observed max |
  |---|---:|---:|---:|
  | Non-preemptive Dynamic-tail | 83/100 | 1.01361 | 1.1875 |
  | Full-flow rollout | 89/100 | 1.00820 | 1.125 |
  | Full-flow + WAIT rollout | 92/100 | 1.00645 | 1.125 |

  其中 7/100 的 `OPT_optional_idle < OPT_work_conserving`。这些数字不是正式结果；新实现应能解释任何差异，不能为了匹配临时原型而牺牲语义。

  ### 退出条件

  完成新的 2-bound/tightness 证明、正式随机实验、WAIT 消融和主要 heuristic 反例；所有算法时间线满足节点不可抢占。

  ———

  ## 十三、修订阶段 R3：一般 DAG

  ### 13.1 Residual Dynamic-tail 的新定位

  Dynamic-tail 继续表示 ready flow 完成后的 residual critical tail，但只在 channel 空闲时用于选择完整 flow。active flow 期间可以增量更新未来分数，不能据此切换当前 flow。

  ### 13.2 候选与评价分离

  候选来源包括：

  - Dynamic-tail top-k；
  - SPT/longest-delay；
  - join/latest-start；
  - future-release urgency；
  - `WAIT_TO_NEXT_RELEASE`；
  - 后续拓扑/LLM 语义候选。

  最终评价继续使用端到端 counterfactual makespan，不把 tail、join 和 wait urgency 线性相加。重点新增“等待价值”特征：

  $$
  V_{wait}(s)=
  \widehat J_{start\ now}(s)
  -\left(\delta+\widehat J(s_{wait})\right).
  $$

  ### 13.3 困难集重建

  分开建立三类 hard subset：

  1. `ordering-hard`：`DT > OPT_wc`；
  2. `idle-hard`：`OPT_wc > OPT_idle`；
  3. `combined-hard`：排序和等待都影响结果。

  分别报告 exact 修复数和 gap closed，避免普通实例的高最优率掩盖 WAIT 的独立作用。

  ### 13.4 旧结论的重验

  以下内容必须重新实验，不能直接沿用：

  - raw join bonus 是否仍退化；
  - Join 候选是否有独立覆盖；
  - top-2 是否足够；
  - flow-complete rollout 的实际效果；
  - Beam 相对 rollout 的收益和开销；
  - `random_join_30/40` 的不可抢占时间线。

  ### 退出条件

  Full-flow + WAIT rollout 在 ordering-hard、idle-hard 和 combined-hard 三类集合上均相对 Non-preemptive Dynamic-tail 有稳定正收益，并保留 baseline incumbent。

  ———

  ## 十四、修订阶段 R4：多资源拓扑的不可抢占扩展

  ### 14.1 状态与资源语义

  多 route 模型不再每 tick 重选整个 compatible set。状态必须包含：

  ```text
  active non-preemptive flows
  每条 active flow 的 remaining duration
  被其占用的 directed links / NIC resources
  ready but not started flows
  active compute completion events
  ```

  active flow 直到完成前都不能被候选集合移除。事件发生时只能：

  - 启动与全部 active routes 兼容的新 flow 集合；
  - 或主动等待；
  - flow 完成后释放 route resources；
  - compute 完成只增加 ready candidates。

  ### 14.2 新动作

  $$
  A(s)=
  \{S:S\subseteq R(s),\ S\text{ 与 active flows 兼容}\}
  \cup\{\mathrm{WAIT}\}.
  $$

  只枚举 inclusion-maximal start sets 是否安全，需要重新证明：主动等待和不同 flow duration 可能使“现在多启动一条不冲突 flow”影响未来资源可用性，不能沿用旧 tick 模型中的支配论证。

  ### 14.3 可复用内容

  - topology loader 与 BFS route；
  - directed-link/NIC resource adapter；
  - single-switch、two-rack、four-rack-core；
  - contiguous/cyclic/TP-cross 等 placement；
  - critical path、per-resource load 和 resource-window lower bound。

  ### 14.4 必须重做

  - compatible-set exact Oracle；
  - Dynamic/Resource/Bottleneck pack；
  - Set rollout；
  - full-schedule conflict/action 统计；
  - single-channel 高估比例；
  - 小拓扑的 0.39%--1.15% 收益。

  ### 退出条件

  在手工可算拓扑上验证 active route reservation、不可抢占和 WAIT；再在 exact 小窗口中比较动作合法性与最优值。

  ———

  ## 十五、修订阶段 R5：LLM 结构特化

  LLM sidecar 和候选含义可以保留，但所有 candidate 都必须转成“启动完整 flow/flow set”的动作：

  - backbone-first：启动完整 backbone flow；
  - deferred gap-fill：只在其完整执行不会阻塞下一关键 release 时启动；
  - optimizer deadline：比较“立即启动完整 DP/W”与“等待下一 backbone release”；
  - replica/chunk wavefront：决定同类完整 flow 的开始顺序；
  - dimension candidate：决定本事件启动哪个维度的完整通信集合。

  周期缓存复用候选标签，不缓存会中断 active flow 的旧集合。Teacher 应枚举合法完整动作并包含 WAIT；leave-one-feature-out、teacher coverage 和完整 makespan 全部重跑。

  优先研究的 LLM-specific 问题改为：

  > 在可预测的 PP steady-state release pattern 中，什么时候值得主动等待即将释放的短关键 flow，而不是立即启动一个长 deferred DP/W flow？

  这比继续设计 raw join bonus更符合不可抢占模型。

  ### 退出条件

  至少一个 LLM 特征在两种 workload/placement 上展示相对“Full-flow + WAIT general rollout”的独立收益，而不是只相对 Dynamic-tail 有收益。

  ———

  ## 十六、修订阶段 R6：真实 AICB 与 Executor

  现有 AICB parser、DP override、builder、serializer、effective DAG、topology 和 route 可以继续复用。当前 strict-priority max-min allocator允许 flow 共享带宽、降到零后恢复，不符合目标模型，只保留为 fluid/preemptive 对照。

  新 executor 研究路径需要：

  1. 单 channel 首先实现完整带宽的 non-preemptive flow execution；
  2. channel busy 时新 ready flow 只能排队；
  3. channel idle时允许策略返回 `START(flow)` 或 `WAIT(next_event)`；
  4. 多 route 版本维护 active route reservations；
  5. 不修改生产 Default/Puppeteer/Hermod，继续使用隔离 policy/sidecar；
  6. 输出主动等待次数/时长、forced idle、queue delay、critical unlock delay和调度开销；
  7. 分别比较：
     - Default order；
     - Non-preemptive Dynamic-tail；
     - Full-flow rollout；
     - Full-flow + WAIT rollout；
     - LLM-specific WAIT/ordering strategy。

  Profile 扰动重点不再是 priority tier 抖动，而是：错误等待、错误启动长 flow、release-time 预测偏差和等待阈值鲁棒性。

  ### 退出条件

  真实 AICB 时间线中所有 flow 单区间连续执行；收益覆盖至少两种 PP 策略或明确限定的 restricted class；调度收益大于控制开销，并在 release/profile 扰动下保持正收益。

  ———

  ## 十七、修订后的统一评测与报告口径

  ### 必报算法

  - Non-preemptive FIFO/SPT/LPT；
  - Longest-delay/Dynamic-tail；
  - exact work-conserving order；
  - exact optional-idle order；
  - Full-flow rollout；
  - Full-flow + WAIT rollout；
  - operation-level Beam/Monte Carlo；
  - topology/LLM-specific variants。

  ### 必报指标

  - $T_H/OPT_{idle}$；
  - $T_H/OPT_{wc}$；
  - ordering regret 与 idle regret；
  - makespan 与 `makespan/LB`；
  - voluntary idle / forced idle；
  - network-compute overlap；
  - ready flow queue delay；
  - critical unlock delay；
  - WAIT 次数和等待总时长；
  - 调度决策时间与 exact states；
  - profile/release-time 误差；
  - 非法抢占数必须恒为 0。

  原指标中的“抢占次数”不再是性能指标，而是语义断言：任何非零值都表示实现错误。

  ### 修订后的阶段退出条件

  | 阶段 | 退出条件 |
  |---|---|
  | R0 语义 | 手算时间线一致，任何 flow 都只有一个连续区间 |
  | R1 Oracle | 两个 exact solver 的 `OPT_idle` 一致，WAIT 合法性通过 |
  | R2 并行链 | 2-bound/tightness、WAIT 消融和正式随机实验完成 |
  | R3 一般 DAG | 三类 hard subset 上均稳定改善 Dynamic-tail |
  | R4 拓扑 | active route reservation 与 exact 小窗口闭环 |
  | R5 LLM | 相对 general WAIT rollout 有独立语义收益 |
  | R6 系统 | 真实 AICB 不可抢占闭环、收益超过开销并通过扰动测试 |

  ### 新的最终预期成果

  1. **理论层：** non-preemptive work-conserving 基线的 2-bound及其 tightness，optional-idle restricted class 的更强结果；
  2. **算法层：** Full-flow + WAIT Counterfactual Dynamic-tail Rollout；
  3. **结构层：** 利用 LLM 周期 release、replica/chunk 和 deferred-work deadline缩小完整动作候选；
  4. **系统层：** 不可抢占、可等待、事件驱动的 runtime-adaptive flow-order scheduler。

  修订后的核心研究问题不再只是“当前 ready flows 中谁的 tail 最长”，而是：

  > 现在应该完整启动哪条 flow，还是值得等待下一条即将释放的关键 flow？这个决定如何改变后续通信释放波次、网络空洞和最终 makespan？
