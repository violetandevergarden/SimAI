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