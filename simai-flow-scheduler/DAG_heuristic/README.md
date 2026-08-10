# DAG Heuristic 调度算法仓库

这个目录是一个可独立运行的 DAG 调度算法实验仓库，用于让不同开发者在统一的任务语义、测试集和输出接口下开发、比较算法。研究路径按“并行链 → 一般 DAG → 多通道拓扑”组织。

## 当前实现的场景与限制

### 统一调度语义

- DAG 节点分为计算任务和通信任务，依赖只有在前驱任务完成后才满足。
- 所有节点均不可抢占：任务一旦开始，必须连续运行到完成。
- 调度器只在任务完成事件后重新决策，可以主动 `WAIT`。
- 目标是最小化整个 DAG 的完成时间（makespan）。
- 当前时间、任务时长和 makespan 均使用整数。

### 单通道并行链 `single_channel/parallel_chain`

每条链具有 `compute(r_j) -> communication(p_j) -> compute(q_j)` 结构。不同链的计算可以并行，所有通信共享一个独占 channel；任一 flow 开始后会占满 channel 并完整传输。

这个场景适合研究通信顺序、主动等待、短视 rollout 和精确动态规划。它不表达 fork/join、多个通信资源、带宽共享或路由。

### 单通道一般 DAG `single_channel/complex_chain`

计算和通信可以形成任意无环依赖，包括 fork、join 和多层流水。计算任务仍视为可并行，通信任务仍竞争同一个独占 channel。

这个场景比并行链更接近训练 DAG 的因果结构，但仍不模拟 GPU 数量限制、计算资源竞争、具体网络路由和多链路并发。

### 多通道拓扑 `muti_channel`

每个 flow 使用一组固定的独占资源，例如有向链路和端点 NIC。资源集合互不相交的 flow 可以同时运行；flow 启动后持续占用完整 route resource set，直到传输完成。调度动作可以启动兼容 flow 集合，也可以选择非最大集合或 `WAIT`。

当前限制如下：

- 路由预先固定，算法不联合优化选路与调度；
- 资源是排他的，不实现按比例带宽共享；
- 不模拟链路级抢占、分片后重调度和动态路由；
- 拓扑测试主要是小型人工图和固定路由转换图；
- 精确 oracle 只适用于小实例；
- `experimental_llm.py` 是尚未注册的候选实验，不代表已经验证的 LLM 特化算法。
- `real` 测试集和流水线 DAG 导出/审计会读取主仓库的 `src/`、`inputs/`；仅复制 `DAG_heuristic/` 时，随机和人工测试仍可使用，但这些真实转换功能不可用。

目录名 `muti_channel` 是现有公开接口的一部分，虽然拼写不是 `multi_channel`，扩展时请保持兼容。

## 代码结构

```text
DAG_heuristic/
├── README.md
├── run.py                         # 三种场景的统一命令行入口
├── common/
│   ├── interface.py               # AlgorithmSpec、TestCase、makespan 接口
│   ├── benchmark.py               # 通用 DAG 表示、构造器和 JSON 转换
│   ├── model.py                   # 单通道不可抢占事件状态机
│   ├── oracle.py                  # 单通道 exact DP / branch-and-bound
│   └── oracle_benchmark.py        # 精确算法的小实例 benchmark
├── single_channel/
│   ├── parallel_chain/
│   │   ├── algorithms.py          # Longest-tail、Rollout、Beam、Exact DP
│   │   ├── interface.py           # 本场景算法注册表
│   │   ├── testsets/              # random / adversarial / real
│   │   └── tests/                 # 本场景回归测试
│   └── complex_chain/
│       ├── algorithms.py          # 一般 DAG 的优先级、Join、Rollout、Beam
│       ├── interface.py
│       ├── pipeline_dag_export.py # 训练流水线 DAG 导出
│       ├── pipeline_dag_audit.py  # 有效 DAG 语义审计与指标
│       ├── testsets/              # random / adversarial / real
│       └── tests/
├── muti_channel/
│   ├── algorithms.py              # Pack、集合 Rollout、多资源 Exact Oracle
│   ├── interface.py
│   ├── topology_fixtures.py       # 小型固定路由拓扑
│   ├── real_dag_adapter.py        # DAG 到 route-resource instance 的适配
│   ├── experimental_llm.py        # 未注册的 LLM 结构候选实验
│   ├── testsets/                  # random / adversarial / real
│   └── tests/
├── scripts/                       # 三个场景的便捷运行入口
└── tests/                         # 公共接口、runner、model 和 oracle 测试

```

当前对应关系如下：

| 旧职责 | 当前实现 |
|---|---|
| DAG benchmark / exact model | `common/benchmark.py`、`common/model.py`、`common/oracle.py` |
| 并行链研究 | `single_channel/parallel_chain/` |
| 一般 DAG 与流水线审计 | `single_channel/complex_chain/` |
| 多资源、拓扑和真实 route adapter | `muti_channel/` |

导出或审计真实训练流水线 DAG：

```powershell
python -m DAG_heuristic.single_channel.complex_chain.pipeline_dag_export
python -m DAG_heuristic.single_channel.complex_chain.pipeline_dag_audit
```

## 测试集约定

每个场景都提供三类测试集：

| 分类 | 用途 | 要求 |
|---|---|---|
| `random` | 衡量普通分布上的平均表现 | 固定并报告 seed、样本数和生成范围 |
| `adversarial` | 攻击特定贪心规则或理论猜想 | 实例应命名，并说明攻击对象与预期行为 |
| `real` | 来自 LLM motif、导出 DAG 或拓扑适配器 | 记录 workload 和转换来源，不能称为随机实例 |

测试集入口统一为：

```python
cases(category, samples=10, seed=260819) -> list[TestCase]
```

算法结果必须公开整数属性 `makespan`。测试集只负责生成实例，不应把待测算法的决策写入实例。

## 仓库使用方法

以下命令均从 `simai-flow-scheduler` 根目录运行。

列出某个场景的算法：

```powershell
python -m DAG_heuristic.run parallel_chain `
  --algorithm longest_tail --list-algorithms
```

运行随机并行链：

```powershell
python -m DAG_heuristic.run parallel_chain `
  --algorithm rollout_wait2 --category random --samples 100 --seed 260819
```

运行一般 DAG 攻击集：

```powershell
python -m DAG_heuristic.run complex_chain `
  --algorithm depth2_wait2 --category adversarial
```

运行多通道真实转换集，并保存 JSON：

```powershell
python -m DAG_heuristic.run muti_channel `
  --algorithm rollout_optional2 --category real `
  --output DAG_heuristic/outputs/muti_channel_real.json
```

也可以使用便捷入口：

```powershell
python DAG_heuristic/scripts/run_parallel_chain.py --algorithm longest_tail
python DAG_heuristic/scripts/run_complex_chain.py --algorithm rollout_wait2
python DAG_heuristic/scripts/run_muti_channel.py --algorithm rollout_optional2
```

运行全部迁移后测试：

```powershell
python -m pytest `
  DAG_heuristic/tests `
  DAG_heuristic/single_channel/parallel_chain/tests `
  DAG_heuristic/single_channel/complex_chain/tests `
  DAG_heuristic/muti_channel/tests -q
```

Exact 算法用于给小实例提供最优值。比较 heuristic 时，建议同时记录最优率、平均近似比、最坏近似比和算法运行时间，不能把有限测试集上的最大比值写成理论近似界。

## 扩展算法的方法

先在目标场景的 `algorithms.py` 实现求解函数，再在同目录 `interface.py` 注册。求解函数接收一个场景实例，并返回带整数 `makespan` 的结果对象。

以下是并行链算法的最小示例：

```python
from DAG_heuristic.common.interface import AlgorithmSpec
from DAG_heuristic.single_channel.parallel_chain.interface import register


def solve_my_algorithm(instance):
    # 根据不可抢占、单 channel 语义生成调度结果。
    ...


register(
    AlgorithmSpec(
        name="my_algorithm",
        solve=solve_my_algorithm,
        description="Describe the scheduling decision in one sentence.",
        exact=False,
        supports_wait=False,
    )
)
```

注册只是让当前 Python 进程可见。要让 CLI 默认加载算法，应把 `AlgorithmSpec` 静态加入相应 `interface.py` 的 `ALGORITHMS`。

提交新算法至少应包含：

1. 注册表条目和清楚的决策规则说明；
2. 一个能区分该算法与已有 baseline 的固定测试；
3. 在固定随机种子上的性能和运行时间；
4. 若声称解决某类失败案例，将该案例加入 `adversarial.py`；
5. 若利用真实结构，将来源和转换过程加入 `real.py`；
6. 若声称近似比，提供独立证明和紧例。

## 扩展场景或基础代码的方法

如果现有三个场景不能表达新问题，按以下边界扩展：

1. 先写清任务、资源、抢占、等待、带宽和目标函数语义；不要直接复用名字相近但语义不同的状态机。
2. 在独立目录定义 instance 和 schedule/result 类型，保持结果具有整数 `makespan`。
3. 实现合法动作生成、状态转移、完成事件推进和终止判定；所有依赖必须存在且无环。
4. 为小实例实现或适配 exact oracle，用它校验 heuristic，而不是用另一个 heuristic 当真值。
5. 建立 `random`、`adversarial`、`real` 三类 testset，并在 `run.py` 增加场景分派。
6. 添加接口测试、语义测试、oracle 交叉验证和 CLI smoke test。

修改公共层时要特别谨慎：

- `common/model.py` 和 `common/oracle.py` 是单通道不可抢占语义的核心，不是普通工具函数集合；
- 修改合法动作或 `WAIT` 条件后，要同时验证 model、oracle 和所有单通道算法；
- 多通道资源集合与单通道状态不同，不要把多资源逻辑硬塞进单通道 model；
- 真实 SimAI workload 的转换应放在 adapter/testset 层，避免让算法依赖主模拟器的全局状态。

## 当前已注册算法

| 场景 | 算法 |
|---|---|
| 并行链 | `longest_tail`、`rollout_flow2`、`rollout_wait2`、`beam_wait8`、`beam_wait32`、`exact_optional` |
| 一般 DAG | `longest_tail`、`join_bonus`、`rollout_flow2`、`rollout_wait2`、`depth2_wait2`、`beam_wait8`、`exact_optional` |
| 多通道 | `longest_tail_pack`、`resource_pack`、`bottleneck_pack`、`rollout_maximal2`、`rollout_optional2`、`exact_optional` |

详细实验结论、理论证明和反例位于 `docs/heuristic总结.md`、`docs/heuristic进度.md` 和 `docs/heuristic反例.md`。README 只定义当前可运行代码的边界和协作方式。
