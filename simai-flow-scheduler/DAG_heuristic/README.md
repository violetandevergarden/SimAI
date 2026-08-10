# DAG Heuristic Collaborative Lab

这个目录是 LLM training DAG 通信调度算法的协作开发区。目标是让不同开发者能够在相同的执行语义、
测试集和指标下添加算法，避免每种 heuristic 各自维护一套模拟器。

当前权威语义是：

- 计算和通信节点一旦开始就连续执行到完成；
- 单通道中一条 flow 独占 channel 并完整传完；
- 多通道中 flow 持续占用其完整 route resource set；
- 只在 task-completion event 后重新决策；
- 允许主动 `WAIT`；多通道还允许启动非最大 compatible subset；
- 目标是最小化 DAG makespan。

> 目录名 `muti_channel` 按当前项目约定保留。它表示 multi-channel / multi-resource 场景。

## 目录结构

```text
DAG_heuristic/
├── README.md
├── run.py                         # 三类场景的统一 CLI
├── common/
│   ├── interface.py               # AlgorithmSpec / TestCase 公共接口
│   ├── benchmark.py               # BenchmarkDAG、fixture 与 JSON 转换
│   ├── model.py                   # 单通道不可抢占事件状态机
│   ├── oracle.py                  # Exact DP / Branch-and-Bound
│   └── oracle_benchmark.py        # Oracle benchmark runner
├── single_channel/
│   ├── parallel_chain/
│   │   ├── algorithms.py          # Longest-tail、Rollout、Beam、MC、DP
│   │   ├── interface.py           # 并行链算法注册表
│   │   ├── testsets/
│   │   │   ├── random.py          # 固定 seed 随机实例
│   │   │   ├── adversarial.py     # 紧 2、等待、Beam 反例
│   │   │   └── real.py            # Pipeline chain projection
│   │   └── tests/                 # 原并行链完整回归
│   └── complex_chain/
│       ├── algorithms.py          # 一般 DAG Longest-tail/Join/Rollout/Beam
│       ├── interface.py           # 一般 DAG 算法注册表
│       ├── testsets/
│       │   ├── random.py          # 随机 fork/join DAG
│       │   ├── adversarial.py     # ordering/join 攻击集合
│       │   └── real.py            # LLM motif 与真实 DAG JSON loader
│       └── tests/                 # 原一般 DAG 完整回归
├── muti_channel/
│   ├── algorithms.py              # Pack、set rollout、multi-resource oracle
│   ├── interface.py               # 多通道算法注册表
│   ├── topology_fixtures.py       # 小型 BFS topology
│   ├── testsets/
│   │   ├── random.py              # 随机 route-conflict DAG
│   │   ├── adversarial.py         # reservation/non-maximal 反例
│   │   └── real.py                # 经真实 BFS/resource adapter 转换的图
│   └── tests/                     # 原多通道完整回归
├── tests/                         # 跨场景接口与 runner smoke tests
├── scripts/                       # 三个便捷运行入口
├── archive/                       # 历史 tick/流体原型，仅供追溯
└── docs/                          # 规划、进度、总结、反例和组会文档
```

原 `scripts/` 中的同名路径只用于兼容已有命令和测试。后续算法修改应只发生在本目录。

## 三类测试集

每个场景都公开相同的分类接口：

| 分类 | 用途 | 可复现性要求 |
|---|---|---|
| `random` | 扫描普通分布和平均表现 | 必须显式记录 seed 和样本数 |
| `adversarial` | 攻击某个具体算法或理论猜想 | 固定命名，说明攻击目标和期望差距 |
| `real` | 从 LLM motif、导出 DAG 或真实 route adapter 得到 | 记录 workload/转换来源，不冒充随机图 |

一般 DAG 的真实集合还提供：

```python
from pathlib import Path
from DAG_heuristic.single_channel.complex_chain.testsets.real import load_export

cases = load_export(Path("my_exported_dags.json"))
```

JSON 使用 `common/benchmark.py` 中 `dag_to_json` / `dag_from_json` 的格式。

## 已注册算法

### `single_channel/parallel_chain`

- `longest_tail`
- `rollout_flow2`
- `rollout_wait2`
- `beam_wait8`
- `beam_wait32`
- `exact_optional`

`muti_channel/experimental_llm.py` 保存下一阶段 LLM 结构候选实验，但没有注册为默认算法。

### `single_channel/complex_chain`

- `longest_tail`
- `join_bonus`
- `rollout_flow2`
- `rollout_wait2`
- `depth2_wait2`
- `beam_wait8`
- `exact_optional`

### `muti_channel`

- `longest_tail_pack`
- `resource_pack`
- `bottleneck_pack`
- `rollout_maximal2`
- `rollout_optional2`
- `exact_optional`

Exact 方法只用于小实例标尺，不应作为大 DAG 默认算法。

## 运行方法

以下命令都从 `simai-flow-scheduler` 根目录运行。

列出某场景算法：

```powershell
python -m DAG_heuristic.run parallel_chain --algorithm longest_tail --list-algorithms
```

运行随机并行链：

```powershell
python -m DAG_heuristic.run parallel_chain `
  --algorithm rollout_wait2 --category random --samples 100 --seed 260813
```

运行一般 DAG 攻击集合：

```powershell
python -m DAG_heuristic.run complex_chain `
  --algorithm depth2_wait2 --category adversarial
```

运行多通道真实拓扑转换集合并写 JSON：

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

## 添加一个新算法

算法只需接受该场景的 instance，并返回带整数 `makespan` 字段的结果对象。

```python
from DAG_heuristic.common.interface import AlgorithmSpec
from DAG_heuristic.single_channel.parallel_chain.interface import register


def my_algorithm(instance):
    # 返回现有 ChainSchedule，或自定义但包含 makespan 的结果。
    ...


register(
    AlgorithmSpec(
        name="my_algorithm",
        solve=my_algorithm,
        description="One sentence describing the decision rule.",
        supports_wait=False,
    )
)
```

提交算法时至少同时添加：

1. 一个 registry 条目；
2. 一个能区分它和 Longest-tail 的固定测试；
3. 随机集合上的 seed、样本数、平均比值和最坏比值；
4. 若声称改进某类反例，把该反例加入 `testsets/adversarial.py`；
5. 若声称利用真实结构，把转换来源加入 `testsets/real.py`；
6. 若声称近似比，给出独立证明；有限样本最大值不能写成理论上界。

## 测试

运行新仓库全部测试：

```powershell
python -m pytest `
  DAG_heuristic/tests `
  DAG_heuristic/single_channel/parallel_chain/tests `
  DAG_heuristic/single_channel/complex_chain/tests `
  DAG_heuristic/muti_channel/tests -q
```

当前迁移基线为：

```text
核心 R0--R4 回归：45 passed
包含 experimental LLM：51 passed
```

## 当前结论边界

- 独立并行链：任意不主动等待的完整-flow 策略有紧 2-近似界；
- 一般 DAG：Rollout 保留 Longest-tail incumbent，因此逐实例不差于该基线；尚无一般常数界；
- 多通道：`rollout_optional2` 在当前 37 图集合中为 36/37 最优，但这不是理论保证；
- LLM 特殊结构仍是下一阶段，不作为当前默认算法结论。

详细理论、实验表和反例见 `docs/heuristic总结.md`、`docs/heuristic进度.md` 和
`docs/heuristic反例.md`。
