# 从 99k 额度划拨的 9k 全流程 pilot

本 pilot 从原计划每 campaign 99,000 个 mixed/MiniAOD 事件中划拨额度：
`JJP_SPS_G` 生产 4,000，`JJP_DPS2_G` 生产 5,000，总计 9,000。完成后两者原阶段三剩余额度分别为 95,000 和 94,000；不得把 pilot 事件重复计入剩余额度。

## HELAC profile 与有限输入

2J/psi 使用扫描的初始基线 A：`pool_2jpsi_g`、`nmc=1,000,000`、
`unwevt=100,000`，原过程和 cuts 不变。gg 保持已冻结的低成本参考：
`pool_gg`、`nmc=500,000`、`unwevt=10,000,000`。pilot runtime 的独立
profile 将 planner cap 冻结为 1,000 个 2J/psi LHE 事件/源和 7,000 个
gg LHE 事件/源；这是下游有限规划上限，不改变 HELAC 参数。

| Pool | 源数 | 每源规划上限 | 完整 100-event blocks | profile |
|---|---:|---:|---:|---|
| `pool_2jpsi_g` | 13 | 1,000 | 130 | 初始基线 A |
| `pool_gg` | 4 | 7,000 | 280 | gg 参考 |

保守需求按共享 pool2 物理 blocks 计算：SPS 40 个 MIX × 200 LHE = 8,000；
DPS2 50 个 MIX × 200 pool2 LHE = 10,000。跨 campaign 可共享 pool2 的
BLOCK，因此容量取较大的 10,000，而不是相加。加 25% 后需 125 个完整
blocks；13 个源按 cap 可提供 130 个。gg 需求为 50 × 400 = 20,000 LHE；
加 25% 后需 250 blocks，4 个 gg 源按 cap 可提供 280 个。planner 仍按
每个源的实际 gzip-aware 事件计数运行，容量不足时照实报告。

种子使用独立范围：pool2 `94000..94012`，gg `96000..96003`。后续剩余
99k 生产不得复用这些 pilot source IDs、seeds 或已消费的 BLOCKs；后续
HELAC 种子从 pool2 `94013`、gg `96004` 起。

## 处理规模与 DAG 覆盖

| Campaign | Mixed/MiniAOD 目标 | Processing 节点 | 每节点目标 | Shards | MiniAOD merge |
|---|---:|---:|---:|---:|---|
| `JJP_SPS_G` | 4,000 | 40 | 100 | 4 × 10 节点 | 4 组 × 1,000 |
| `JJP_DPS2_G` | 5,000 | 50 | 100 | 5 × 10 节点 | 5 组 × 1,000 |

覆盖源生成、PLAN、COORD、动态 block SubDAG、两种 campaign 的 shower/MIX、
CMSSW MiniAOD、按 1,000 事件分组的 MiniAOD merge、JJP ntuple、selection/
efficiency、最终 handoff。DPS2 同时覆盖 `pool_2jpsi_g:normal` 和
`pool_gg:phi_mpi_off`；SPS 覆盖 `pool_2jpsi_g:phi_mpi_off`。

Pilot 的 `global_job_index_start` 为 SPS=10、DPS2=10；完成后剩余阶段三
分别从 SPS=50、DPS2=60 继续。阶段二占用 merge group 0，因此 pilot 的
`merge_group_index_start` 为 1；剩余阶段三分别从 SPS=5、DPS2=6 继续。
EDM event identity 仅由这些全局 job index 派生；不覆盖 CMSSW RNG seed，
随机性来自 HELAC 输入硬散射事件。

## 并发与资源

- CERN source DAG：`lhe_planning=10`，同时最多推进 10 个 HELAC/PLAN 源；
  每 LHE 1 CPU、3072 MB、4 GB scratch，HELAC flavour=`tomorrow`。
- Block SubDAG：`block_processing=20`，以 2 CPU/6000 MB 的现有 CMSSW
  配置匹配 worker 请求；source cleanup 上限 20。
- DAGMan：submitted=200、idle=100、submit interval=20、delay=0。
- IHEP combined allocation 使用 `hep_sub -n 2`，在已验证的十台节点上
  轮转；merge/ntuple、selection/efficiency 使用 pilot handoff 的完整流程。

原扫描 DAG `13552706` 和扩展 DAG `13555065` 在提交本 pilot 前仍有 6 个
LHE worker 运行。pilot 可以同在 `bigbird27.cern.ch` 排队；提交记录需注明
该资源重叠，不能据 DAG 上限推断 Condor 立即有空闲槽位。
