# JJP 全规模 IHEP handoff 设计（2026-09-22）

本文固定全规模生产的口径和边界，不代表已经生成或提交全规模 DAG。

## 1. 设计口径

这里的 subprocess 对应当前仓库 `JJP_ALL` 中的六个 campaign：

```text
JJP_SPS_CS
JJP_SPS_G
JJP_DPS1
JJP_DPS2_CS
JJP_DPS2_G
JJP_TPS
```

“每个 subprocess 约 (10^6) 个事例”指最终混合输出的事例数，不是
LHE 文件数、输入 LHE 事例数，也不是 ntuple 文件数。

处理边界固定为：

```text
CERN HTCondor MIX-only
    ↓ audit-split --stage mix
IHEP handoff
    ↓ MERGE gate
    ↓ NTUPLE gate
```

CERN 不运行正式 MERGE 或 NTUPLE。IHEP 的 coordinator manifest 是输入权威；
不得从文件名或文件大小反推事件数。

## 2. 现有清点库存和计算依据

本次只读计算使用已完成清点的库存：

```text
文件：generated/lhe_inventory_jjp_20260717_184812.json
SHA256：9c393a706aa3ad4368a3ca3c10a822cdd65f7c4daee86950df3e4e9044369e68
```

库存总量为：

| pool | 文件数 | 清点 LHE 事例数 |
|---|---:|---:|
| `pool_2jpsi_cs` | 1000 | 24,701,792 |
| `pool_2jpsi_g` | 1900 | 387,456 |
| `pool_jpsi_CSCO_g` | 2000 | 141,412,548 |
| `pool_gg` | 2000 | 51,721,627 |

采用已有 campaign job spec 中的 pilot 效率和每个处理节点的 LHE 预算：

| subprocess | 每个 source slot 的 LHE budget | pilot efficiency |
|---|---|---|
| SPS_CS | 1000 | 0.763 |
| SPS_G | 200 | 0.780 |
| DPS1 | 860, 1000 | 0.932862, 0.802286 |
| DPS2_CS | 1000, 1100 | 1.000, 0.909 |
| DPS2_G | 200, 400 | 1.000, 0.909 |
| TPS | 1000, 1000, 977 | 0.932862, 0.932862, 0.955143 |

估算使用：

```text
expected events per processing node
    = min(LHE budget × pilot efficiency for every source slot)
```

实际 coordinator 还会受到 1000-event 或 200-event block 边界影响，所以最终
可用节点数必须以生成后的 allocation/coordinator manifest 为准。

## 3. 现有库存下的可行性

下表是按当前库存和上述预算得到的目标节点数。目标节点数取到预计产出略高于
(10^6) 的最小整数；它是设计基线，不是已经执行的生产结果。

| subprocess | block | 目标处理节点 | 预计混合事例 | 当前库存结论 |
|---|---:|---:|---:|---|
| SPS_CS | 1000 | 1311 | 1,000,293 | 可行 |
| DPS1 | 1000 | 1247 | 1,000,420 | 可行 |
| DPS2_CS | 1000 | 1001 | 1,000,900 | 可行 |
| TPS | 1000 | 1072 | 1,000,028 | 可行 |
| SPS_G | 200 | 6411 | 1,000,116 | 当前不可行 |
| DPS2_G | 200 | 5000 | 1,000,000 | 当前不可行 |

当前 `pool_2jpsi_g` 的低产出文件和 200-event block 边界只能形成约 1461 个
完整的 DPS2_G/SPS_G 处理位置。对应的预计产出约为：

```text
SPS_G  ≈ 227,916 events
DPS2_G ≈ 292,200 events
```

因此不能通过把 DAG 的节点数写大来解决问题；那只会让 coordinator 在源池耗尽
后失败。

若两个 G 类 subprocess 都要达到约 (10^6)，在当前预算下，
`pool_2jpsi_g` 至少需要约：

```text
SPS_G  : 1,282,052 LHE events
DPS2_G : 1,000,000 LHE events
合计   : 2,282,052 LHE events
```

这是事件数下限。正式设计应在补充 HELAC LHE 后重新清点，并保留 block 尾部和
pilot 效率波动的余量；在新的带 SHA256 的 inventory 产生前，不生成 G 类全规模
job spec。

## 4. 两个 CERN MIX-only 流程

不要把 1000-event 和 200-event block 混进同一个全局 v2 planning spec。当前
planning schema 对一次生成使用一个全局 `block_events`，强行统一为 200 会把
大池切成过多 block，统一为 1000 又会让 G 类预算严重跨 block 超额消费。

### 4.1 主流程：SPS_CS、DPS1、DPS2_CS、TPS

新建一个带明确 `campaign_allocation_order` 的 v2 job spec，建议顺序固定为：

```text
JJP_SPS_CS
JJP_DPS2_CS
JJP_DPS1
JJP_TPS
```

使用：

```text
planning.block_events = 1000
planning.shuffle_mode = stratified
planning.drop_incomplete_last_block = false
```

campaign 的 `max_processing_nodes` 先冻结为：

```text
JJP_SPS_CS  = 1311
JJP_DPS2_CS = 1001
JJP_DPS1    = 1247
JJP_TPS     = 1072
```

这四个 campaign 共享 pool 时，必须由同一个 planning spec 的 allocation manifest
分配不重叠的 source block；不能分别生成四个互不知情的 planner 后再拼接结果。

### 4.2 G 类流程：SPS_G、DPS2_G

补充并重新清点 `pool_2jpsi_g` 后，另建一个 v2 job spec：

```text
planning.block_events = 200
planning.shuffle_mode = stratified
planning.drop_incomplete_last_block = false
```

campaign 的 `max_processing_nodes` 冻结为：

```text
JJP_SPS_G  = 6411
JJP_DPS2_G = 5000
```

两个 campaign 必须共享同一份 G 类 allocation manifest，确保两者消费不同的
`pool_2jpsi_g` block。新 inventory 至少要证明这些节点的完整预算可以被分配；
只看总文件数不够。

## 5. IHEP handoff 执行顺序

每个 CERN MIX-only 流程完成后：

1. 对冻结 coordinator manifest 执行 `audit-split --stage mix`，生成 split
   manifest；这一步只接受完整且可 merge 的 processing sidecar。
2. 在 IHEP 登录节点串行运行 `ihep_build_batch.py`，生成持久的
   `merge-ntuple` workspace，并执行 pre-submit merge audit。
3. 只有 MIX gate 完成后提交 MERGE；IHEP MERGE 不钉节点。
4. MERGE gate 完成后才提交 NTUPLE；NTUPLE 从一开始 round-robin 钉到
   AVX2-safe 的 11 台节点。
5. NTUPLE gate 完成后再做最终 inventory；不要在 retry cluster 运行时重新
   audit。

每个约 (10^6) 事例的 subprocess，以 5000-event merge target 粗略对应约
200 个 merge/ntuple group。实际 group 数由 coordinator manifest 中的实际
component event counts 决定，不由这个粗略数字替代。

## 6. 存储和提交边界

按现有 `bytes_per_final_event = 90000` 的配置，六个 subprocess 各约 (10^6)
事例的最终产物下限约为：

```text
6 × 1,000,000 × 90,000 = 540,000,000,000 bytes ≈ 540 GB
```

这只是最终产物估算，不包括组件 MiniAOD、合并 MiniAOD、暂存文件、日志和
manifest。正式 spec 仍需把保留中间产物和已有 retained bytes 纳入 warning/hard
limit 检查；不能用 540 GB 代替完整 storage gate。

正式生成和提交前必须再次确认：

```text
XRootD 输出 URL 使用 root://cceos.ihep.ac.cn:1094///store/...
log_root 位于 AFS 或 IHEP 持久存储，不在 /tmp
每个 stage 都保留冻结 manifest、gate、cluster ID 和 UTC 提交时间
不通过 legacy full-chain CERN MERGE/NTUPLE 路径
```

## 7. 当前 go/no-go

```text
主流程四个 subprocess：可以在新的 combined v2 spec 验证后生成 MIX-only DAG
G 类两个 subprocess：等待 pool_2jpsi_g 补充、重新清点、校验 SHA256 后再生成
全六 subprocess 的正式提交：在两个 spec 和两个 MIX gate 都准备好之前不提交
```

本设计中的数字来自源码、现有 job spec 和已完成 inventory 的只读计算；尚未
执行全规模 DAG，也未提交任何全规模 HTCondor 或 HepJob 作业。
