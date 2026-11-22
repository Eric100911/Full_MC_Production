# 三阶段计划 Stage2 1k IHEP cutover 小结

- 日期：2026-10-25
- 实验：`three_stage_20261018T022046Z`
- 运行：`three_stage_20261018T022046Z_s2_1k`
- 结果：**完成**

## 验证范围

对 `JJP_SPS_G` 和 `JJP_DPS2_G` 各处理 1000 个事件。CERN 侧每个 campaign 有 10 个 MIX/MiniAOD 节点，每节点 100 个事件；每个 campaign 形成一个 1000 事件 merge group。IHEP 侧验证 merge、ntuple、selection、efficiency 四阶段。

- CERN root DAGMan：`bigbird27.cern.ch:13578063`，正常结束。
- runtime：`v06_00-three_stage_s2_planfix1`。
- core revision：`490207851b8a06f99f506983e759c8b06bdbb085`；`source_dirty=false`。
- IHEP analyzer revision：`1cd1a5a69b85d8cbc6d9c41320d500f20f1602e8`。
- handoff：`generated/three_stage_20261018T022046Z/stage2/cern/handoff.json`；2 个任务，`rejected=[]`，每个任务含四个阶段。
- IHEP 持久目录：`/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production/runs/three_stage_20261018T022046Z/stage2`。

## IHEP 提交与结果

首次生成的未提交 batch 暴露出旧默认值：`request_cpus=1`，安全节点名单包含 `ams020`。在提交前将 handoff 设置为 `ihep_request_cpus=2`，并固定到十个安全节点：`ams017/018/019`、`cws047/048/049/051/052/053/054`。废弃的预检目录保留在 `stage2/batch-preflight-invalid-20261025T`；其中没有提交作业。

最终 manifest 请求 `-n 2`，两个任务分别提交到 `ams017` 和 `ams018`：

| Campaign | HepJob cluster | merge | ntuple | selection | efficiency |
| --- | ---: | --- | --- | --- | --- |
| `JJP_SPS_G` | `82820924` | `ok`，1000 events，62,892,781 B | `ok`，1000 entries，12,010,117 B | `ok`，207,704 B | `ok`，204,800 B |
| `JJP_DPS2_G` | `82820925` | `ok`，1000 events，69,610,566 B | `ok`，1000 entries，12,219,535 B | `ok`，147,926 B | `ok`，184,320 B |

八个 checkpoint 的 identity 均对应各自任务、阶段和 runtime release；八个最终产物的 EOS sidecar 与实际文件大小核验通过。DPS2_G 的 stderr 留有一次 ntuple XRootD 断连信息，但最终 ntuple 及其余阶段产物均通过 sidecar、大小和 provenance 核验，未造成产物缺失。

## 队列为空时的完成判定

对两个精确 HepJob ID 查询时均未返回队列行，manifest 的 scheduler 字段因此保留 `unknown`。这只是调度器当前不可见该作业的状态；它不否定已由持久证据证明的完成结果。

本次两个任务均有四阶段匹配 checkpoint 且 `status=ok`，所有声明产物及 sidecar 有效，merge/ntuple 计数各为 1000。因此 cutover 判定为完成；没有继续追问 scheduler unknown，也没有重提任务。通用操作规则见 [`testing.md`](testing.md) 的 mcprod cutover 小节。

## 证据路径

- CERN run：`generated/three_stage_20261018T022046Z/stage2/cern/`
- CERN handoff 与配置备份：`stage2/cern/handoff.json`、`stage2/cern/handoff.pre-ihep-settings.json`
- IHEP batch manifest：`stage2/batch/batch_manifest.json`
- IHEP submit/status 记录：`stage2/logs/batch_submit.json`、`stage2/batch/logs/`
- IHEP 阶段 checkpoints：`stage2/batch/checkpoints/0000/`、`stage2/batch/checkpoints/0001/`
