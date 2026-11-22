# IHEP cutover pilot 经验记录

记录日期：2026-09-29
适用 run：`dps1_cutover_20260929_r3`
状态：已完成

本记录来自一次完整的 CERN→IHEP JJP_DPS1 cutover pilot。它只记录本轮
实际观察到、并且会影响后续操作的经验。

## 本轮基线

- core revision：`ae605280850ea316af87552b20b83c1620fd28ef`
- NtupleAnalyzer：`1cd1a5a69b85d8cbc6d9c41320d500f20f1602e8`
- runtime release：`production-ae60528-dps1-cutover-20260929-r3`
- 计划 LHE：20 事件；每 block 5 事件；2 个 processing job；merge 目标 9 事件
- subprocess：`normal`、`phi_mpi_off`
- source ID：`sps-jpsi-103`
- inventory SHA256：`9c393a706aa3ad4368a3ca3c10a822cdd65f7c4daee86950df3e4e9044369e68`
- CERN schedd：`bigbird27.cern.ch`
- root DAGMan cluster：`13490580`
- IHEP HepJob：`81739783`，节点 `ams017.ihep.ac.cn`

实际 processing 输出为 5 和 4 个事件，合并与 ntuple 均为 9 个事件。
selection 的 `selected` tree 为 0 条；efficiency 输入 1 个 ntuple 文件，
`n_full_gen=2`，最终通过数为 0。这些数字只用于链路验收。

## 经验、观察和建议

| 经验 | 观察 | 建议 |
| --- | --- | --- |
| 版本与 runtime 成套固定 | 干净 checkout、core、NtupleAnalyzer 和两个 site runtime 一致；旧 runtime 问题没有重现。 | 提交前核对 checkout、子模块指针、runtime release 和 `source_dirty=false`。 |
| schedd 与作业 ID 显式记录 | 默认 `SCHEDD_NAME` 未解析；指定实际 schedd 后才提交成功。 | 保存 schedd、root DAGMan cluster 和 HepJob ID；后续只查询记录的 ID。 |
| 空队列不代表成功 | 最终 `hep_q -i 81739783` 为空，但 worker、checkpoint 和 EOS sidecar 均成功。 | 用 worker 状态、checkpoint、sidecar 和实际产物共同判断终态。 |
| combined handoff 要展开 | handoff 顶层是一个 combined job，四个实际阶段位于 `subtasks`。 | `batch prepare` 后逐项检查 merge、ntuple、selection、efficiency。 |
| 事件数按阶段核对 | merge/ntuple 为 9；selection 为 0；efficiency 有独立的 `n_full_gen` 和 cutflow 计数。 | 不用一个总事件数代表所有阶段；小样本不作物理效率结论。 |
| subprocess 检查实际 MIX task | 两个实际 processing task 都包含 `normal` 和 `phi_mpi_off` 输入。 | 提交前检查生成的 MIX task，而不是只看 campaign spec。 |

## 临时问题处理

### 1. 隔离 checkout 无法使用 `git worktree add`

如果父仓库的 `.git` 目录不可写，`git worktree add` 可能失败。可以在
同一可见文件系统创建独立 clone，再切到目标 core revision，并单独核对
子模块指针。

只有在目标提交和所需子模块提交已经确认可取得时才继续；否则停止 runtime
构建和作业提交，不退回旧 analyzer。

### 2. CERN schedd 未从环境解析

先执行 `myschedd bump` 和 `myschedd show` 取得实际 schedd，再在提交命令中
显式设置：

```bash
_CONDOR_SCHEDD_NAME=<recorded-schedd> ./mcprod run submit --run <run-dir>
```

提交返回后立即保存 `submission.json`，确认只有一个 root cluster；不要因为
前置解析失败就盲目重复提交。

### 3. IHEP runtime 复制不完整

递归 `scp` 可能只复制部分文件。应使用本轮唯一的临时目录和可恢复同步：

```bash
rsync -a --partial <runtime>/ \
  ihep:<new-runtime-parent>/.stage-<release>/
```

同步完成后核对 `runtime.json`、core revision、site、`source_dirty` 和所需
bundle，再把临时目录改名为正式 release。runtime 未完成前不要执行
`batch prepare`。

### 4. IHEP 默认 Python 或 HepJob PATH 不匹配

在 IHEP 登录节点显式设置：

```bash
export PATH=/cvmfs/common.ihep.ac.cn/software/hepjob/bin:$PATH
python3.11 <runtime>/controller/core/mcprod ...
```

提交 wrapper 仍必须使用作业私有的
`credentials/x509_user_proxy`，不能安装共享的 `/tmp/x509up_u$UID`。

### 5. `hep_q -i` 返回空结果

空结果可能表示作业已离开 live queue，也可能是查询环境问题。不要据此
重提交。先保存原始查询输出，再检查：

- `logs/0000.out` 和 `logs/0000.err`；
- 四个 `checkpoints/0000/<stage>.json`；
- EOS 主产物和 sidecar 的 `status`、`complete`、大小和适用事件数；
- 必要时用 `xrdfs stat` 和 ROOT tree entries 独立核对。

### 6. selection 或 efficiency 没有统一事件字段

selection 应读取实际 ROOT tree 的 entries；efficiency 应读取其 manifest、
cutflow 和适用的输入计数。缺少某个阶段的事件字段时，报告该阶段实际可
观测到的计数，并保留“仅链路验证”的结论边界。
