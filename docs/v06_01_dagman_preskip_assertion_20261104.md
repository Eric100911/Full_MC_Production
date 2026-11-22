# v06_01 DAGMan PRE_SKIP ID 冲突与分组 rescue 方针（2026-11-04）

## 状态与范围

本记录保存故障证据、上游源码分析及后续恢复方针。所有时间均为 UTC；
原始 CERN DAGMan 日志在本次故障日期使用 CET，换算时减去一小时。
本次工作仅更新文档，未修改 HTCondor、生产 DAG、事件日志或 ledger，未操作队列。

| 项目 | 值 |
| --- | --- |
| Run ID | `v06_dps2g_tps_1m_20261104_dps2cs_gg_appendinv` |
| Run 目录（相对生产工作区） | `generated/dps2g-tps-replacement-20261103/run-appendonly-production-20261104/` |
| Schedd | `bigbird102.cern.ch` |
| Root DAGMan | `6729016`，由持久日志确认 |
| 生产工具 worktree | `.worktrees/v06_01-append-only-inventory/` |
| Runtime release | `v06_01-append-only-inventory-20261104` |
| Runtime core revision | `f534e0b723937e920d1e74f84dc0d08cca78d222` |
| Assertion 报告的构建版本 | `condor-25.13.3` |

`submission.json` 的 `root_cluster_id` 为 null，不能单独用它推断 root 身份。
这不是 2026-10-26 的旧生产 root `6702429`。

## 已确认的事件链

以下文件均位于上述 run 目录，行号是本次检查时的位置：

| UTC 时间 | 证据 |
| --- | --- |
| 02:46:29 | `run.dag.nodes.log:291`：事件 034 / PRE_SKIP 为 `CLEANUP_pool_2jpsi_g_000003` 记录虚拟 ID `(0.2147483647.1)`。 |
| 07:00:41 | `run.dag.dagman.out:963820`：root 开始 recovery。 |
| 07:01:39 | `run.dag.dagman.out:969050`：root 完成 recovery。 |
| 07:10:12 | `run.dag.nodes.log:10814`：另一个节点 `CLEANUP_pool_2jpsi_g_000191` 使用同一个 PRE_SKIP 虚拟 ID。 |
| 07:10:13 | `run.dag.dagman.out:1002385`：打印 `Reassigning the id of job CLEANUP_pool_2jpsi_g_000191 from (0.2147483647.1) to (0.2147483647.1)`；下一行触发 assertion。 |
| 07:16:01 | 自动重启后的 recovery 再次触发相同 assertion，见 `.dagman.out:1007821`。 |
| 07:22:05 | recovery 再次触发，见 `.dagman.out:1013256`。 |
| 07:29:41 | recovery 再次触发，见 `.dagman.out:1018691`。 |
| 07:38:45 | recovery 再次触发，见 `.dagman.out:1024126`。 |
| 07:38:46 | `run.dag.dagman.log` 记录 root 最后一次退出，return value 4。 |

完整断言内容是：

```text
Assertion ERROR on ((*findResult).second == node)
```

构建路径指向 `condor-25.13.3/src/condor_dagman/dag.cpp:3363`。
它要求同一个事件 ID 在 DAGMan 内部映射到同一个 DAG 节点。
原始日志已经证明两个不同 CLEANUP 节点复用了同一个 PRE_SKIP 虚拟 ID；
结合首次断言前的节点重分配消息，直接触发点高度指向此冲突。

后四次回放最后打印的是 `SOURCE_000191` 的成功 POST，但不能据此判定该 SOURCE
计算失败。首次故障的更直接证据在其后继 CLEANUP 的 PRE_SKIP 事件中。
PRE_SKIP 是跳过节点并视为成功的机制，与 PRE 返回 75、按 DEFER 等待 staging
准入不同；不能将普通准入等待归为此次 assertion 的原因。

## 上游缺陷判断及确定程度

2026-11-04 只读核对了 HTCondor 官方 `main` 源码：

1. `DagSubmit::PreSkipSubmit()` 调用 `GetFakeId()` 分配虚拟 ID；后者递增内部计数器。
   应用 PRE 脚本只返回退出码，并不分配这个 ID。
2. `Dag::Bootstrap()` 在 recovery 结束时调用
   `submitter->SetFakeId(_recoveryMaxfakeID)` 恢复计数器。
3. `dag.cpp` 中更新 `_recoveryMaxfakeID` 的位置在 `ProcessSubmitEvent()`，
   对 recovery 中的 NOOP 提交更新最大虚拟 ID。
4. `ULOG_PRESKIP` 分支直接调用 `TerminateNode(node, recovery)`，
   没有相应的最大虚拟 ID 更新；其事件查找分支会建立 ID 到节点的映射。

据此推导：recovery 可以恢复旧 PRE_SKIP 节点的映射，却遗漏已用编号的最大值；
恢复后的新 PRE_SKIP 便可能分配旧编号，引发映射冲突。本次事件链与该路径吻合。
应优先按 **DAGMan 上游 PRE_SKIP 恢复缺陷** 处理，不能将合法使用 PRE_SKIP
直接归为生产配置错误。

尚未完成生产二进制 25.13.3 的精确源码对照，也未确认官方缺陷编号、修复提交或
已修复版本。上述判断是日志证据与官方 `main` 代码路径支持的高度可信诊断，
不是官方确认，也不表示所有 HTCondor 版本都受影响。

官方参考：

- [PRE_SKIP / NOOP 虚拟 ID 分配](https://github.com/htcondor/htcondor/blob/main/src/condor_dagman/dagman_submit.cpp)
- [Recovery、PRE_SKIP 事件及映射断言](https://github.com/htcondor/htcondor/blob/main/src/condor_dagman/dag.cpp)
- [PRE_SKIP 事件定义](https://htcondor.readthedocs.io/en/23.0/codes-other-values/job-event-log-codes.html)

## 为什么没有 root rescue，以及为何自动恢复没有推进

正常失败收尾由 `main_shutdown_rescue()` 调用 `Rescue()` 写恢复文件。
致命 assertion 的异常退出路径不保证执行这一收尾；本次 root 日志在 assertion
处结束，没有正常的 `Aborting DAG...` / `Writing Rescue DAG...` 消息。
检查时 root 没有 `run.dag.rescueNNN`，但存在 45 份嵌套 DAG rescue；这些是
各子 DAG 自己的状态文件，不等同于 root rescue，也不会主动启动恢复作业。

Root submit 文件的 `on_exit_remove` 表达式对退出码 4 不移除作业，持久事件日志
也显示前几次退出后被重新排队。自动恢复确实发生，但 recovery 再次读取包含
冲突 ID 的 `run.dag.nodes.log`，因此重复失败，无法完成 root 状态重建并继续调度。
最后一次事件为 return value 4 的终止记录；这里只记录结果，不推断最终退出的
额外调度器决策。

`DAGMAN_AUTO_RESCUE = TRUE` 表示启动时选择已有 rescue，不表示 assertion 后
必定能写出 rescue，也不是独立恢复服务。没有 rescue 时，标准 recovery
本来用于从事件日志重建状态；本次持久日志中的 ID 冲突使这条后备路径也失败。

官方参考：

- [正常 rescue 收尾源码](https://github.com/htcondor/htcondor/blob/main/src/condor_dagman/dagman_main.cpp)
- [致命异常退出源码](https://github.com/htcondor/htcondor/blob/main/src/condor_utils/except.cpp)
- [DAGMan rescue 与 recovery](https://htcondor.readthedocs.io/en/25.0/automated-workflows/dagman-completion.html)
- [DAGMan 配置项](https://htcondor.readthedocs.io/en/v25_x-branch/admin-manual/configuration/dagman.html)

## 已实现的恢复工具

总体方案已获认可。工具、命令入口、测试和本文统一维护在
`.worktrees/v06_01-append-only-inventory/`，分支为
`feat/v06_01-append-only-inventory`。生产工作区仍是
`/afs/cern.ch/user/c/chiw/condor/Full_MC_Production`；表中 run 路径不在开发 worktree 内。

本轮复用该 run 部署的 `controller.sh accept-result` 入账，未来再整合控制器内部接口。
本次实现未执行生产补账、控制文件移动或队列提交。

### 命令

```bash
MC=/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/v06_01-append-only-inventory/mcprod
RUN=/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/generated/dps2g-tps-replacement-20261103/run-appendonly-production-20261104

# 默认预览；需要访问记录的 schedd 和必要的远端产物。
"$MC" recovery prepare-rescue --run-dir "$RUN" \
  --schedd bigbird102.cern.ch --root-cluster-id 6729016

# 审阅预览后，补账并准备恢复文件，仍不提交。
"$MC" recovery prepare-rescue --run-dir "$RUN" \
  --schedd bigbird102.cern.ch --root-cluster-id 6729016 --apply

# 仅准备成功后执行。提交结果不明确时不要再次提交。
"$MC" run resume --run "$RUN"
"$MC" run status --run "$RUN"
```

受限沙箱内，上述联网命令应直接使用获准的非沙箱执行。默认预览不修改 run；
临时下载在 `/tmp/chiw/`，输出 JSON 列出候选补账、每组 DONE、剩余节点和阻碍。
预览中的 DONE 包含拟补账成功后的结果，不表示预览已经入账。

### 结果证据和重建

已 accepted 的节点要求对应完成凭据一致，不依赖原始 result 文件，也不重新检查
可能已合法回收的上游文件。未入账节点优先使用完整凭据或本地结果，否则按阶段重建：

| 阶段 | 重建依据 |
| --- | --- |
| LHE / MIX | 精确输出的已发布伴随元数据、实际事件数、task/runtime 身份和远端大小。 |
| MiniAOD | 同上，并保留 partial、expected_events 与 detector profile，交部署控制器对照 MIX 校验。 |
| PLAN | 冻结源计数、原文件引用及精确 block 路径；实际读取 gzip block 统计事件数，不猜测事件数，不重跑 shuffle。 |
| ARCHIVE | 归档伴随元数据和实际 tar 成员，对照已入账 producer 身份、大小及事件数。 |
| RECLAIM | 冻结清单中对象明确不存在时，恢复 already-missing 记录；对象尚在则仍需原回收任务执行。 |

只对尚未入账且有执行证据/结果记录的任务尝试重建，不扫描全 EOS。PLAN block
不制造 sidecar。网络和认证故障不等同于文件不存在。缺失实物、身份冲突、计数
不匹配会被报告，不能绕过校验补账；恢复新产物替换孤立旧凭据的规则见下节。

本地补账按原任务依赖顺序调用部署入口，不添加 SOURCE 连续前缀要求。
重建结果、旧 result 备份和证据来源保存在恢复目录。归档仅 local_path 不同的
情况采用内容等价的已有凭据，保留原始记录，不覆盖不可变凭据。

### 原生 rescue 与控制文件归档

每个 DAG 使用自己的新 partial rescue，保留已入账 worker。root 的 SOURCE、SHARD、
CLEANUP 完成状态另行判断：子 DAG worker 已全部入账但 root POST 未完成时，
root 节点仍不标 DONE，让 DAGMan 正常补完成手续。PRE_SKIP 虚拟 ID 冲突不用于
合并节点；PRE DEFER 75 不视为完成。

必须先确认记录 schedd 上原 root、已知后代和相同运行目录的作业全部离队。
Held 也会阻止准备；工具不自动移除或释放作业。准备期间不允许其他操作员提交同一 run。

`--apply` 生成各组下一编号的 rescue，并把将重新启动的 DAG 的以下控制文件
移入 `recovery/<root-id>-<UTC>/control-files/`，保留相对目录：

```text
.nodes.log  .lock  .dagman.out  .dagman.log
.condor.sub  .lib.out  .lib.err  .metrics
```

不会移动原 DAG、任务、历史 rescue、物理产物或永久 staging ledger 锁。
`recovery/current.json` 和本次 `manifest.json` 记录准备进度；中断后使用同一
`--apply` 入口续接。返回 75 或校验错误时停止本次推进，不高频重试。
已完成准备的重复调用返回既有记录，不再生成一轮 rescue。

本次 submission.json 中 earlier submit 的 already-exists 错误不会被改写为成功：
工具另追加 reconciled-existing-root 记录，保留原失败尝试，并用精确 root
终止日志核实。其他未能解释的提交不确定状态不会自动消除。

`run resume` 会复查准备状态与队列，继续使用记录的 schedd。没有 metrics 时，
可用归档的原 root 终止日志作为失败证据。root 和子 DAG 各自选择最新 rescue，
不统一指定 DoRescueFrom 编号，不使用 -force。

这是重复可用的绕过流程，不修复上游缺陷。未来发生同类失败时，应在相关作业
停止后再次按当时状态准备 rescue，而不是不断回放冲突事件日志。

## 恢复重跑产生新产物时替换旧凭据

用户确认：恢复重跑已产生新文件时，允许新结果替换旧凭据，不能仅因文件大小
变化而永久阻止恢复。`prepare-rescue --apply` 对已验证的新产物执行此规则：
先把尚未提交的旧凭据移入本次恢复目录的 `superseded-receipts/`，保存新旧
输出差异，再调用部署控制器正常入账。不会直接修改 accepted 或 staging 数值。

当前入口处理的是未 accepted、未结算且没有已入账下游引用的孤立凭据。
已提交或已有下游的替换涉及下游状态协调，不能按这个孤立凭据流程处理。

## 实际恢复记录（2026-11-04，UTC）

- 使用本接口恢复 root `6729016` 对应的原 run，未更改输入清单或生产目标。
- 恢复目录：`recovery/6729016-20261104T155035Z/`，清单为该目录的 `manifest.json`。
- 共补账 20 个结果：2 LHE、4 PLAN、5 MIX、6 MiniAOD、2 ARCHIVE、1 RECLAIM。
- 用户确认恢复新文件允许替换旧凭据后，备份并替换 4 份未入账 MIX 凭据：
  DPS2_G BLOCK132，TPS BLOCK181–183；旧文件在 `superseded-receipts/`，差异在 manifest。
- 为 root 和 60 个子 DAG 准备 rescue；root 保留 889 个 DONE 节点，子 DAG
  保留 409 个 DONE worker 节点。归档 425 个控制文件，未移动物理数据。
- `2026-11-04T16:09:23Z`：`mcprod run resume` 成功提交，新的 root 为
  `6737764`，schedd 为 `bigbird102.cern.ch`，使用 `run.dag.rescue001`。

本次使用正常入账规则更新 state/staging，未修改 HTCondor 源码。

提交后仅检查一次：`2026-11-04T16:10:32Z`，root `6737764` Running，
root 节点 DONE=889、Failed=0；树中 15 个 DAGMan Running，97 个 worker Idle，
worker Running=0、Held=0。root 日志确认加载 rescue001，未见新的 assertion。
旧 `mcprod_snapshot.py` 因 SHARD state 无 next_step 在联网前退出，本次使用
精确 root 的 `condor_q -name bigbird102.cern.ch -dag 6737764` 完成检查，未持续轮询。

## v06_01 改用 ALWAYS_RUN_POST（2026-11-05）

新生成的 `campaign_shards` root / 子 DAG 统一启用
`DAGMAN_ALWAYS_RUN_POST = TRUE`，root 不再写出 `PRE_SKIP` 指令。
PRE 仍使用原有 0 / 75 / 99 约定：0 正常执行，75 按 DEFER 等待，99 表示无需执行。
99 通过 PRE 非零→不提交 worker→执行 POST 的普通路径完成，避免 PRE_SKIP 虚拟 ID。

所有新生成 POST 传递 `--pre-return-code $PRE_SCRIPT_RETURN` 与
`--return-code $RETURN`。`complete-source`、`complete-shard`、`accept-result`
增加可选 `--pre-return-code`，省略时为 0，兼容既有 DAG 和本地恢复入口。
没有 PRE 的 worker 使用 DAGMan 的 -1，也继续正常入账。

只有 PRE=99、worker return=-1004，并且提交端 state 已持久记录对应的
source skipped 或 cleanup skipped/completed/urls=[]，POST 才确认成功。
不创建虚假 worker result 或 accepted 记录，不把已跳过 source 改成 completed。
SHARD 当前没有无需执行的 PRE 分支，99 仍作为错误；其他 PRE 错误始终失败。
PRE 的 75 不会被 POST 转成自己的 DEFER，只有 POST 自身的锁忙等等待可返回75。
正常 worker 失败仍按原规则处理，正常完成仍执行全部入账和子 DAG 完成校验。

本次只修改 v06_01 生成器与控制器，不改 legacy wave 生成器，不改写正在运行的
生产 DAG/config/launcher。启用需要在明确检查点使用配套控制器及新 DAG/config；
只热换 controller.sh 或只编辑磁盘 config 不能视为运行中的 DAGMan 已切换。
旧 DAG 的 PRE_SKIP 规则保持原语义，不能与新规则混用后宣称问题已消除。

验证包含多个节点跳过、PRE75 defer、在第三个 POST 中途终止 DAGMan 后回放恢复、
此前完成节点不重做，以及真实 PRE42 仍导致失败。离线 native DAGMan 测试在
`/tmp/chiw/` 中运行，worker submit 文件故意不存在，不提交任何实际作业：

```bash
MCPROD_NATIVE_DAGMAN_TEST=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
  python3 -m unittest tests.test_core_post_completion.NativePostRecovery
```

本机已验证版本为 HTCondor 25.0.14；这不等于已对生产 schedd 的 25.13.3
二进制完成相同验证。当前生产 root 未在此次开发中重启或切换。

## 生产切换记录（2026-11-05，UTC）

再次 assertion 的当前 root 是 `6739360`，于 `09:59:07 UTC` 退出；切换前
精确队列与后代检查为空。恢复接口为该 root 准备 `run.dag.rescue003`，保留
root 的 1,791 个 DONE 节点和子 DAG 的 362 个 DONE worker；TPS BLOCK237
确认为计算失败，保留为正常重跑节点，不补造成功结果。

新控制器 `v06_01-control-20261105-post-completion` 以已部署的 v06_01 inventory
控制器为底座，仅加入 POST 协议相关的 application/shards.py、planning/shards.py、
CLI 三处变更。干净构建源码 revision 为 `acaa0d269e258ccad917b534cbee8d230dc92b97`，
产物位于 `generated/controller-releases/v06_01-control-20261105-post-completion/`。
worker runtime、run.json 和输入规格保持原值。

使用可复用脚本进行停止态切换，默认预览，加 `--apply` 才转换文件并激活控制器：

```bash
python3 tools/migrate_post_completion.py \
  --run-dir <run-directory> --build-dir <prepared-controller-build> [--apply]
```

该脚本要求本次 root 已完成 rescue 准备、队列为空；只删除 PRE_SKIP 行、为相关
POST 补充 PRE 参数，并将共享配置设置为 TRUE。原 DAG、配置位于本次恢复目录
`post-completion/original/`，启动器由现有 controller activate 接口备份。
本次转换 41 个 DAG 和一个配置文件，核对了 5,768 条 POST 命令。
`migration.json` 完成前，`run resume` 会拒绝提交；脚本本身不提交作业。

恢复入口同时修正缺失远端伴随元数据的判断：实际失败的 worker 没有发布记录时，
应作为待重跑节点，而不是误报已发布结果损坏；成功记录缺失产物时仍阻断。

`2026-11-05T13:05:33Z` 已通过 `mcprod run resume` 提交新 root **6747397**，
schedd **bigbird102.cern.ch**，采用 **run.dag.rescue003**。

提交后的单次检查确认 root Running、Failed=0；5 个 DAGMan Running，
7 个 worker Running、11 个 Idle、Held=0。现场 root 日志确认配置加载为 TRUE，
并于 `13:06:11–13:06:12 UTC` 记录 `CLEANUP_pool_gg_000214` 的 PRE99→POST 成功，
证明新协议已经实际执行，而非仅修改生成文件。检查后停止轮询。
