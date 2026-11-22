# v06.01 控制器升级与启动器切换

控制器版本是提交端使用的独立代码包，内容来自现有的 `CORE_PAYLOAD`
白名单。它与 `run.json` 中记录的 worker runtime release 分开管理。准备或切换
控制器不会改写 `run.json`、冻结的 task、DAG、worker package 或 `state.json`。
切换后，已经启动的 PRE/POST 进程继续使用启动时加载的代码；之后启动的进程
读取新的 `controller.sh`。回滚会恢复切换前保存的启动器字节和权限位。

每次构建都要使用一个新的、位于持久存储上的 release 目录。构建记录包含源代码
Git revision、构建时的 dirty 状态，以及 payload bundle 的 SHA256。`generated/`、
凭据和 Git 数据不属于 payload。

```bash
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPYCACHEPREFIX=/tmp/chiw/v06_01_implementation/candidate/pycache \
/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/v06_00-staging-scheduler/mcprod controller prepare \
  --core /path/to/Full_MC_Production \
  --release v06_01-control-20261030-02 \
  --output-dir /path/to/controller-releases/v06_01-control-20261030-02
```

离线查看 run 当前的启动器指向和控制器构建信息，不会查询 Condor：

```bash
/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/generated/controller-releases/v06_01-control-20261030-02/controller/core/mcprod \
  controller status --run-dir /path/to/run
```

在选定的 DAG 检查点切换共享的 `controller.sh`。根 DAG 和子 DAG 都调用这个
启动器，因此不需要重新生成 DAG。切换使用独立锁，先持久保存原启动器，再通过
原子替换更新启动器。之后启动的 PRE/POST 进程会获得固定的
`MCPROD_CONTROLLER_BUILD`。以下三个优化选项默认关闭：

```bash
/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/generated/controller-releases/v06_01-control-20261030-02/controller/core/mcprod \
  controller activate \
  --run-dir /path/to/run \
  --build-dir /path/to/controller-releases/v06_01-control-20261030-02 \
  --admission-hints --producer-index --control-events
```

`--admission-hints` 设置 `MCPROD_ADMISSION_HINTS=1`，`--producer-index` 设置
`MCPROD_PRODUCER_INDEX=1`，`--control-events` 设置
`MCPROD_CONTROL_EVENTS=1`。启动器会先清除这三个环境变量，再按命令行选项设置，
避免从 DAGMan 环境意外继承开关。

Admission hint 是可丢弃的负向缓存，不会授权或分配工作。它基于一次成功读取的
state 生成。每份快照的 TTL 是从读取 state 的 `observed_at` 起算 120 秒；写入缓存
较晚或发布顺序颠倒都不会延长旧快照的 TTL。之后成功读取到的新 state 可以发布
新快照并刷新 TTL。无效或过期时 PRE/POST 回到带锁的权威检查。命中一份接近过期的
快照后，DAGMan 按脚本配置等待 60 秒的 `DEFER` 间隔再试；如果重试时快照已过期，
这次重试会执行权威检查。

Producer index 只是已接受 producer receipt 的查找索引。每次查询都会校验索引的
`accepted_count`，并校验命中的 accepted marker 和 receipt。旧控制器若写入新
accepted marker，会使计数不一致；新代码随后会扫描 receipt 并重建索引。receipt
和 accepted marker 仍是事实来源。

可在检查点通过 `sync` 预热索引：

```bash
/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/generated/controller-releases/v06_01-control-20261030-02/controller/core/mcprod \
  controller sync --run-dir /path/to/run
```

`sync` 在现有 staging 非阻塞锁内重建索引，只改 state 中的 `producer_index` 字段。
锁被占用时返回 exit code 75。它是预热操作，不会阻止仍在运行的旧控制器继续写入；
后续写入可能让索引再次过期，查询时仍会根据 `accepted_count` 检测并回退到 receipt
扫描。

回滚时可用 `--expect-build` 确认当前活动版本：

```bash
/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/generated/controller-releases/v06_01-control-20261030-02/controller/core/mcprod \
  controller rollback --run-dir /path/to/run \
  --expect-build v06_01-control-20261030-02
```

status 中的 `runtime_release` 是保持不变的 worker 身份；`controller_build_id`、
构建目录、源 revision 和 dirty 状态描述之后启动的控制器进程。status 只检查磁盘
上的启动器指针，不能证明已运行进程加载了哪个版本。需要运行时证据时，应检查
DAGMan 日志。

## 本次候选构建验证（未部署）

2026-10-30 在主 checkout 生成候选 02。候选 01 的 smoke event 把
`PLAN_000000` 记录成 `PLAN_000000:0`，已由候选 02 替代；候选 01 未部署，也未再修改。
候选 02 源自 `.worktrees/v06_00-staging-scheduler`，revision 为
`fdce0e9b9fd1f4b866174800d7101801572df956`；构建时工作树为 dirty，详细文件清单在
manifest 的 `source_status` 中。payload bundle SHA256 为
`9f61c1b9da2ed339f71a7d07794b6e693e9872ac6b2dc8c2b68edf93820b58b2`。
归档包含 91 个成员，`__pycache__` 与 `.pyc` 均为 0；其中包含最新的
`staging.py`、`shards.py`、`producer_index.py`、`admission_hints.py`、
`control_events.py`、CLI 和控制器模块。

```bash
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPYCACHEPREFIX=/tmp/chiw/v06_01_implementation/candidate/pycache \
/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/v06_00-staging-scheduler/mcprod controller prepare \
  --core /afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/v06_00-staging-scheduler \
  --release v06_01-control-20261030-02 \
  --output-dir /afs/cern.ch/user/c/chiw/condor/Full_MC_Production/generated/controller-releases/v06_01-control-20261030-02
```

候选目录：
`/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/generated/controller-releases/v06_01-control-20261030-02`。
`controller/core/mcprod` 存在且可执行；tarball 的实际 SHA256 与 manifest 一致。

使用候选控制器对当前 run 执行离线 status：

```bash
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPYCACHEPREFIX=/tmp/chiw/v06_01_implementation/candidate/pycache \
/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/generated/controller-releases/v06_01-control-20261030-02/controller/core/mcprod \
  controller status \
  --run-dir /afs/cern.ch/user/c/chiw/condor/Full_MC_Production/generated/v06-dps2g-tps-launch-20261026/run-manual-20261026T081209Z
```

命令返回 exit code 0。status 识别到原始、未托管的 campaign_shards 启动器；
`runtime_release` 仍为 `v06-dps2g-tps-fdce0e9b-20261026`，原始 `core_revision`
仍为 `fdce0e9b9fd1f4b866174800d7101801572df956`。真实 run 的 `controller.sh` 在
status 前后保持相同 inode `198808632`、大小 568、权限 `0755` 和 SHA256
`fe6d953a6be25885d799190f2dc6876f19fc218231c555b096e2868fab1857d1`。
status 的 `live_processes_verified` 为 `false`。候选包没有应用到这个 run。

候选 02 合成 run：`/tmp/chiw/v06_01_implementation/candidate/synthetic-run-02-rv82ixrw/run`。
只对这个 scratch run 激活候选并启用三个 flag；共享 wrapper 的 PRE
`prepare-source --source-index 0` 与 POST
`accept-result --task-id PLAN_000000 --return-code 0` 均返回 0。后者使用 fixture
生成的旧 worker 风格 PLAN result，没有运行物理 worker。两个结构化事件的 build ID
均为 `v06_01-control-20261030-02`，runtime 均为 `test-release`，且都有
`state_committed: true` 和 `commit_outcome: replacement_synced`。task 字段分别精确为
`SOURCE_000000` 和 `PLAN_000000`；PLAN accepted marker 与 producer index 已提交。
合成 run rollback 返回 0，原 wrapper 字节与 mode 均恢复。详细事件在
`/tmp/chiw/v06_01_implementation/candidate/synthetic-smoke-02.json`。

对应的合成命令如下；这些命令只指向 `/tmp` 下的 fixture run：

```bash
/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/generated/controller-releases/v06_01-control-20261030-02/controller/core/mcprod \
  controller activate \
  --run-dir /tmp/chiw/v06_01_implementation/candidate/synthetic-run-02-rv82ixrw/run \
  --build-dir /afs/cern.ch/user/c/chiw/condor/Full_MC_Production/generated/controller-releases/v06_01-control-20261030-02 \
  --control-events --admission-hints --producer-index

/tmp/chiw/v06_01_implementation/candidate/synthetic-run-02-rv82ixrw/run/controller.sh \
  prepare-source --source-index 0
/tmp/chiw/v06_01_implementation/candidate/synthetic-run-02-rv82ixrw/run/controller.sh \
  accept-result --task-id PLAN_000000 --return-code 0

/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/generated/controller-releases/v06_01-control-20261030-02/controller/core/mcprod \
  controller rollback \
  --run-dir /tmp/chiw/v06_01_implementation/candidate/synthetic-run-02-rv82ixrw/run \
  --expect-build v06_01-control-20261030-02
```

## 最终验证交接

本次最终本地检查通过：308 个 `test_core_*.py` 测试、19 个 DAG progress 测试、
9 个 coordinator 测试和 5 个 campaign job spec 检查。命令分别为：

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_core_*.py'
python3 -m unittest tests.test_dag_progress
python3 tests/test_coordinate_lhe_blocks.py
python3 tests/test_campaign_job_specs.py
```

既有隔离 AFS/DAGMan 探针记录在
`generated/v06_00_site_validation_20261017T104551Z/`：两个真实 schedd 主机
`bigbird11.cern.ch` 和 `bigbird22.cern.ch` 上的 G3 互斥、解锁后读取、进程退出恢复、
PRE DEFER、PRE_SKIP、dynamic SubDAG、AutoRescue 检查均通过；G4 assessment 的
19 项检查为 true，合成 DAG 恢复后 10/10 节点完成。该探针不模拟 AFS 服务端、网络、
主机或断电故障，也没有运行物理 worker。

当前 run 的单次一致 state snapshot 为 15,453,743 bytes。兼容性读取通过新 staging
检查，并用 2,028 次 immutable accepted receipt 读取在 4.331 秒内构建出 8,892 项
producer index，没有发现重复引用歧义。三个抽样 MIX 查找各读一个 receipt；排序扫描
基线分别需 1、686、1,370 次读取。旧冻结 `staging.py` 的 `_check` 接受带新索引字段的
state；仅在 `/tmp` 合成副本上运行旧 transaction 后，索引字段仍保留。

13 MB 规模的暖本地计时共 7 次：事务路径由 136.165 ms 降至 59.275 ms；hint 读取为
0.434 ms。hint 数字不含 Python 进程启动时间；这些计时不代表冷缓存、AFS 或生产时延。

本候选没有实现 sparse cleanup、清理协议变更、数据库迁移或 inventory index。当前
cleanup 的 frozen-set 漂移恢复问题仍在范围外。本地 immutable writer 会同步新建的父目录
直到第一个已存在的 ancestor；它假设该 ancestor 已由 run/setup 持久化，重启后无法判断
哪个已有目录可能来自此前中断的目录创建。该前提已在 `src/full_mc/io.py` 中说明。
本节记录候选准备阶段的验证；该阶段结束时尚未对真实 run 执行 activate 或 sync。实际部署
记录见下节。

## 生产部署记录

候选 02 于 2026-10-30 09:45:37.164 UTC 切换到当前 run。操作针对已记录的根 DAG
`6702429@bigbird102.cern.ch`；切换前 DAGMan 正在运行。没有改写 DAG、冻结 task、worker
runtime、`run.json` 或物理 worker。`run.json` 的 SHA256 前后均为
`ef44a7799ec0e466500964efb36d665d312cefdf00ca329b363f173ba9788ef9`；其中的
`runtime_release=v06-dps2g-tps-fdce0e9b-20261026` 和 core revision 保持原值。

候选 ID 为 `v06_01-control-20261030-02`，payload SHA256 为
`9f61c1b9da2ed339f71a7d07794b6e693e9872ac6b2dc8c2b68edf93820b58b2`；`controller.json`
SHA256 为 `c451a67a9cccb0b17cbac7c85ba12d4344fb97101f17a6559ccb32531cba5204`。部署前执行
`controller sync` 一次即成功，重建索引报告 `accepted_count=5045`、
`indexed_artifact_count=8895`。随后 `controller activate` 返回 0，启用了
`control_events`、`admission_hints` 和 `producer_index`。离线 status 返回 0，确认新
launcher 指向候选 02，且 `runtime_release` 未变。

原 launcher 的逐字节备份保存在：
`generated/v06-dps2g-tps-launch-20261026/run-manual-20261026T081209Z/.controller-switch/backups/launcher-fe6d953a6be25885d799.sh`。
备份 mode 为 `0755`，SHA256 与切换前原文件相同。若需回滚，使用候选控制器的绝对路径；
`--expect-build` 可防止覆盖意外发生的后续 launcher 切换：

```bash
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPYCACHEPREFIX=/tmp/chiw/v06_01_rollout/deploy/pycache \
/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/generated/controller-releases/v06_01-control-20261030-02/controller/core/mcprod \
  controller rollback \
  --run-dir /afs/cern.ch/user/c/chiw/condor/Full_MC_Production/generated/v06-dps2g-tps-launch-20261026/run-manual-20261026T081209Z \
  --expect-build v06_01-control-20261030-02
```

部署 journal 记录了切换前 launcher 字节和 mode、manifest 与 run.json 摘要、sync/activate
结果及有限的 post-cutover event 检查：[controller upgrade journal](../../../generated/v06-dps2g-tps-launch-20261026/run-manual-20261026T081209Z/logs/controller-upgrade-v06_01-control-20261030-02-20261030T094504Z.json)。
截至 09:45:53.927 UTC，controller log 中新增 259 条结构化事件，build/run 标识及 task、PID、operation 字段完整；样本中没有 traceback、fatal 或 state commit failure。观察到的 PRE exit 75 是 earlier-source admission 与 state lock defer；一个 cleanup exit 99 对应 DAG 中声明的 `PRE_SKIP CLEANUP_pool_2jpsi_g_000492 99`，并已同步提交 empty-cleanup 状态。该窗口是切换后的有限检查，不代表已验证所有后续进程。

独立 post-check 随后读取同一 controller log（inode `198808662`），从 byte offset `347465761` 至 `349897227`，覆盖 09:45:53.964–09:49:02.780 UTC 的 2,716 条事件，均标记 build `v06_01-control-20261030-02`。其中 2,690 次 admission-hint fast reject、26 次未用 hint；exit code 计数为 75×2,695、0×20、99×1。事件含 11 次 `replacement_synced`、10 次 `no_op`、2 次 `deferred`、2,693 次 `not_attempted`，以及 3 次 lock-busy；没有 hard-error 或 `visible_unsynced` 事件。

观察到的已提交 POST 包括 `complete_source SOURCE_000491`（09:45:59.299 UTC），以及 `accept_result` 的 `MINIAOD_*BLOCK000352`（09:46:45.150 UTC，receipt read count=1）、`PLAN_000493`（09:47:08.423 UTC）和 `PLAN_000494`（09:47:40.591 UTC）。该窗口内没有成功的 `complete_shard` POST。这些日志证明新控制器代码正在运行，单个有限窗口不能证明持续吞吐提升。

后续 coherent state read 通过新 staging `_check`：`accepted_count=5053` 与 producer index 的 accepted count 一致，索引包含 8,910 个 refs；138 个 allocations 中 134 个 complete；659 个 source complete，6 个 admitted；resident bytes 为 3,428,014,567 / usable 120,000,000,000，claims 为 10。首条 root-scoped JSON 队列结果超出工具输出限制；09:51:34.842–09:51:35.047 UTC 在同一 schedd 和 DAG root 上执行的投影属性查询返回 exit 0，共 40 个 ads：10 个 DAGMan 均 running，30 个 vanilla 中 29 个 running、1 个 idle、0 个 held。根 DAGMan `6702429.0` 为 Running，`DAG_Status=0`，1,720/3,522 nodes done，failed=0。查询只覆盖记录的 root DAG，没有队列修改。完整交接见 [controller upgrade journal](../../../generated/v06-dps2g-tps-launch-20261026/run-manual-20261026T081209Z/logs/controller-upgrade-v06_01-control-20261030-02-20261030T094504Z.json)。
