# v06_01 staging 控制层方案调查与收益估算

日期：2026-10-30。范围：设计评审、源码核对、已有测试、合成计时及故障注入；没有部署优化、修改现有实现或冻结 runtime，也没有查询实时队列、访问远端产物或改变生产任务。

主要代码基线是本地 `.worktrees/v06_00-staging-scheduler`，分支 `feat/v06_00-staging-scheduler`，HEAD `fdce0e9b9fd1f4b866174800d7101801572df956`，**包括已有未提交改动**。其中 staging 的 no-op 补丁不在远端分支已提交源码中。主工作区分支和 HEAD 是另一套基线，不能混用。下文源码行号均对应本次调查时的本地文件。

前次证据是主工作区 `docs/v06_staging_lock_findings_20261028.md` 与 `docs/v06_staging_lock_verification_20261028.md`。后者纠正了前者的部分统计和因果推断，不能把两份报告都当作对原结论的支持。本次没有重新证明任何历史进程加载了哪个 runtime，也没有重建缺失的历史采样原始输出。

## 1. 判断及实施顺序

保留独立 SOURCE/SHARD SubDAG、确定性输入分配和 run 内统一容量事务，先优化状态控制层，是有代码依据的方向。当前没有证据要求重新引入 WAVE/GATE、增加常驻调度服务或先迁移数据库。

第一轮建议包含：结构化计时、逐分支的纯拒绝退出、跨文件恢复缺陷修复及回归测试。producer 定位索引适合单独提交、单独计时评估。准入提示先覆盖顺序等待，再考虑库存条件。空间回收与数据库迁移继续由实测决定。

但“现有所有跨文件中断都已可恢复”不能作为前提：本次已复现 SOURCE/SHARD manifest 的重试冲突，见第 5 节。将昂贵的文件生成移出锁外之前，应先补齐这项保证。

## 2. 保留的机制与边界

- `planning/shards.py:103` 起分别声明 SOURCE 和 SHARD，没有前一 shard 完成才启动后一 shard 的依赖。普通 cleanup 依赖对应 source/shard；终端 cleanup 与 FINALIZE 保留收尾依赖。独立推进不意味着删除这些必要的归档和最终清理依赖。
- `application/shards.py:432` 起按 `assigned_progress` 分配完整输入范围，冻结 manifest 后推进 `allocation_cursor`；计算完成顺序独立。
- SOURCE 一次预留 bundle；`admit_producer()` 通过 shard manifest 的完整 MIX claims 进行整体准入。不能只预留单个 MIX 而仍让整个 shard 等待统一归档。
- `staging.py:50` 起验证 `resident_bytes + sum(claims) <= usable_bytes`，同时检查对象身份、驻留字节及 claim/ceiling 对应关系。缩短事务不能取消这些正确性约束。
- `complete_shard()` 只按连续完成前缀推进 `safe_cursor`；实际删除确认后 `settle_reclaim()` 才释放容量。
- ledger 仍是 run-scoped。多个独立 run 共用 EOS 配额时，本机制不提供跨 run 的总预算保证；静态分区或更高层配额属于另一项设计。

## 3. 无效 PRE 的局部优化

### 3.1 逐分支退出，不能按退出码跳过提交

当前 `transaction()`（`staging.py:143`）使用非阻塞 `flock`，成功取得锁后读取完整 JSON、解析、入口检查、执行事务体、退出检查、序列化，再比较是否需要写盘。普通 `return` 不会跳过上下文管理器的退出部分。

| 分支 | 本次调用能否按纯拒绝退出 | 原因 |
|---|---|---|
| SOURCE 等待更早同 pool source | 可以 | 尚未调用 `_deficits()`，未修改 state |
| SHARD 等待更早 allocation | 可以 | 尚未分配或冻结 manifest |
| SHARD 等待完整输入范围 | 可以 | `_inventory()` 读取 receipts，未推进游标 |
| SOURCE_ARCHIVE 等待 PLAN | 可以 | 未修改 state，也未写任务记录 |
| `reserve_bundle()` 确定容量不足 | 函数本身可以；调用方需审计 | SOURCE 此前可能已修改规划目标 |
| SOURCE soft window/headroom 拒绝 | 不能直接转换 | `_deficits()` 经 `_targets()` 写入 `target_progress` |
| 首次 SOURCE 跳过或首次空 cleanup | 必须保留提交 | 返回 99 同时写 skipped marker |
| 重复 accepted POST/已记录 skip | 可独立研究只读路径 | 不能影响事务外的重建文件操作 |

建议让专门的 `Deferred` 异常穿出 `transaction()`，在入口处精确转为退出码 75。第一步无需一次性推广通用三态事务接口。若引入 `READ_ONLY/ABORT`，必须明确：它们只约束 state 的提交，并不撤销事务体此前写出的 manifest/receipt。已产生的跨文件副作用需要单独恢复协议。

仍应保留已有 no-op 字节比较补丁。真实修改后又还原、重复 POST，以及未显式标成只读的路径仍然需要它。

### 3.2 提示必须允许丢失，并保证最终重新检查

优先提示 `source_next[pool]` 和 `shard_next[campaign]`。它们只允许提前拒绝；可能满足条件时仍进入原权威事务。提示缺失、格式错误、run 身份不符或超过强制复核期限，都回到权威检查，不授予额度。

还需补充几项设计约束：

1. 提示和等待记录带 run 身份、格式版本及状态代数，避免恢复或复用路径后误用旧记录。不能只凭 mtime/size 授权。
2. 复核期限属于这个等待条件或节点。不能因为无关 campaign 持续刷新全局提示时间，就永久延长某个过期条件的等待。
3. 提示由已提交的 state 派生。锁外发布可能乱序，较旧提示可覆盖较新提示；既然提示不权威，允许这种情况，但复核机制必须仍能推进。更新提示失败不能撤销已成功的 ledger 提交。
4. 库存等待可能涉及多个 pool、连续 PLAN 前缀和耗尽标记；单个 `plan_generation[pool]` 必须覆盖完整依赖集合。先做顺序等待，避免过早把所有等待原因都塞进同一缓存。
5. 每个 PRE 仍有进程启动和提示读取成本。若维护逐节点等待文件，也要计入新增的 AFS 元数据操作，不能把它们假定成零。

当前 `SCRIPT DEFER 75 60` 至少等待 60 秒后重试；提示不减少 DAGMan 启动脚本的次数。官方文档还明确：等待 DEFER 时不占 maxpre/maxpost 并发名额。因此 MaxPre 既不是每秒请求率，也不能直接当整个嵌套 DAG 的统一准入窗口。[HTCondor scripts](https://htcondor.readthedocs.io/en/latest/automated-workflows/dagman-introduction.html#scripts)

## 4. 有效 POST：先定位 receipt，再考虑更大的状态改造

`_find_producer()`（`shards.py:523`）扫描 accepted 中 LHE/PLAN/MIX 的完成凭据，并搜索 `outputs + originals`。MiniAOD 验证和 archive member 验证调用它；archive 多个成员还可能多次扫描。

权威 `artifact_id -> task_id` 索引可以随 accepted marker 在同一个 state 提交中更新，使每次查找只读取目标 receipt。它减少 receipt 文件访问，**不会消除整个 state 的读取、检查和序列化**；索引也会略增 JSON 大小。重建成本应单独计算，不能每个 PRE/POST 都重建。

索引有一个容易遗漏的兼容条件：生成的 LHE 既出现在 LHE receipt 的 outputs，又出现在 PLAN receipt 的 originals。`accept_result()`（574–584 行）主动验证两者一致。因此不能简单规定同一 artifact_id 只能出现在一个 task 中。应定义稳定的原始 producer，验证重复引用的一致性，并在旧状态迁移时重建后校验。不能把“indexed” marker 当成索引完整性的证明。

`_inventory()` 必须维持按 source 顺序的连续 accepted PLAN 前缀；后完成的 PLAN 不能插进已冻结 block 的前方。producer 定位索引与库存索引是两项改造，建议分别评估。

重复 `_check()` 的删除也应单独进行。`reserve_bundle()` 拒绝合计三次检查，成功合计四次检查；直接删除 helper 的检查可能改变 helper 被单独调用时的契约。先明确哪些 helper 只允许在已验证事务内执行，再精简内部检查。暂不将完整检查降为周期审计。

## 5. 已复现的跨文件恢复问题

| 注入位置 | 实际结果 | 解释与边界 |
|---|---|---|
| SOURCE manifest 写成，state 替换前抛错，然后立即重试 | `RuntimeError: immutable record conflict` | 不需要其他任务推进就会触发 |
| SHARD manifest 写成，state 替换前抛错，然后立即重试 | 相同错误 | allocation、tasks、claims 相同，dependencies 比较失败 |
| 上一 SHARD 场景后，其他 source 先推进，再重试 | 相同错误 | 同时提示未来还需检查重算与已冻结内容的一致性 |
| accepted receipt 写成，state 替换前抛错，再次 POST | 本次 fixture 中成功接受 | receipt 重放成功，不能由此推断所有 manifest 同样安全 |
| `os.replace` 成功，目录 fsync 抛错 | 调用报错，但新 state 可见；重试读取新 state | 进程可见性不证明断电后持久性 |

直接原因：`_source_tasks()`、`_processing_tasks()` 使用 tuple 表示依赖边；JSON 写出再读回后是 list。`io.write_immutable_json()`（10–17 行）直接将解码结果与新 payload 比较，因此将语义相同的依赖误判成冲突。正常“state 已提交后重试”绕过 `_freeze()`，所以已有分配重试测试没有发现它。

建议在后续实现中统一不可变记录的 JSON 表示并增加上述回归测试。修掉 tuple/list 误判以后，还须测试：尚未提交的 cleanup manifest 已存在，但其他事务让可回收集合扩大时，如何重用冻结集合和补建其 pending claim。不能只靠重新计算恰好得到同一集合。此 cleanup 场景目前是源码指出的风险，本轮未完成该变化场景的故障复现。

`io.atomic_json()` 目前没有 state writer 的 file/directory fsync。原子替换保证不读到半文件，不等于故障后一定保留该目录项。后续应明确不可变记录在状态指针提交前的持久化顺序；若把生成移到锁外，还需定义单写者/内容冲突检测和过期计算的处置。现有锁外“存在则比较，不存在则 replace”本身不是完整的并发创建协议。

## 6. 收益估算：分清已有收益、局部计时与生产预测

### 6.1 已有补丁的收益不能再算一次

前次报告的写率从 3924/h 降到 48/h，对应 **98.78% 的观测写率下降**。采样可能漏掉替换，窗口也不同，因此不是精确的 no-op 比例或因果实验。当前本地已包含该补丁；v06_01 的新收益应从补丁后的版本算起。

历史 19.8 次调用/s、2.73 次获锁/s、86.2% busy、315 ms 平均持锁，没有保存足以重建窗口的原始输出。本轮只把它们用于条件估算。

报告还列出 5131 次 busy、748 次等待更早 source、72 次等待更早 allocation。后两者合计 820，接近 300 秒乘 2.73/s 的获锁次数；总计 5951 又与 19.8×300=5940 略有差异。这提示该记录窗口的获锁事务可能主要在做纯顺序拒绝，但不能据此知道 busy 的节点原本会因什么拒绝，也不能当成整个生产周期的精确分布。

### 6.2 本次合成计时

环境：本地 `/tmp/chiw/`，单进程、暖缓存、9000 个对象、序列化 state 为 **12,951,296 B**。这接近历史约 13 MB 的文件规模，但对象内容、receipt 大小、AFS 缓存和机器负载不同。以下七次样本用于估计计算工作量，不是生产服务等级承诺。

| 路径 | `_check` / 序列化次数 | 中位耗时 | 范围 |
|---|---|---:|---:|
| 普通事务内无变更返回 75 | 2 / 1 | 149.2 ms | 123.7–179.5 ms |
| 专用异常穿出事务，不执行退出部分 | 1 / 0 | 75.0 ms | 66.6–106.0 ms |
| `reserve_bundle` 容量拒绝，另一组合实验 | 3 / 1 | 153.0 ms | 141.1–188.6 ms |
| 已知无变更的容量判断后异常退出，另一组合实验 | 1 / 0 | 65.5 ms | 63.9–81.5 ms |

第一组隔离了退出检查和序列化，**本地中位耗时减少约 49.7%，单次路径约快 1.99 倍**。第二组还省掉 helper 的额外入口检查，约减少 57.2%、快 2.34 倍，不能全部归功于纯 Deferred 改动。两组实验没有统计显著性或真实 AFS 的置信区间。

据此，纯拒绝事务的优化可以按“约一半计算耗时”的量级规划试验；尚不能承诺线上同样减半。全量读取、解析和入口验证仍然存在。若将未独立核验的历史 315 ms 机械地套用 1/2 系数，会得到约 158 ms；这只是情景换算，不是本次测得的线上值。

### 6.3 轻量提示的条件收益

设所有 PRE 尝试中可由所选提示判断的无效请求占比为 `p`，提示拦截这些请求的比例为 `h`。忽略提示自身成本时，完整事务尝试减少比例为 `p*h`。不能把历史 busy 比例直接当 `p`，因为 busy 尚未执行权威业务判断。

| 假定 `p` | 假定 `h` | 完整事务尝试减少 | 若原需求为 19.8 次/s，剩余需求 |
|---:|---:|---:|---:|
| 50% | 80% | 40% | 11.88 次/s |
| 80% | 90% | 72% | 5.54 次/s |
| 95% | 95% | 90.25% | 1.93 次/s |

这些是**待验证情景**，不是当前分布的测量值。历史记录与“顺序等待占比高”的假说一致，值得测试中、高收益情景，但不足以选定其中一个。

若暂用历史 315 ms 的串行服务时间，理论服务率约 3.17/s，19.8/s 的尝试需求需减少约 **84%** 才低于它。因此仅减少 40% 或 72% 的尝试，仍可能存在争锁；与纯拒绝耗时优化结合更有意义。这个简化计算未模拟不同事务成本、非阻塞抢锁、60 秒重试及公平性，不能当作实际排队模型。成功过滤的请求不再全量读 state，但逻辑读取减少不等于 AFS 后端流量按同比例减少。

### 6.4 producer 索引的收益

合成 receipts：1140 MIX + 352 PLAN，合计 1492。把目标放在最后一个 PLAN，当前 `_find_producer()` 一次读取 **1492 个文件**，本地耗时 129.7 ms。已加载的 `artifact_id -> task_id` 指针加一次 `_receipt()`，七次都读取 **1 个文件**，中位 0.119 ms（0.106–1.068 ms；目标 receipt 只有 64 B）。

这证明最差位置的查找文件数可减少 **99.93%**，不是证明整个 POST 快约千倍。实际查询目标分布、PLAN receipt 大小和 AFS 往返不同；索引还要加载、更新和恢复。历史快照已知 MIX+PLAN 就有 1492 个 producer，但不能假定所有实际查询都会扫到最后，也未计未知数量的 LHE。

只作均匀位置模型时，1492 条记录平均约读 746.5 个，降到 1 个即约减少 99.87% 的 lookup 文件访问。更实际的预期是：**数百至上千次 receipt 访问降到一次，局部查询通常有很大优化空间；POST 总耗时收益取决于查询占比。** 若某 POST 原有 80% 时间花在该查询、查询快 100 倍，则总耗时约剩 20.8%、快 4.81 倍；若查询只占 10%，总耗时约剩 90.1%、只快 1.11 倍。这两个比例均为示例假定。

不能把“全索引构建后内存查询零文件读取”的实验当成当前每次启动独立进程的实现；本方案仍会读取目标 receipt。重建索引要一次性扫描历史，之后与每次 accepted 提交增量维护。

### 6.5 空间周转和最终产出

10 月 28 日 13:24:43 UTC 快照：120 GB 预算中 resident+claims 为 10.880 GB，约占 **9.07%**，账面余量约 **90.93%**。这只说明该时点预算不紧张；不是整个运行期从未受容量限制。

同一快照中未完成 DPS2_G allocations 已接受 50 个 MIX、5 个 MiniAOD；未完成 TPS allocation 已接受 10 个 MIX、9 个 MiniAOD。这支持继续追踪下游 MiniAOD 的结果接收、排队与执行，不能直接判定是 CPU、队列还是 POST 慢。

因此，回收优化本轮**不预先计入吞吐收益**。没有时间序列就无法估计被连续前缀阻塞的 GB·小时；也不能从 109.12 GB 账面空闲推导扩大容量会加速。对生产完工时间，当前证据不能给出可信百分比：若 HTCondor/CMSSW 是关键路径，控制层优化可能几乎不改变产出；若关键 POST 长时间受阻，收益可能显著。

用于规划的串行时间示例：若控制等待占关键路径 10%，将其加速 2–5 倍，整体仅快约 5.3%–8.7%；若占 30%，整体约快 17.6%–31.6%。这里的 10%/30% 没有被测量，不作为生产提速预测；资源释放引起的并行度变化也不在该简化模型内。

## 7. 回收策略需额外核对的实施条件

现有 `_cleanup_refs()`（`shards.py:673`）同时要求：source archive 已接受、所有相关 stream 不再可能需要 block、对象驻留且未被 pending cleanup 占用。新稀疏消费索引必须覆盖所有共享 campaign、重复同 pool 的 slots、stride/offset、未来 allocation 和已冻结但未完成的消费者，不能只看某一 shard 已完成。

还需设计新的清理触发机制：每个预声明 cleanup slot 只触发一次；若当时无对象，记录 skipped 后不会因后来消费者完成而自动重开。若未来允许在 MIX 接受后提前释放 LHE，原来仅依赖 source/shard 完成的 cleanup 触发点不一定能及时兑现收益。可以保留独立 DAG，但不能只改 `reclaimable()` 而不考虑何时有 worker 执行删除。

归档分组也需区分两种实验：降低 `shard_nodes` 同时改变 allocation 粒度、根节点/PRE 数与 archive 粒度；它不是“只缩小同一 shard 内归档组”的等价实验。可以先做参数敏感性测试，但结果必须记录这些共同变化。若将完整预算也缩到组级，应逐组证明可以完成、归档、回收，而不会互相占用完成所需容量。

## 8. 观测和验收

结构化事件记录 UTC 时间、run、DAG 层级、node/task、phase、PID、固定发布版本、退出原因、是否提交及账目。用单调时钟计时，分开 read、parse、检查、事务体、receipt 访问、serialize、文件同步与目录同步。`flock` 是非阻塞的：失败调用没有长时间在锁内等待，应单独统计 busy 次数，以及跨多次 DEFER 的首次尝试到最终提交延迟。

现有 `_entry()` 捕获整个入口的 `BlockingIOError`，并统一标成 `local state lock busy`，所以后续应只在获锁处标记真正锁冲突，其他 I/O 异常保留真实类别。日志本身尽量锁外写，不做每次调用全源码哈希，也不记录完整账本。

相同工作负载、相同状态规模下比较四个版本：当前 no-op 基线、纯拒绝优化、增加顺序提示、再增加 producer 索引。报告每原因的尝试/获锁/提示拒绝次数、锁内时间分位数、POST 首次尝试到提交的尾部延迟、有效 shard 启动延迟、读取次数和提示维护成本。工作负载需包含顺序等待为主、容量紧张、集中 POST，以及早期 shard 变慢的场景；合成跑分之后才安排另外授权的真实小规模试验。

每阶段必须保持容量不超额、确定且不重复的分配、重试不重复授予/释放、跨文件故障可恢复，以及没有新增 worker 完成屏障。负向提示只保证安全，不自动保证无饥饿；需专门验证丢失提示、乱序发布、持续无关更新和强制复核。

SQLite WAL 的网络文件系统限制与方案一致：官方要求相关进程在同一主机共享内存，不支持通过网络文件系统直接多机使用。若以后采用单实例 API，数据库应放在服务主机合适的持久存储；AFS 上既有 flock 核验不能替它证明 WAL 语义。[SQLite WAL](https://www.sqlite.org/wal.html)

## 9. 本次实际验证与待处理项

在 v06 worktree 运行，使用 `PYTHONDONTWRITEBYTECODE=1`，测试临时目录放在 `/tmp/chiw/v06_01_investigation/transaction_evidence/`：

| 命令/实验 | 结果 |
|---|---|
| `python3 -m unittest tests.test_core_staging` | 21 通过 |
| `python3 -m unittest tests.test_core_staging tests.test_core_shards tests.test_core_shard_simulation` | 41 项，40 通过、1 失败 |
| `PYTHONPATH=src python3 -m unittest tests.test_core_shard_interfaces` | 8 通过；首次未设置路径时导入失败，已纠正命令 |
| `python3 -m unittest tests.test_dag_progress` | 18 项，17 通过、1 失败 |
| 合成 state 计时与 receipt 定位计时 | 见第 6 节；没有修改生产实现 |
| SOURCE/SHARD/receipt/replace 故障注入 | 见第 5 节；SOURCE/SHARD 重试缺陷已复现 |

Shard 测试的失败为 `test_root_is_finite_independent_and_explicitly_configured`：fixture 配置 3，但断言期待 `MAXJOBS lhe_planning 2`；前次调查已在去掉 no-op 补丁的基线上复现，本轮不把它归因于新优化。

Progress 测试的失败为 `test_dashboard_counts_skips_as_successful_terminal_nodes`：期望 2/2 完成（1 done、1 skipped），实际 1/2。测试末尾 `ULOG_PRESKIP` 行缺换行符；当前增量缓存仅扫描完整行，所以保留 waiting，直到换行到达。这是当前代码/fixture 的行为差异，不能报告全套已通过。

当前工具已依据节点的 `PRE_SKIP` 声明识别匹配退出码，并在完整 `ULOG_PRESKIP` 事件后显示 skipped。不能再笼统地说“所有 cleanup 99 都按失败处理”，也不能直接将所有 exit 99 视为成功。现有测试没有使用 CLEANUP 节点；建议后续补充真实 cleanup 行和缓存路径覆盖，明确终止但末行无换行日志的处理契约。

没有实施代码改动，因此没有运行完整仓库回归或生成新生产 DAG。本次唯一新增仓库文件是本报告；原有 dirty 文件保留。

协调记录：主代理 `/root` 负责范围、解释和整合；`local_evidence`、`transaction_evidence`、`recovery_evidence` 均显式使用 GPT-6-Luna 完成机械读取、测试或合成实验，无递归委派。执行证据目录为 `/tmp/chiw/v06_01_investigation/`（临时目录可能清理），恢复复现脚本见 `recovery_evidence/repro.py`。持久结论及数值保存在本报告。

下一轮待做：在独立改动中补全恢复协议、实现并测量纯拒绝退出、确定提示强制复核协议、评估 producer 索引；尚未授权或执行部署、队列变更或生产 runtime 热替换。
