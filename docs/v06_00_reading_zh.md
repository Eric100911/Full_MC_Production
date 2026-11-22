# v06_00 运行方案：给读者的说明

本文是 [v06_00 campaign-shard 计划](v06_00_campaign_shards.md) 的中文阅读版，便于理解设计目的和需要守住的边界。实施、配置校验、现场门槛和操作步骤均以主计划为准；两份文档表述不一致时，以主计划为准。这里描述的是待验证方案，不代表 CERN 锁实验、真实 pilot、发布标签或生产部署已经完成。

## 这次改动要解决什么

现有新版流程要等前一波 worker 全部结束，下一波才开始。少数较慢的 MiniAOD 会让其他已经有输入的处理也停下来。v06_00 计划用数量有限的动态 campaign-shard（campaign 分片）子 DAG 替换整波完成屏障，让条件已满足的 campaign 和 shard 可以继续调度；同一个 campaign 的 shard 按确定顺序分配输入，但不必等前一个 shard 完成后才继续。

调度器仍负责依赖、重试和救援；项目代码负责冻结每个 shard 的具体输入，以及控制 CERN 暂存区容量。方案不增加常驻 admission 服务、Condor 状态镜像数据库或通用引用计数服务。

## 同一个 BLOCK 怎样复用

同一个 campaign 里的 BLOCK 不能重复分给不同 MIX，包括多个指向同一个 pool 的 source slot（source 位置）。不同 campaign 可以选择同一个 BLOCK；这表示输入事件被复用，不能把相关样本宣称为相互独立。冻结的 allocation manifest（分配清单）会记录真实输入引用、job ID、seed 和 EDM event ID，重试或救援时继续使用同一份分配。

每个 `(campaign, pool)` 有自己的消费流。默认按 pool 序号逐个取块，`stride=1, offset=0`。400:80 的示例中，DPS1 依次取 `0,1,2,...`，TPS 固定取 `0,5,10,...,395`。TPS 每次选择每五个位置中的第一个，不随机抽取。某个 source slot 按实际事件数预算取完整 BLOCK，达到预算后停下；同 campaign、同 pool 的后续 slot 从刚才的位置接着取。

这两个游标都用冻结 pool 序列里的**实际 BLOCK 序号**表示，而不是“已经选了多少块”。`allocation_cursor` 表示下一个可分配的 pool 序号；`safe_cursor` 表示连续完成且满足归档条件的安全边界。比如 DPS1 的安全位置是 80、TPS 是 40，那么 `[40,80)` 中 `40,45,50,55,60,65,70,75` 仍可能被 TPS 使用。任务已经拿到输入，不等于 worker 已完成。即使按序号判断某块可能已不再需要，也要确认消费者已完成、所需原始文件已归档，再由精确引用的 Reclaim（删除暂存文件并释放额度）任务删除；不能只根据游标删文件。

## 状态和容量怎样保护

pilot 先使用一个隔离的暂存容量和一份权威 `state.json`。不可变 allocation manifest 和 worker、archive、reclaim 完成凭据保存历史；报告文件只能从权威状态派生。所有 DAGMan 子流程使用同一个稳定的 `ledger.lock` 文件进行短暂互斥，再原子替换完整状态文件。锁文件自身不能删除或替换。

预留额度按最大输出字节记账，输出经过现有 sidecar（伴随元数据文件）和完成凭据验证后，才按实际文件大小入账。只有成功的 Reclaim 完成凭据才能释放额度。失败生产任务会保留预留额度，供 DAGMan 重试或救援。MiniAOD 存放在 IHEP；worker 本地 archive scratch（归档暂存区）也不是 CERN staging，因此两者不计入这份 CERN 容量账本。

同一 shard 的归档和回收要等该 shard 中所有 MiniAOD 都完成；如果只为部分 MIX 生产任务预留额度，可能导致半组输出无法归档或回收。因此，首次为一个已提交 shard 执行 MIX 的 PRE 时，必须在一次原子操作中为该 shard 的全部 MIX 生产任务预留额度；之后每个生产任务仍按各自结果单独结算。source 阶段预留的 headroom（预留余量）必须覆盖一个完整 shard 及其互补输入，避免只启动部分 MIX 后卡住。

对于 LHE、PLAN 和 MIX 这些会占用 staging 的输出，worker 在上传前检查本地数据和 sidecar 的总字节数不超过冻结的 `staging_claim_bytes`。沿用现有发布流程：`_common.publish()` 先在本地写 sidecar 并核对额度上限，再上传数据和 sidecar；结果接受流程会核对 producer result、run-owned 暂存对象清单和现有 sidecar 约定。PRE 只读取 AFS 上的本地状态和已验证凭据，不运行 `xrdfs`、容器或远端文件检查。

容量账本依赖 AFS 上的全文件 `flock` 互斥，这一点仍待现场证明。历史上 PRE 能读写 AFS，不能证明不同 schedd 之间互斥、解锁后的新内容可见，或持锁进程崩溃后能够恢复。G3 必须在至少两个真实 schedd 上做隔离 NOOP 验证；结果未通过前，不能启用真实生产准入。

## DAG 布局与短暂等待

根 DAG 启动前声明有限的 source、campaign-shard、pool-cleanup 和 finalizer 位置。每个 source 子 DAG 生成 LHE 并 PLAN 成 BLOCK；source 自己负责把原始 LHE 归档一次，不让 DPS1、TPS 等每个 campaign 各归档一遍。campaign-shard 的 PRE 根据已确认的 PLAN 库存冻结 manifest，再生成完整子 DAG。库存暂时不足时，PRE 返回 75 并稍后重试；不得等待上一个 shard 的 worker 全部完成。每个 pool-cleanup 位置由对应 source/shard 的完成边触发；PRE 有明确可回收文件就准备普通 Reclaim 作业，没有则立即以 `PRE_SKIP=99` 跳过该位置，不等待未来文件。

`PRE_SKIP=99` 只表示当前预声明位置没有任务可做，不能拿来表示“暂时没空间”。其他 PRE 错误必须失败并留下诊断。worker 成功时，结果 POST 校验 worker 返回码、结果格式、任务和输出、sidecar 及 run-owned 暂存对象；通过后写接受凭据并更新权威状态。campaign-shard 完成 POST 会确认子任务均已接受，再推进连续完成的 `safe_cursor`。上游 worker 失败时 POST 返回非零，不接受该任务结果，也不推进完成状态；保留已有 manifest、额度和接受凭据，由 DAGMan retry/AutoRescue 幂等恢复。

pool cleanup 由预先声明的有限位置触发，读取已提交的安全水位和完成凭据，只为已证明可回收的明确文件引用创建普通 Reclaim 作业。清理不需要新的生产额度，所以暂存区满时仍可释放空间。最终汇总在本地读取 `state.json`、完成凭据和各个预声明位置的最终状态，不访问 EOS。

## 计划参数与小型 pilot

配置使用顶层 `orchestration: "campaign_shards"`，并在 `limits` 中设置 `shard_nodes=10`、`target_window_nodes=20`：每个 campaign shard 最多十个处理节点，规划器按最多二十个节点的有限窗口决定接下来补哪些输入。目标进度是规划值，不代表 Condor 已完成节点数，也不是 BLOCK 消费游标。补充 source 时优先照顾缺少互补输入的 campaign；不会为了达到规划目标另行申请容量 Claim（额度预留）。

首个 site pilot（站点试运行）的计数夹具为：J/psi 800 个事件、gg 100 个事件，最多 50 个事件一个 BLOCK，因此分别形成 16 个和 2 个 BLOCK。DPS1、TPS 各运行 2 个处理节点，每个节点目标 20 个混合事件，merge 目标 40 个事件。现场运行前仍须用实际冻结 manifest 验证这些预算是否相容；夹具数字本身不是远端生产库存的证据。

site 配置包含 `site_validation.proof_path` 和正整数 `isolated_budget_bytes`。后者必须精确等于 `staging_bytes - reserve_bytes`。证明文件必须符合 `cern-afs-lock-proof-v1`：`schedds` 是至少两个不同 schedd 名称组成的字符串列表，`evidence` 是指向现存本地证据文件的路径列表，`checks` 中互斥、解锁后可见、崩溃恢复、PRE defer、PRE skip、动态 SubDAG 和 AutoRescue 七项都必须是布尔值 `true`。G3 由人工在明确授权后执行 NOOP 验证并写下记录；规划程序只读取和校验记录，不会代替现场测试。缺少证明或检查未通过时不能提交 site run；不得把本地测试结果当成 CERN 现场证明。

## 验证顺序

1. **G0：** 冻结 `6c62e05`，只读确认代码、运行目录和站点配置，记录与计划不同之处；保持现有运行和 runtime 不变。
2. **G1：** 离线验证步幅分配、跨 campaign 复用、同 campaign 不重复、事件预算、临时缺货原子回滚、短尾报告、稳定 ID/seed、清理边界与安全水位。
3. **G2：** 在本地隔离目录验证额度 Claim、发布与回收只结算一次、失败额度保留和状态文件恢复。本地文件系统通过不等于 AFS 通过。
4. **G3：** 获得明确授权后，在至少两个 CERN schedd 做隔离 NOOP/锁语义测试，并收集 proof。失败就暂停，不替换成未经批准的锁或后台服务。
5. **G4：** 用本地模拟 DAG 验证慢 MiniAOD 不挡住无关 shard、回收能解除 defer、retry/救援复用原任务身份，且没有全局 PLAN 或 shard 完成屏障。
6. **G5：** G0–G4 通过并获明确授权后，才准备独立 pilot 目录、冻结 runtime 和两 campaign 夹具。每个 `v06_00_preXX` 只能标记对应的干净、已审阅、过关提交；所有验收完成后才考虑 `v06_00`。

主计划规定各不可变标签的条件顺序：`v06_00_pre01` 为分配和单元测试，`pre02` 为容量账本和经验证的 AFS 锁，`pre03` 为动态 DAG 与模拟验证，`pre04` 为需求驱动补充和获批 pilot；后续 `pre05+` 只收录有新证据支持的窄修复，最终 `v06_00` 需要完整验收。某个标签名并不表示相应关卡已经通过；不可移动或覆盖已有标签。

当前离线 stream 测试命令为 `TMPDIR=/tmp/chiw python3 -m unittest tests.test_core_streams`，用于检查物理序号步幅、`[40,80)` 保留集合、配置校验、短尾和临时缺货回滚；应以每次实现后的实际测试结果为准。G3 CERN 锁验证和 G5 pilot 仍待完成，单元测试不能证明现场锁语义或 pilot 准备就绪。
