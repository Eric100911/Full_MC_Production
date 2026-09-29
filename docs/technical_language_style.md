# 技术说明语言规范

本规范适用于中文技术解释、代码评审报告、设计与计划文档，以及面向
repository-aware coding agent 的讨论。目标是让读者能够理解代码、原因和
风险，而不必先掌握一组未解释的软件工程术语。

## 1. 中文负责解释，英文负责对齐代码

中文应承担主要解释工作。代码中的函数名、字段名、DAG 指令和协议名称可以
保留英文，以便读者直接回到源码，但周围的推理应使用自然中文。

不推荐：

```text
terminal reclaim fold 完成后 completion short-circuit 阻止 re-entry
重新 derive leftovers。
```

推荐：

```text
最后一轮清理任务的结果已经正式写入状态后，整个 run 已进入完成态。
此时如果因为 rescue 或人工操作再次调用 `advance_wave()`，函数应该立即
返回，而不能重新计算还有哪些暂存文件待删除，否则已经完成的 run 可能
被错误地重新打开。
```

可以保留这些名称：

```text
advance_wave()
state["status"]
PRE_SKIP
merge_group_counts
```

但不要让整段说明只剩下英文术语和缩写。

## 2. 第一次出现时解释术语

第一次使用不常见的英文词时，要说明它在本仓库中的含义，而不是只给字典
翻译。例如：

```text
hydrate（从 producer sidecar 中补全 artifact reference 的实际元数据）
```

之后优先写“从 sidecar 补全 reference 元数据”，不要反复堆叠 `hydrate`。

同样要解释那些日常英语在软件工程中有特殊含义的词，例如 `seam`、
`receipt`、`cursor`、`terminal`、`guard`、`contract`、`commit`、`snapshot`、
`materialize`、`re-entry`、`stale`、`atomic`、`monotonic`、`regression`、
`smoke`、`live` 和 `slot`。

## 3. 本仓库术语表

### wave（波次）

根 DAG 的一次推进单位，概念上包括：

```text
PRE controller transition
+
该次转换准备的 SubDAG/worker
```

它不只是“同时提交的一批作业”。

### seam / wave seam（波次衔接边界）

已完成 worker 波次和下一波 worker 之间的控制边界。submit-host 上的 PRE
脚本在这里读取本地结果、更新状态并准备下一张 DAG。说明文档中优先使用
“波次衔接边界”。

### fold（归并上一波结果）

验证已完成波次的结果，并把它正式写入可靠的工作流状态。

### reclaim（删除暂存文件并回收暂存空间）

删除已经不再需要的暂存产物，使这些产物占用的 staging 容量重新可用。

### hydrate（从 sidecar 补全产物引用的实际元数据）

读取 producer sidecar（随主产物一起发布的小型元数据文件），为不完整的
artifact reference 补上 `size_bytes`、`events`、`runtime_release` 和
`count_source` 等实际信息。它不下载产物。

### materialize（把远端产物准备成本地可用文件）

获取或准备真正的产物文件，使消费者可以在本地读取它。下载由
`materialize_input()` 完成，并在使用点检查本地文件大小。

### artifact（数据产物）

工作流产生的数据产品，例如 LHE、LHE block、HepMC、MiniAOD 和 archive。
中文优先使用“数据产物”，不要直译成“工件”。

### artifact reference / ref（产物引用）

描述产物的元数据记录，例如 `artifact_id`、`url`、`kind`、`size_bytes` 和
`events`。它不是产物文件本身。

### sidecar（伴随元数据文件）

随主产物一起发布的小型元数据文件。第一次出现时应说明这个“伴随文件”
保存什么信息，不要假定读者知道这个比喻。

### ledger（不可变历史账本/记录）

不可变或 create-or-compare 的历史记录存储。已写入的记录不能被新内容静默
覆盖。

### receipt（完成凭据）

不可变记录，证明某个工作流操作已经以指定内容完成。

### cursor（消费进度位置）

有序资源池中的消费位置，表示已经推进到哪里。它不是鼠标光标。

### pending（待提交）

某个波次已经准备或执行，但结果还没有正式归并到权威状态。

### commit marker（提交标记）

决定哪些已经写入的不可变记录正式被接受的状态值。这里的 `commit` 不是
Git commit。

### durable（已经可靠落盘、可用于恢复）

状态已经持久化，即使 controller 进程失败，也不会丢失恢复所需的信息。

### terminal state（终止态）

不应再转回普通运行状态的工作流状态。本工作流中
`state["status"] == "complete"` 是终止态。

### short-circuit（短路返回）

决定性条件已经满足时立即返回，不再继续普通的状态推导。例如完成态应立刻
重建 `.complete` 并返回。

### guard（保护条件）

在可能不安全的状态转换或破坏性操作前检查的保护条件。

### fail closed（无法证明安全就停止）

如果验证不能证明安全或正确，就停止推进，而不是默许工作流继续。

### invariant（不变量）

每次合法状态转换都必须保持为真的逻辑关系。例如：

```text
pool cursor 已越过一个 staged block
    ⇒
该 block 不再计入驻留 staging 使用量
```

### contract（接口/行为契约）

组件之间明确规定的输入、输出和行为要求。它不是法律合同。

### producer / consumer（生产者 / 消费者）

创建产物的 worker 是生产者，之后读取该产物的 worker 是消费者。

### source of truth（权威状态来源）

可以重建派生文件的权威表示。优先写“权威状态来源”。

### derived snapshot（派生快照）

从权威状态重新构造的便利文件。它本身不是权威来源。

在本仓库中：

```text
merge_groups.json = 派生的 operator snapshot
handoff.json      = 正式的下游接口
```

### atomic write（原子写入）

读者只能看到旧的完整文件或新的完整文件，看不到半写入文件。

### monotonic state/snapshot（单调状态/快照）

逻辑进度不能倒退。必须明确说明：`atomic` 不等于 `monotonic`；原子替换
只能避免半文件，不能阻止较旧的逻辑状态覆盖较新的状态。

### regression test（回归测试）

专门防止已经观察到的 bug 再次出现的测试。

### differential test（差分测试）

让两个实现或两个版本处理同一个逻辑输入，再比较输出。

### smoke test（冒烟测试）

小而便宜的测试，只说明关键路径基本可用，不是完整验证。

### live test（真实系统测试）

使用真实外部运行环境的测试，例如启动真实 DAGMan，而不是只运行 Python
模拟。

### slot（预声明的根 DAG 位置）

根 DAG 中预先声明的位置，例如 `WAVE_000003`。如果 run 已完成，这个位置
可以不包含真实 worker 工作。

### PRE_SKIP

PRE 脚本仍然会运行。如果它返回配置的 PRE_SKIP 退出码，DAGMan 会跳过该
节点的剩余部分，并把节点视为成功。它不是“跳过 PRE 脚本”。

### re-entry（再次进入）

在早先执行过之后再次进入同一个生命周期函数，例如 rescue/recovery 或
operator 命令再次调用 `advance_wave()`。

### wedged（卡死）

工作流既没有干净失败，也无法继续前进，进入无法推进的卡死状态。不要只
写英文 `wedged` 而不解释。

### stale snapshot（过期快照）

表示较旧权威状态的派生快照。优先写“过期快照”。

## 4. 必须区分的歧义

### “local” 的两个含义

`local-only PRE seam` 表示 PRE 只使用 submit-visible 的本地控制记录，不
访问 EOS。

`local cern_staging` 表示产物存储本身是本地/POSIX 路径。

这两个概念完全不同；前者不是对后者的支持承诺。

### “complete” 的两个层级

```text
artifact sidecar: complete: true
workflow state:    state["status"] == "complete"
```

前者表示一个产物发布完成，后者表示整个 production run 完成。

### “validation” 的三个层级

不要用一句模糊的“已验证”覆盖不同信任边界：

1. 结构验证：ID、URL、成员关系、schema 和数量；
2. 生产者发布验证：sidecar 身份、`complete` 标记和实际远端大小；
3. 消费者文件验证：下载/本地文件确实符合声明的大小和事件数。

## 5. 未来评审文档的写法

每个重要结论优先按以下顺序说明：

```text
代码做了什么
→ 为什么重要
→ 防止了什么失败
```

不推荐：

```text
The guard makes terminal reclaim fail closed.
```

推荐：

```text
`terminal_reclaim_refs()` 只有在 archive 记录已经低于 committed state
marker 时，才把原始 LHE 视为安全保留。因此 controller 崩溃前刚写入、但
还没有完成状态提交的 archive receipt 会被忽略，直到对应状态转换正式
提交。这避免终端清理仅凭一份未提交的 ledger 文件就删除原始产物。
```

技术精确性应增加解释，而不是取代解释。
