# mcprod 波次 DAG 只读监控实施计划

## 已确认的事实与范围

- 目标是 production-core worktree。它已有多处未提交修改，实施时保留这些修改，并对已改动的 docs/testing.md 只作局部补充。
- 复现 run 的 submission.json 指定 current_attempt=2，即 bigbird26.cern.ch / 12758251；attempt 1 是 12740955。last_status 仍指向旧 attempt，不能用它选择监控对象。
- 当前根 DAG 已退出队列。持久日志显示根 DAG 5/5 成功：两个 WAVE_* 子 DAG 真正运行，三个 WAVE_* 经 PRE_SKIP 99 成功跳过。state.json 为 complete，.complete 存在。
- 本任务只实现一次性只读监控。入口不调用会写 submission.json 的 mcprod run status，也不提交、恢复、释放或移除 Condor 作业；不修改 src/full_mc/cli.py。
- 另一个 worktree 正在修改未来 IHEP batch worker 的 command log 落盘路径。该修复尚未进入当前 batch 的固定 runtime，本计划不改动那两个源文件，也不把新路径当作当前 batch 的日志证据。此前已停止的 IHEP monitoring 保持停止。

## 1. 修正共享 DAG 解析与结构视图

在 tools/dag_progress.py 的 PlannedNode 上保存 PRE_SKIP 码，解析 DAG 中的 PRE_SKIP <node> <code>。按日志顺序更新节点状态：匹配该节点配置码的 PRE 失败行属于过渡事件，不记失败；只有后续 ULOG_PRESKIP 才记为 skipped。未配置或不匹配的退出码仍记为 failed。内部完成记录可用 PreSkipped=True 区分 skipped 与普通 done。

~~~python
PRE_FAILURE = re.compile(
    r"\bPRE Script of Node (?P<name>\S+) failed with status (?P<code>\d+)"
)
PRESKIP_EVENT = re.compile(
    r"\bEvent: ULOG_PRESKIP for HTCondor Node (?P<name>\S+)\b"
)
if match := PRE_FAILURE.search(line):
    node = planned.get(match["name"])
    if node and int(match["code"]) == node.pre_skip_code:
        continue
    record_terminal(match["name"], "failed")
elif match := PRESKIP_EVENT.search(line):
    states[match["name"]] = {
        "DAGNodeName": match["name"], "JobStatus": 4,
        "ExitCode": 0, "PreSkipped": True,
    }
~~~

所有进度汇总以 done + skipped 计算成功终态完成数，同时单列 skipped。修改 dag_snapshot.py 的汇总与文本/JSON 输出，使显式 --dag-file 的旧入口也正确显示 12/12 完成、其中三个跳过。结构校验接受名称为 WAVE_* 的外部 SubDAG；折叠视图增加明确的 Wave/SubDAG 汇总及状态，避免只有波次节点时显示空结构。保留旧 DAG 的原有名称和阶段归类。

## 2. 新增可复用的 mcprod 入口

新增 tools/mcprod_snapshot.py，接口为：

~~~bash
python3 tools/mcprod_snapshot.py --run <run-dir> [--schedd <host>] [--cluster <id>] [--json]
~~~

从显式 run 目录读取 run.dag、run.json、submission.json、state.json。根据 current_attempt 查找唯一且号码匹配的 attempt，从该 attempt 读取 schedd 与 root_cluster_id；校验 submission.json 顶层副本、run_dir、dag 路径，以及用户显式参数。不读 last_status 作为当前状态，也不扫描最近提交。缺少或冲突的当前 attempt 身份时报错，不退回旧 attempt。

~~~python
number = submission["current_attempt"]
matches = [a for a in submission["attempts"] if a.get("number") == number]
if len(matches) != 1:
    raise ValueError("current attempt is ambiguous")
attempt = matches[0]
schedd, cluster = attempt["scheduler"], attempt["root_cluster_id"]
if (submission.get("scheduler"), submission.get("root_cluster_id")) != (schedd, cluster):
    raise ValueError("submission identity disagrees with current attempt")
~~~

队列查询只针对该 schedd/root。根 DAG 已离队或查询失败时，仍从匹配当前 cluster 的 run.dag.dagman.out、run.dag.dagman.log 和 run.dag.metrics 读取持久结果。根日志可能追加多个 attempt：按 DAGMan 的 STARTING UP cluster 标记切分，只取末尾属于当前 cluster 的连续运行段；从该段的 WAVE_* 提交事件取得各子 DAG cluster，再在对应 steps/*.dag.dagman.out 中只取匹配子 cluster 的运行段。找不到匹配段时标记证据缺失，不借用旧日志。

~~~python
STARTUP = re.compile(
    r"condor_scheduniv_exec\.(?P<cluster>\d+)\.0 .*STARTING UP"
)
starts = [(i, int(m["cluster"])) for i, line in enumerate(lines)
          if (m := STARTUP.search(line))]
if not starts or starts[-1][1] != current_cluster:
    return None
first = len(starts) - 1
while first and starts[first - 1][1] == current_cluster:
    first -= 1
return lines[starts[first][0]:]
~~~

输出根 DAG、每个 wave、PLAN 与各 worker 阶段的 running、queued、held、failed、done、skipped、pending 数量及关键节点。worker 阶段从 tasks/<node>.json 的 stage 读取，避免把 MINIAOD、ARCHIVE 和 RECLAIM 都误称为 processing。活动和 held 优先采用当前队列记录；读取本次 run.dag.nodes.log、steps/*.dag.nodes.log 与 logs/events.log 中的提交、执行、held、释放及终止事件作为补充，并标明它们是最后观察值。终态采用本次 DAGMan/node 日志及完成凭据。只有根成功退出、state.json 为 complete、.complete 存在，且已运行节点有本次终态证据时才报告整体 complete；证据冲突或缺失则明确报告 unknown/inconsistent。

## 3. 产物凭据与只读约束

读取 step < state.json.next_step 的 ledger/waves/*.json 与相应 archive 凭据；当前待归并波次的 results/*.json 单列为 uncommitted。按 task_id、artifact_id 和 URL 对齐结果与产物引用，避免重复展示 archive 输出。每条记录显示凭据来源、结果状态、是否已归并、events、size_bytes、url、sidecar_url；缺失字段显示为空，不从文件名或大小推算事件数。archive member 和已回收的暂存产物标明其凭据角色，不暗示远端仍存在。JSON 中加入 remote_stat_checked=false，文本也说明未独立 stat 远端文件。入口只读本地记录并执行只读 condor_q，不执行 xrdfs/xrdcp 或任何写状态的后端函数。

更新 Condor operator 的 SKILL.md 与 operations reference，并局部修正 docs/testing.md：mcprod 使用上述 --run 命令；旧 DAG 使用 dag_snapshot.py <root> --schedd <host>，根已离队时另给 --dag-file；详细结构继续用 dag_progress.py --structure=collapsed。

## 4. 回归测试与验收

- 测试 PRE 返回 99 后尚无成功事件、随后 ULOG_PRESKIP、非匹配 PRE 退出码、真实失败和 held；验证 skipped 单列且计入完成数。
- 测试 WAVE_* 折叠结构、根队列为空或查询失败、当前 attempt 与旧 attempt 的混合日志、过期 last_status、身份参数冲突，以及缺少子 DAG 日志时不借用旧 attempt。
- 测试已归并 wave、未归并 result、archive member 与 sidecar 字段；对临时 run 的 submission.json、state.json 和 ledger 做调用前后内容及修改时间检查。
- 执行 tests.test_dag_progress 和新增的 mcprod snapshot 测试，以及 AGENTS.md 要求的静态检查与相关既有测试；用已保存的 12758251 DAGMan 日志和凭据做离线回放，模拟空队列，应显示根 5/5、两个已运行 wave、三个 skipped、零失败及凭据中的 MiniAOD/Archive 详情。回放前后记录保持不变，不重新查询当前 CERN/IHEP 作业。
