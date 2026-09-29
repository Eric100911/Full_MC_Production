# IHEP cutover 重试失败报告

日期：2026-09-29

调查范围：2026-09-28 的 IHEP cutover retry，Job 81709065
操作边界：本报告依据已留存日志、固定 runtime 和代码 revision 做只读核查；未改动作业、EOS 产品、runtime 或源代码。

## 后续修复与恢复（2026-09-29）

上面的操作边界只适用于最初对 Job 81709065 的调查。随后已修复 batch worker 的运行包准备路径，并在 IHEP 上恢复同一个 merge group。修复使 `batch prepare` 在任务中记录固定 runtime 目录；combined worker 将目录传给各阶段。ntuple 阶段从该目录取 CMSSW15 包，在自己的工作目录建立 `common/packages`；selection 和 efficiency 阶段从同一 runtime 取 analyzer 包，在各自工作目录解压。共享的 `controller/core` 和原有固定 runtime 均未被改写。`runtime_dir` 不参与阶段检查点身份计算，所以有效的 merge 检查点得以复用。

实际恢复还揭示三个后续兼容问题，已在 `external/NtupleAnalyzer` 修复：JJP kaon 索引的新字段名需要映射到既有 selected 输出名；ROOT Snapshot 需要将四个嵌套触发索引列转为可写出的 `std::vector<std::vector<int>>`；效率计算需要把无效 muon 索引视为不匹配，并将候选级触发匹配归并为每事件一个标志。这些问题在原报告的 ntuple 初始化错误之后，只有继续运行完整链路才能观察到。

恢复过程复用了原 merge checkpoint。Job 81724955 完成 ntuple，得到 9 事件原始 ntuple；Job 81725418 暴露 Snapshot 类型问题；Job 81725817 完成 selection，并暴露效率计算问题。修正后的效率命令先用同一份 9 事件 ntuple 在 IHEP 私有目录中跑通，再由 Job **81726395** 完成正式批任务发布。最终 `merge.json`、`ntuple.json`、`selection.json`、`efficiency.json` 均为 `status=ok`。四个数据产物及其 sidecar（伴随产物发布的元数据文件）均通过 IHEP EOS `xrdfs stat`；大小分别为 3,432,535、222,689、17,816 和 143,360 bytes。selection 输出为 0 事件；这份 9 事件的小样本只用于链路验证，不用于物理效率结论。

最终重试记录保存在 IHEP 的 `/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production/runs/dps1_cutover_20260924_r2/retry-snapshot-20260929T031626Z/retry_manifest.json`，状态为 `completed`，`completed_job_id=81726395`。修复代码当时通过私有重试目录提供给 worker；旧的 `production-1449aa9-dps1-reclaimfix` 发布包仍是原始内容。新的完整 cutover pilot 应先从已提交且可获取的 core 与 analyzer 修复构建干净 runtime，不应把这次私有目录当作正式发布包。

验证包括 core 相关单元测试 29 项、analyzer 回归测试 5 项、Python 编译检查，以及实际 9 事件 ntuple 的 selection 和 efficiency 命令。旧版 `run_all_tests.sh` 与 `mock_test_worker.sh` 在受限 Codex 沙箱内被 Singularity 的 Unix socket 权限阻断；该失败不能证明或否定本次 batch 修复。

## 结论

IHEP 重试已经结束，结果失败。Job 81709065 在 JJP_DPS1 combined task 的 ntuple 阶段，以 exit code 1 退出。直接错误是 worker 所用目录缺少：

~~~text
.../worker/ntuple/native/common/packages
~~~

冻结 runtime 根目录其实包含 CMSSW15 runtime 压缩包；缺的是 batch worker 执行路径下将它放到 worker 预期位置的步骤。现有 merge checkpoint 有效并被复用，失败发生在 ntuple 初始化，不是 merge 失败。ntuple、selection 和 efficiency 产品没有由这次 retry 生成。

## 作业身份与时间线

- Retry：retry-20260928T134951Z；task ID：JJP_DPS1_run3_2022_legacy_premix_v1_MERGE000000；task SHA256：959d33947472d61db9ca52d4c2547c637f893cac03f03d513228b8062afb360a。
- IHEP job ID：81709065；节点：ams018.ihep.ac.cn；提交时间：2026-09-28 13:56:56 UTC。
- runtime：production-1449aa9-dps1-reclaimfix；固定 core revision：1449aa9f3c6550c31e9acd3b3bb2e4f0d0ba8e4c；descriptor 标记 source_dirty=false。
- 13:57:49 和 14:01:36 UTC 的精确查询都显示 1 idle、0 running。
- IHEP stdout/stderr 文件时间为当地 22:05:36 和 22:05:44（UTC+8），对应约 14:05:36 和 14:05:44 UTC。worker 启动后很快在 ntuple 命令失败。
- 14:09:19 UTC 查询显示队列中已无该 job。2026-09-29 02:18:44 UTC 再次精确查询仍显示 0 jobs。
- 注意：retry_manifest.json 保留的 state 仍是 submitted，last_queue_snapshot_at 是 13:57:49；它没有反映最终状态。判断终态应看后续 hep_q 快照和 worker stderr/commands.log。hep_q 中的 “0 completed” 是当前队列计数，不代表该任务成功。

## 实际执行到哪里

此前的 merge checkpoint status=ok，输入事件数为 5 和 4，合并输出为 9 个事件，ROOT 文件大小 3,432,535 bytes。其 ROOT 文件和 JSON sidecar 均通过 IHEP EOS stat。retry_manifest 指明复用了这个 checkpoint，所以没有重做 merge。

JJP combined pipeline 顺序是 merge → ntuple → selection/efficiency。retry 在 ntuple 进入 Step 7 后因 packages 路径缺失而停止。检查点目录只含 merge.json，没有 ntuple checkpoint；失败后再次 stat，ntuple ROOT 和 efficiency tar 都不存在。selection 和 efficiency 没有机会运行。

## 关键日志片段

### worker 最终错误

0000.err：

~~~text
ERROR: cmssw15-ntuple-el9 command failed (1); diagnostics: /scratchfs2/.../retry-20260928T134951Z/work/0000/worker/ntuple/commands.log
~~~

commands.log：

~~~text
[WARN] No valid VOMS proxy detected. Set X509_USER_PROXY to a job-private or persistent proxy path.

MC Production Chain: campaign=JJP_DPS1 job=JJP_DPS1_run3_2022_legacy_premix_v1_MERGE000000_ntuple analysis=JJP sources=1 max_events=-1 target_mixed=100
-- Step 7: Ntuple (JJP) --
[ERROR] Packages directory missing: /scratchfs2/.../retry-20260928T134951Z/work/0000/worker/ntuple/native/common/packages
~~~

stdout 记录了 task ID 和持久 worker 工作目录。日志中的 VOMS 告警不是这次退出的直接错误：提交预检时 proxy 尚有约 669,000 秒有效期，merged MiniAOD 也已成功落到 worker 工作目录。commands.log 随后明确报错为 packages 目录缺失。

### merge 与输出核查

merge.json 记录 status=ok、输入事件数 [5, 4] 和 9-event merge 输出。output-audit 在提交 retry 前确认 merge ROOT 与 sidecar 存在，而 ntuple、selected ROOT 和 efficiency tar 不存在。2026-09-29 的只读复查再次确认 ntuple ROOT 和 efficiency tar 不存在。

## Runtime 目录证据

runtime.json 把 cmssw15-ntuple-el9 映射到单独的 profile bundle：

~~~json
{
  "bundles": {
    "core": "core.tar.gz",
    "cmssw15-ntuple-el9": "cmssw15_tpsonia2mumu_runtime.tar.gz"
  },
  "bundle_bytes": {
    "core.tar.gz": 196999,
    "cmssw15_tpsonia2mumu_runtime.tar.gz": 10595396
  },
  "runtime_release": "production-1449aa9-dps1-reclaimfix",
  "git_revision": "1449aa9f3c6550c31e9acd3b3bb2e4f0d0ba8e4c",
  "source_dirty": false
}
~~~

IHEP 上只读检查到的 runtime 根目录包含 cmssw15_tpsonia2mumu_runtime.tar.gz；但 controller/core/common 下只有 campaigns、cmssw_configs、compression helpers、node_config_defaults.json、octet_pdg.py 和 profiles，**没有 packages/**。worker 的 native/common 符号链接指向该 controller/core/common，因此 worker 的 native/common/packages 也不存在。

## 固定 revision 中的代码路径

以下代码片段均来自部署 descriptor 指定的 core revision 1449aa9f。

### 1. Batch worker 直接进入 combined-task 执行

[src/full_mc/cli.py（137–140 行）](../src/full_mc/cli.py#L137-L140)：

~~~python
if args.command == "batch":
    if args.action == "worker":
        from .application.resume import run_combined_task
        return run_combined_task(args.task, args.work_dir, log_level=args.log_level)
~~~

[src/full_mc/application/resume.py（79–86 行）](../src/full_mc/application/resume.py#L79-L86) 先检查阶段 checkpoint；没有可复用 checkpoint 时直接执行该 stage：

~~~python
saved = _checkpoint_ok(checkpoint, current)
if saved:
    outputs = saved["outputs"]
    state = "resumed"
else:
    outcome = execute_task(current, root / stage)
~~~

本次 merge 是 resumed，ntuple 被执行。此执行路径没有经过 runtime/bootstrap.py 中负责把 profile bundle 安装到 core/common/packages 的 unpack_runtime 函数。

### 2. Worker common 目录链接到固定 core

[src/full_mc/workers/chain.py（13–27 行）](../src/full_mc/workers/chain.py#L13-L27)：

~~~python
root = work_dir / "native"
processing = root / "processing"
processing.mkdir(parents=True, exist_ok=True)
shutil.copy2(core_path(task, "processing/run_chain.sh"), processing / "run_chain.sh")
shutil.copytree(core_path(task, "processing/cmssw_helpers"),
                processing / "cmssw_helpers", dirs_exist_ok=True)
if not (root / "common").exists():
    (root / "common").symlink_to(core_path(task, "common"), target_is_directory=True)
~~~

这与 IHEP 上观察到的符号链接目标一致：worker/ntuple/native/common → runtime/ihep/controller/core/common。

### 3. Core bundle 与 profile bundle 分开发布

[src/full_mc/runtime/packages.py（33–42 行）](../src/full_mc/runtime/packages.py#L33-L42) 的 CORE_PAYLOAD 只列出明确的 core 文件；其中没有 common/packages。构建 CMSSW15 profile 时，代码把归档作为 runtime release 根目录中的独立 bundle（[228–240 行](../src/full_mc/runtime/packages.py#L228-L240)）：

~~~python
source = core / "common/packages" / CMSSW15_RUNTIME
if source.is_file():
    validate_cmssw15_runtime(source)
    shutil.copy2(source, staging / source.name)
    bundles.update({profile: source.name for profile in cmssw15_profiles})
~~~

### 4. 标准 bootstrap 有安装 bundle 的步骤

[src/full_mc/runtime/bootstrap.py（55–72 行）](../src/full_mc/runtime/bootstrap.py#L55-L72) 会为受支持的 profile 建立 common/packages，再把 bundle hardlink 或复制进去：

~~~python
packages = core / "common/packages"
packages.mkdir(parents=True, exist_ok=True)
target = packages / Path(descriptor["bundles"][profile]).name
try:
    os.link(asset, target)
except OSError:
    shutil.copy2(asset, target)
~~~

但上面的 batch worker 分发直接调用 run_combined_task，没有走这个 unpack_runtime 安装路径。这解释了为什么 release 根目录有 10.6 MB 的 CMSSW15 archive，worker 仍看不到 common/packages 中的 archive。

### 5. run_chain.sh 对缺失目录硬失败

[processing/run_chain.sh（57 行）](../processing/run_chain.sh#L57) 将包目录设为 common/packages。prepare_cmssw15_from_package 在目录不存在时直接返回错误（[1253–1263 行](../processing/run_chain.sh#L1253-L1263)）：

~~~bash
if [[ ! -d "${PACKAGES_DIR}" ]]; then
    msg_error "Packages directory missing: ${PACKAGES_DIR}"
    return 1
fi
~~~

这与 commands.log 中的失败路径逐字吻合。prepare_cmssw15_for_ntuple 会先查找 packages 目录下的预构建 CMSSW15 runtime；找不到时回退到 source package 路径，而回退函数也要求 packages 目录存在。

## 交接给修复会话

建议从 batch combined worker 的 runtime materialization 接口处理：运行 ntuple stage 前，按 task runtime profile 将 CMSSW15 bundle 放入该 stage 可见的 common/packages，或让 worker 以明确的 bundle 路径消费它。不要因为包缺失而重跑有效 merge。runtime 是共享固定 release；修复实现时应避免在共享 controller/core 中做并发可变写入，并针对实际 batch worker 路径增加验证，确认 ntuple 开始运行且能读取预构建 archive。

VOMS 警告可单独核查容器内环境传递，但本次直接退出原因已由 packages 路径错误充分定位。

## AFS 留存日志索引

日志目录：generated/mcprod-pilots/dps1_20260924_reclaimfix/operator-logs/ihep-retry-20260928T134951Z/

| 日志 | 用途 |
|---|---|
| [retry_manifest.json](../generated/mcprod-pilots/dps1_20260924_reclaimfix/operator-logs/ihep-retry-20260928T134951Z/retry_manifest.json) | task hash、job ID、节点、runtime 和 checkpoint 复用关系；最终 state 字段已过时 |
| [submit-20260928T134951Z.txt](../generated/mcprod-pilots/dps1_20260924_reclaimfix/operator-logs/ihep-retry-20260928T134951Z/submit-20260928T134951Z.txt) | hep_sub 返回值、Job ID、节点、proxy 预检 |
| [hep_q-20260928T135749Z-81709065.txt](../generated/mcprod-pilots/dps1_20260924_reclaimfix/operator-logs/ihep-retry-20260928T134951Z/hep_q-20260928T135749Z-81709065.txt)、[hep_q-20260928T1400Z-81709065.txt](../generated/mcprod-pilots/dps1_20260924_reclaimfix/operator-logs/ihep-retry-20260928T134951Z/hep_q-20260928T1400Z-81709065.txt) | 首批队列快照；显示 idle |
| [status-20260928T140916Z-81709065.txt](../generated/mcprod-pilots/dps1_20260924_reclaimfix/operator-logs/ihep-retry-20260928T134951Z/status-20260928T140916Z-81709065.txt) | 14:09 队列与 scratchfs2 持久文件清单 |
| [runtime-logs/0000.out](../generated/mcprod-pilots/dps1_20260924_reclaimfix/operator-logs/ihep-retry-20260928T134951Z/runtime-logs/0000.out)、[runtime-logs/0000.err](../generated/mcprod-pilots/dps1_20260924_reclaimfix/operator-logs/ihep-retry-20260928T134951Z/runtime-logs/0000.err) | worker stdout/stderr |
| [runtime-logs/commands.log](../generated/mcprod-pilots/dps1_20260924_reclaimfix/operator-logs/ihep-retry-20260928T134951Z/runtime-logs/commands.log) | 实际 ntuple chain 错误 |
| [runtime-logs/chain.json](../generated/mcprod-pilots/dps1_20260924_reclaimfix/operator-logs/ihep-retry-20260928T134951Z/runtime-logs/chain.json) | 实际 stage 的 detector profile 和 runtime |
| [merge.json](../generated/mcprod-pilots/dps1_20260924_reclaimfix/operator-logs/ihep-retry-20260928T134951Z/merge.json)、[output-audit-20260928T135213Z.txt](../generated/mcprod-pilots/dps1_20260924_reclaimfix/operator-logs/ihep-retry-20260928T134951Z/output-audit-20260928T135213Z.txt) | 可复用 merge 和下游产物检查 |
| [retry-preflight-20260928T134951Z.txt](../generated/mcprod-pilots/dps1_20260924_reclaimfix/operator-logs/ihep-retry-20260928T134951Z/retry-preflight-20260928T134951Z.txt) | runtime revision、proxy、scratchfs2 容量和输入/输出预检 |
| [前次 0000.err](../generated/mcprod-pilots/dps1_20260924_reclaimfix/operator-logs/ihep-retry-20260928T134951Z/0000.err)、[81701659 的 hep_q 快照 1](../generated/mcprod-pilots/dps1_20260924_reclaimfix/operator-logs/ihep-retry-20260928T134951Z/hep_q-20260928T134318Z-81701659.txt)、[快照 2](../generated/mcprod-pilots/dps1_20260924_reclaimfix/operator-logs/ihep-retry-20260928T134951Z/hep_q-20260928T134403Z-81701659.txt) | 前一次失败任务的有限证据；其 /tmp 诊断日志未持久化 |
