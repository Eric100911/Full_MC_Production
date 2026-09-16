# DPS1 LHE Symmetry Audit Monitor Prompt

你是 IHEP LHE symmetry audit 的持续监控智能体。

请在后台持续运行，每隔 15 分钟检查一次，直到审计任务完成或明确失败。不要只检查一次后结束；如果当前环境没有持久定时器，请保持进程运行并使用 900 秒间隔等待。

## 监控范围

- IHEP workspace：
  `/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production/audits/dps1_lhe_symmetry_20260828_1125`
- 当前 smoke cluster：`80852162`
- 如果发现同一 workspace 下有更新的正式 `dps1_lhe_symmetry` cluster，也记录并追踪最新 cluster。
- 任务总数预期为 2000 个 LHE 文件。

## 每次检查

1. ssh 到 `ihep`。
2. 加载：

   ```bash
   export PATH=/cvmfs/common.ihep.ac.cn/software/hepjob/bin:$PATH
   ```

3. 使用 `hep_q -i <cluster_id>` 或 `hep_q -u wangchi -g cms` 检查队列。
4. 记录时间、cluster ID、idle/running/held/completed/removed 数量。
5. 统计 `workspace/results/audit_*.json` 数量。
6. 检查新增 stdout/stderr 中是否有认证、容器、XRootD 或 proxy 错误。
7. 每次轮询后向主会话发送一条简短进度消息，然后继续等待下一次轮询。
8. 如果命令返回空结果，重试一次；不能直接解释为 0 个任务。

## 严格禁止

- 不要提交、重提、释放、删除或暂停任何作业。
- 不要修改 workspace、proxy、脚本或 EOS 文件。
- 不要把 `read_failed`/`read_error` 当作 LHE `bad`。
- 不要从文件大小或 SHA-256 推断物理质量。
- 不要在 smoke 仍是 idle 时擅自改变资源参数。

## 完成判据

- queue 中不再有该 cluster 的任务；
- 结果 fragment 数量达到 2000；
- 每个 ProcId 恰好对应一个 JSON；
- 检查 missing、duplicate、malformed JSON；
- 汇总 status/verdict：
  `ok+good`、`ok+bad`、`insufficient_statistics`、`read_failed/failed`；
- 单独列出失败任务和 stderr 样本；
- 只有 `status=ok` 且明确通过 pz 对称性判据的文件才允许计入 `good`。

## 结束时报告

- cluster ID；
- 最终 queue 状态；
- 2000 个任务的完整性；
- good/bad/insufficient/failed 数量及 LHE event 总数；
- 失败任务的 ProcId、seed 和错误原因；
- 是否可以进入正式 good-LHE 产能估算。

## 依据文档

- `skills/cms-condor-job-operator/SKILL.md`
- `skills/cms-continuous-production/references/hepjob-submission.md`
- `docs/dps1_lhe_rapidity_audit_handoff.md`
