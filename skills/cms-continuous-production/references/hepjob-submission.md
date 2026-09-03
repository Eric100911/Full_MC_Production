# IHEP HepJob 提交参考

> 实战经验总结，来源：SPS_G / DPS2_G / SPS_CS / DPS2_CS 的 CERN→IHEP MERGE+NTUPLE
> 交接（2026-08）。每条都是踩过的坑 + 已验证的应对。适用于通过 `hep_sub` 在
> IHEP 提交 MERGE / NTUPLE HepJob。

## 1. hep_sub 基础

- **PATH**：`hep_sub` 位于 `/cvmfs/common.ihep.ac.cn/software/hepjob/bin/`，
  ssh 到 IHEP 后必须先 `export PATH=...:$PATH` 才能调用。
- **标准调用模式**（每次提交一个任务，单参数即 job index）：

  ```bash
  export PATH=/cvmfs/common.ihep.ac.cn/software/hepjob/bin:$PATH
  hep_sub <run_script> -g cms -gwn CMS -wt mid \
    -argu <idx> -n 1 \
    -o <log_dir>/<name>_${idx}.out \
    -e <log_dir>/<name>_${idx}.err
  ```

  - `-argu <idx> -n 1`：单个参数就是任务 index，worker 脚本里用 `$1` 读；
    `-n 1` 只申请一个 slot。
  - `-g cms -gwn CMS`：IHEP 侧计费组名（与 CERN 侧 Condor 无关）。
  - 日志按 `_${idx}` 命名，重跑/排错靠它定位，不要把多任务日志混进同一文件。
- **同步返回**：`hep_sub` 提交完成才返回（不像 `condor_submit` 立即回）。
  批量提交在循环里逐个调用、按返回码统计 `ok/fail`，不要并发刷。

## 2. 节点选择（最重要：AVX2）

- **事故**：ntuple 用 `cmssw-el9` 容器跑 CMSSW_15，在 **Ivy Bridge 节点
  cws050 / cws055（无 AVX2）** 上 `rc=132 Illegal instruction`。merge 用
  `cmssw-el8`，不受影响，正常提交即可。
- **安全节点（11 台）**：
  `ams017 ams018 ams019 ams020`（各 56 slot）+ `cws047 cws048 cws049 cws051 cws052 cws053 cws054`（各 24 slot）。
  ⚠️ **cws052 是安全的**（E5-2650 v4 Broadwell，has_avx2=true）。早期 SKILL.md
  把 cws052 列进坏节点是错的，已修正。
- **`hep_sub` 不能按 CPU 特性/AVX2 过滤**，只有 `-wn <hostname>` 钉单节点
  （已核对 IHEP 官方文档：afsapply.ihep.ac.cn/cchelp/zh/local-cluster/jobs/HTCondor/）。
- **正确做法**：提交 ntuple 时**从一开始就用 `-wn` 在 11 台安全节点上
  round-robin**（`nodes[idx % 11]`），从源头避开 ~50% 的 rc=132 浪费，不要等
  失败再补跑。G 交接裸跑失败率 14–32%，CS job_0 裸跑 50%（cws050/055 空闲被占满）。
- **现成脚本**：`skills/cms-continuous-production/scripts/submit_ntuple_pinned.sh
  <campaign> <workspace>`（IHEP 侧执行）——重建 proxy bundle、校验 merge gate、
  round-robin 钉安全节点提交全部 ntuple。Python 版
  `submit_ntuple_safe.py --campaign <C> --job-index <N>`（CERN 侧缓存驱动）。

## 3. Proxy 处理

- **事故**：`run_miniaod_merge.sh` 把 proxy 装到节点共享的
  `/tmp/x509up_u$(id -u)`，同节点并发同 UID 作业互相覆盖 →
  `install: cannot create regular file ... File exists` → merge 未跑就死。
  这是 **节点全局共享路径 + 并发作业** 的竞态。
- **修复**：改作业私有路径 `PROXY_TARGET="${PWD}/credentials/x509_user_proxy"`
  + `chmod 600`（不碰 /tmp、不 `rm credentials`）。ntuple 分支本来就是私有路径。
- **提交前重建 proxy bundle**（幂等）：

  ```bash
  TMP=$(mktemp -d "$WS/.proxy.XXXXXX"); trap 'rm -rf "$TMP"' EXIT
  mkdir -p "$TMP/credentials"
  install -m 600 "$PROXY_SOURCE" "$TMP/credentials/x509_user_proxy"
  tar -czf "$WS/bundles/proxy_bundle.tar.gz.tmp" -C "$TMP" credentials
  chmod 600 "$WS/bundles/proxy_bundle.tar.gz.tmp"
  mv -f "$WS/bundles/proxy_bundle.tar.gz.tmp" "$WS/bundles/proxy_bundle.tar.gz"
  ```

  `submit_ntuple_pinned.sh` 里已内建这一步，还带 `voms-proxy-info` 剩余时间检查
  （<10 分钟拒绝提交）。
- ⚠️ CERN 侧还有 ~8 个 wrapper/submit template 用 `/tmp/x509up_u$UID` 旧模式，
  同一隐患未修——将来 CERN 侧跑同样会踩。

## 4. 登录节点 / 并发

- **fork 耗尽**：25 个并行 build 把 IHEP 登录节点打到 load 16–51，所有 build 全挂。
- **对策：构建串行化**，一次只跑 2 个 campaign 级 build（合并成一个大 workspace，
  见 §6），不要每个 job 一个小 workspace 并行刷。
- **`hep_q`/`xrdfs` 在 fork 耗尽时返回空**，容易误判成"队列空 / 0 完成"。
  **看到空结果必须重试**，区分命令失败与真 0。

## 5. ssh 可靠性

- **ControlMaster socket 腐坏** → 反复 `exit 255`。修复：
  `ssh -o ControlMaster=no ...` + `rm -f /tmp/chiw/ssh-cm-*`（CERN 侧 /tmp 会被周期清理）。
- **AFS token 过期** → `klist && aklog` 刷新。
- **EOS 查询走 `ssh ihep` 内网直查更快**（lxlogin 内网），不要从 CERN 侧直接
  出去查 EOS。

## 6. 构建 / workspace

- **workspace 布局**（IHEP）：
  `/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production/runs/<ts>/<batch>/`
  - `scripts/run_ntuple_<campaign>.sh`、`scripts/run_merge_<campaign>.sh`
  - `bundles/proxy_bundle.tar.gz`、`tasks/<campaign>/ntuple_tasks.json`、`gates/`
  - `logs/ntuple/<campaign>/`
- **后台 build**：`nohup bash -lc '<cmd>' > <log> 2>&1 & echo started`。
  log 目录**必须放持久路径**（`/scratchfs2` 下的 `build_logs`），IHEP `/tmp`
  会被周期清理——当时 nohup log 直接丢了。
- **尽量合并成 campaign 级大 workspace**：`audit-split --stage mix` 的
  `export_mix_manifest` 原生支持多 job 合并（`coordinator_manifests` 是 list），
  不要 26 个 job 各建一个小 workspace。

## 7. 监控 / 重跑

- **记录 cluster ID**（hep_q 的 cluster 号）；"卡住"先查是不是**正常排队**
  （等别的 campaign 释放 slot），别急着下结论。
- **补跑只跑缺失 idx**：`xrdfs ls` 统计 EOS 上落盘的 merge/ntuple 目录，缺哪些
  补哪些，不要整批重跑。
- **幂等**：重跑 `xrdcp -f` 覆盖同名输出，冗余提交无害。
- **held triage**：查 `.err` 区分 AVX2（rc=132）/ proxy 竞态 / 资源问题。
- **ntuple 重跑多 cycle**：被重跑过的 ntuple 文件含多个 cycle
  （`mkcands/X_data;1`、`;2`…）。读文件必须取最新 cycle（`;N` 最大的），否则读到
  半截写。判据见 `docs/tps_sampling_anomaly.md` §3.6。

## 8. 检查清单（每次交接）

1. ssh 到 IHEP，`export PATH=.../hepjob/bin:$PATH`
2. `klist && aklog`（token 有效）；proxy 剩余时间 > 10 分钟
3. merge 正常提交（`cmssw-el8`，不钉节点）
4. ntuple 用 `submit_ntuple_pinned.sh`（merge gate 已存在 + `-wn` round-robin）
5. 批量构建串行化；nohup log 放 `/scratchfs2`
6. 监控用 `hep_q -u wangchi`；空结果重试；"卡住"先确认是否排队
7. 补跑只补缺失 idx；`xrdcp -f` 幂等
