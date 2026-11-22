# 执行提示词一：有限 HELAC 扫描与后续运行准备

你负责执行本阶段，不是继续改写计划。目标是完成一次有限、从新输入开始的 HELAC 参数扫描，比较实际产率和成本，交付阶段二可用的精确计数 LHE 清单，并补齐后续 1k/99k 运行不可缺少的接口。扫描本身的原始生成器事件数不计入后续每个 campaign 的 1000 个物理事件。用户已授权这套三阶段实验、IHEP 两核方案和全局 EDM 编号设计；普通提交无需再次征求同一授权。不得另开补产运行，也不得为凑数延长已结束的运行。

## 工作边界与协作

工作目录固定为 `/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/v06_00-staging-scheduler`，分支 `feat/v06_00-staging-scheduler`。起始已知提交 `1c4d00860d57753b945b340ec571a44a71aa8d7f`。先读实际 `AGENTS.md`、`docs/technical_language_style.md`、`/eos/home-c/chiw/.codex/skills/stop-that-shit/SKILL.md`；涉及真实操作时读 `skills/cms-ihep-condor-pilot/SKILL.md`、`skills/cms-condor-job-operator/SKILL.md` 及其 Condor 操作参考；容量分析读 `skills/cms-lhe-capacity-planner/SKILL.md`。这些文件中旧 G3/G5 pending 或 11 节点文字不能覆盖下文的新证据和用户明确指令。

协调者不执行命令。把命令、实现和监控交给执行代理；简单执行/监控优先用 Luna，跨文件实现用 6.1。协调者只接收简短结论、退出码和证据路径，不接收整段日志。每个主要阶段结束后合并做一次评审，证据充分后停止重复检查。同一代理同一工作扇区发生超过三次真实失败，暂停该扇区，完成独立工作后交人工评审；排队、PRE 等待、正常 vacate 和作业尚未完成不算失败。不要新建守护进程、多运行账本、后台协调器或通用基础设施。

现有八项未提交分桶改动属于本实验基础：`docs/v06_00_campaign_shards.md`、`src/full_mc/application/production.py`、`src/full_mc/application/shards.py`、`src/full_mc/planning/graph.py`、`src/full_mc/planning/shards.py`、`tests/test_core_shards.py`、新增 `src/full_mc/local_paths.py`、新增 `tests/test_core_local_paths.py`。保留其内容，评审后与本阶段必要修正形成有范围的本地不可变提交，不能回退、漏打包或借用旧 G5 runtime。可以创建本地提交以构建干净运行包；没有公开 tag、push 或发布授权。

旧 production-core、wrapper、旧生产路径、旧 G5 和记忆中的库存都不动；旧 G5 已完成，不轮询它们。现有 AFS 证明 `generated/v06_00_site_validation_20261017T104551Z/cern-afs-lock-proof.json` 已记录 bigbird11/bigbird22 的 mutual_exclusion、read_after_unlock、crash_recovery、pre_defer、pre_skip、dynamic_subdag、autorescue 全为 true。读其结构及关联评估即可，不为形式合规重跑锁探针或旧现场测试。

## 路径与时间预算

由执行者用 UTC 时间建立一个未使用的实验编号，例如 `three_stage_20261018T120000Z`；下例编号只是路径示例，使用前确认未存在。固定后记入交接，后续两阶段只读同一个实验目录。不要使用 shell `eval` 或把 JSON 输出拼成命令。

```bash
export CORE=/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/v06_00-staging-scheduler
export EXP_ID=three_stage_20261018T120000Z
export EXP="$CORE/generated/$EXP_ID"
export SWEEP="$EXP/stage1/sweep"
export RELEASE="v06_00-$EXP_ID"
export CERN_RUNTIME="$EXP/runtimes/cern"
export IHEP_RUNTIME="$EXP/runtimes/ihep"
export SChedd=bigbird11.cern.ch
export PROOF="$CORE/generated/v06_00_site_validation_20261017T104551Z/cern-afs-lock-proof.json"
```

实验所有暂存、最终产品、归档、交接和运行目录独立。CERN 暂存根为 `root://eosuser.cern.ch///eos/user/c/chiw/Full_MC_Production/three_stage/<EXP_ID>/staging`；IHEP 产品、归档、交接分别为 `root://cceos.ihep.ac.cn:1094///store/user/chiw/Full_MC_Production/three_stage/<EXP_ID>/products`、`.../<EXP_ID>/archive`、`.../<EXP_ID>/handoffs/stage1`。`<EXP_ID>` 必须由固定变量替换为真实值，不作为字面路径提交。scratch 只用 `/tmp/chiw/`，DAG、日志及运行包留 AFS。现有 `/afs/cern.ch/user/c/chiw/condor/mcprod-pilot/sites/ihep.json` 只有 name/destination，作为路径约定参考；不存在可直接依赖的 cern.json。

从首次扫描提交的 UTC 时刻起，本阶段最多监控 48 小时：提交后记录一次基线，此后每两小时一批，最多 24 批。完成或用户通知可以触发立即检查。等待不构成失败；到 48 小时记录未完成的精确 ID、状态、证据，保留运行，不自动移除或重开。

## 先实现必要缺口，再冻结运行包

以下是当前缺口，不能把新接口称为已经存在：

1. 有限 HELAC 扫描没有现成 mcprod 子命令。旧 `generate-helac-matrix` 固定 162 个 Fock 组合，不适用于本次扫描。实现一个小的一次性准备器 `tools/prepare_three_stage_helac_sweep.py`，只支持本次有限任务的 `plan --spec PATH --output-dir PATH` 与 `summarize --run PATH --output PATH`。这是**待实现的 CLI 契约**。plan 使用现有 `planning/graph.py` 的 `write_submit_files()` / `write_tasks_dag()` 及既有运行包/任务格式，输出单张有限 HELAC-only `run.dag`、显式 tasks、run.json、dagman.config 和冻结扫描清单；不经过 campaign source-admission，否则物理需求满足后剩余扫描格会被跳过。不要加 daemon 或新的生产框架。每格是一个独立 source job，恰好 10 个，全部必须得到终态。
2. `workers/lhe.py` 当前固定 `--helac-diagnostics none`，`SKIP_STAGEOUT=1`；bootstrap 仅打包 `*.log`，`RESULT_*.out` 和 `user.inp` 不会自动保留。为本扫描加最小诊断通路，保留实际输入卡、过程卡、结果文件、最大权重警告、停止原因和 CPU/wall；利用既有 diagnostics.tar.gz 返回并按现有分桶路径保存。`tools/helac_inspector.py` 目前不在 CORE_PAYLOAD；若 worker 解析需要它，明确加入最小 allowlist。保留解析所需的 HELAC 目录结构，避免无限 full trace 或在 AFS 展开大量小文件。不要臆造已有 receipt 中没有的 sigma/error/warning 计数。
3. `backends/hepjob.py` 的 `_submit_command()` 当前硬编码 `-n 1`。定点改为本实验已接受的 `-n 2`；仅必要时暴露窄配置，不添加未经实现的 `--cpus`。CMSSW15 固定轮转 **10** 个主机：ams017/018/019、cws047/048/049/051/052/053/054。ams020 的 Start policy 已拒绝，排除；cws050/055 因 rc=132 排除。combined allocation 的 merge 在该分配里运行，独立 merge-only 作业无需绑节点。`hep_sub` PATH 必须含 `/cvmfs/common.ihep.ac.cn/software/hepjob/bin`。运行日志要显示实际两核请求及实际加载的线程/stream 配置。
4. 当前 campaign_shards 新 run 总从 job_index=0 开始，`edm_event_id(0, job_index, ...)` 会导致同 campaign 跨阶段重复。实施最小新字段：每个 campaign 的 `global_job_index_start`（非负整数，旧输入缺省 0），节点 `global_job_index = global_job_index_start + local_job_index`。本地任务/分片进度仍用本 run 的 0 起索引；EDM 的 run/lumi/event 由全局索引派生，阶段二每 campaign 0..9，阶段三 10..999。把字段和最终 EDM 编号冻结到 allocation、task、sidecar/交接；旧完成标记保持只读兼容，旧运行不迁移。
5. 不新增或覆盖 CMSSW RandomNumberGeneratorService 的 seed。物理随机性来自 HELAC 生成的独立硬散射事件；后续 shower、MIX 和 detector 链消费这些输入。记录 HELAC seed/source_id 与输入事件 provenance，保留现有 shower/MIX 确定性 seed 算法；全局 job index 只用于 EDM event identity，不用于派生 CMSSW RNG seed。仅换 run_id 不代表新源独立，新源必须同时换 source_id 和 HELAC seed。
6. `_combined()` 当前外部 task/ref ID 和 efficiency tar 的 `shard_...` 名使用从零开始的 MERGE ordinal，跨 run 会重复，现有 analysis 聚合会拒绝。增加窄字段 campaign.`merge_group_index_start`（非负、旧输入缺省0）：阶段二0，阶段三1，外部组编号分别0与1..99。仅影响外部 task/ref/merge_group 与 tar shard 身份；内部 ledger 文件 ordinal、merge_group_counts 仍从0，不能破坏 load_groups() 恢复。测试两个 run 的 aggregate 输入 artifact_id/tar member 不重复。这是实现后才可使用的新字段，不是现成 CLI flag。

对上述实现运行相关的实际存在的 scoped 单元测试，并按 AGENTS 完成必要检查和一次离线 DAG 检查。重点覆盖：10 格不被需求跳过；诊断失败仍返回可解释证据；IHEP -n 2 和十节点轮转；阶段二/三同 campaign EDM 三元组互不相交、CMSSW 配置不覆盖 RNG seed、旧标记可读；分桶路径、重试任务身份不变。不为已有充分证据重跑全站证明。每个主要实现阶段只做一次合并评审。

## 固定物理参数和扫描格

HELAC-Onia 2.7.6 main path。`pool_2jpsi_g` 为 `generate g g > cc~(3S11) cc~(3S11) g`，即 SPS J/psi+J/psi NLO*；`min_pt_conia=6`、`min_pt_bonia=4`、`min_pt_q=0` GeV。最后记录的目标质量切割是 `min_m_qqp=min_m_q_beam=3.0969` GeV，来自 `6a47717`；用户没有给出更高数值。将其作为本实验假设写入冻结清单；如执行期间用户明确提供新值，必须在**所有格开始前**替换并统一冻结，不能编造其他质量阈值。

共同参数：pp 每束 6800 GeV；effectiveScale=0，固定 muR=muF=12.8 GeV；ScaleFactor=1，muR_over_ref=muF_over_ref=1；pdf=10000/cteq6m、lhapdf=F；cmass=1.54845 GeV，charm pair mass=3.0969 GeV。其他 LDME、积分器和快度参数沿当前 main 模板，不顺手改动。实际 user.inp/process 卡必须证明以上值。

| 格 | pool | nmc | unwevt | seed | source_id 规则 |
|---|---|---:|---:|---|---|
| A，基线控制 | pool_2jpsi_g | 1000000 | 100000 | 93000,93001 | `<EXP_ID>_s1_A_seed<seed>` |
| B，放宽停止数 | pool_2jpsi_g | 1000000 | 10000000 | 93002,93003 | `<EXP_ID>_s1_B_seed<seed>` |
| C，积分量控制 | pool_2jpsi_g | 2000000 | 100000 | 93004,93005 | `<EXP_ID>_s1_C_seed<seed>` |
| D，两项提高 | pool_2jpsi_g | 2000000 | 10000000 | 93006,93007 | `<EXP_ID>_s1_D_seed<seed>` |
| gg 伴随参考，不扫 nmc | pool_gg | 500000 | 10000000 | 95000,95001 | `<EXP_ID>_s1_gg_seed<seed>` |

gg 是 `generate g g > g g`，min_pt_q=4 GeV，其余 conia/bonia pT 和两个质量切割均为 0，不抄入 2jpsi cuts。扫描共 8 个 pool_2jpsi_g + 2 个 gg 新源，每格两个独立 seed；不要再默认 500k/2000 的 2jpsi profile。A/C 保留 100k 作控制，其余本实验非控制 main source 均用正整数 unwevt=10000000；0、负值非法。

wrapper 令 preunw=nopt=nopt_step=nmc/10、noptlim=nmc。nmc 是每轮积分预算，多轮总成本不能冒充一次 nmc 或实际产出数。main 的 unwevt 在 accepted 数相等时停止；高正值 10m 远高于本扫描 final-pass 上界，不应再有隐藏 2000 cap，也不应因该参数分配 10m 数组。CrystalBall addon 不遵守同一输出 cap，未用于本实验，不修改它或其他无关 profile。

在 `common/profiles/generator_profiles.json` 注册明确的五个新 profile ID（包含实验、A/B/C/D/gg），`availability=ready`、`compatible_pools` 和 process/cuts/nmc/unwevt 按上表。不要把 nmc/unwevt/cuts 塞进 source 行，source 只选择 `generator_profile` ID。扫描准备器直接解析完整 profile，不用其 planning_events_hint 决定任务个数；扫描专用 hint 可暂设 1 并明确仅为通过 profile 结构校验，绝不能作物理容量。扫描结束为选中生产 profile 新建真实计数推导的 hint，再形成干净提交和**新的**两站运行包，不能修改已经提交的扫描运行包。

## 新准备器的输入与运行命令

以下 JSON 是**新准备器的待实现输入格式**，不是现有 mcprod run spec。执行者用 Python json 写入 `$EXP/stage1/sweep-spec.json`，将环境路径/ID替换为真实字符串；禁止 eval。字段不得遗漏：

```json
{
  "schema": "three-stage-helac-sweep-v1",
  "run_id": "由EXP_ID加_s1得到",
  "core_revision": "干净提交的完整SHA",
  "runtime_release": "RELEASE实际值",
  "runtime_dir": "CERN_RUNTIME绝对路径",
  "schedd": "bigbird11.cern.ch",
  "proxy_path": "本实验credentials/x509_user_proxy绝对路径",
  "output_root": "本实验独立IHEP archive下的stage1-sources完整URL",
  "site_proof_path": "PROOF绝对路径",
  "resources": {"lhe": {"cpus": 1, "memory_mb": 3072, "disk_bytes": 4000000000, "flavour": "workday"}},
  "scheduler": {"max_jobs_submitted": 200, "max_jobs_idle": 100, "max_submits_per_interval": 20, "submit_delay": 0, "max_pre_scripts": 20, "max_post_scripts": 20, "maxjobs": {"lhe": 2}},
  "cells": [
    {"cell": "A", "pool": "pool_2jpsi_g", "profile_id": "已注册A实际ID", "seeds": [93000,93001]},
    {"cell": "B", "pool": "pool_2jpsi_g", "profile_id": "已注册B实际ID", "seeds": [93002,93003]},
    {"cell": "C", "pool": "pool_2jpsi_g", "profile_id": "已注册C实际ID", "seeds": [93004,93005]},
    {"cell": "D", "pool": "pool_2jpsi_g", "profile_id": "已注册D实际ID", "seeds": [93006,93007]},
    {"cell": "gg", "pool": "pool_gg", "profile_id": "已注册gg实际ID", "seeds": [95000,95001]}
  ]
}
```

以上 source_id 按表确定，输出 URL 必须包含 source_id。HELAC seed 合法区间 11..99999；已知旧全局段 80000..92459 不使用，检查所选区间在本次已有证据中无冲突，不为此扫描整个远端池。

在本地必要提交完成、source_dirty=false 后，用现有命令构建两个新站点包：

```bash
cd "$CORE"
./mcprod runtime prepare --site cern --release "$RELEASE" --core "$CORE" --output-dir "$CERN_RUNTIME"
./mcprod runtime prepare --site ihep --release "$RELEASE" --core "$CORE" --output-dir "$IHEP_RUNTIME"
```

验证 runtime.json 的 git_revision、source_dirty=false、analyzer 身份及必要 bundle，CERN/IHEP 为同一个干净提交和 release；IHEP 优先 `common/packages/cmssw15_tpsonia2mumu_runtime.tar.gz`，仅无预构建包时用已有 submodule fallback。代理安装到本实验私有 credentials/x509_user_proxy，再由每个 worker 安装到 `${PWD}/credentials/x509_user_proxy`，绝不写共享 `/tmp/x509up_u$UID`，不把凭据内容提交 Git 或放进报告。

下面两个 Python 命令仅在新准备器已实现并通过测试后成立；不得当成当前已有 CLI：

```bash
python3 tools/prepare_three_stage_helac_sweep.py plan --spec "$EXP/stage1/sweep-spec.json" --output-dir "$SWEEP"
```

提交前一次检查有限 DAG 恰好 10 个 source、每格两个 seed、运行包身份、输出隔离及资源；说明源数、nmc、unwevt、预计多轮成本和本扫描不产生 detector 事件。选择 schedd 时执行 `myschedd bump` / `myschedd show`，将实际选择写回冻结配置和 SChedd，不凭示例假定环境。真实网络操作使用获准的正常 CERN/IHEP shell；受限 sandbox 的网络失败不代表远端失败。初次提交使用的 proxy 按现有检查要求剩余寿命超过 48 小时。

```bash
cd "$SWEEP"
_CONDOR_SCHEDD_HOST="$SChedd" condor_submit_dag -no_recurse -AutoRescue 1 "$SWEEP/run.dag"
```

把返回的 root cluster ID 与实际 schedd、UTC、DAG路径一起写入 `$EXP/stage1/submission.json`；不能根据最近时间窗口找 root。提交输出不明确时先核对该次记录，不能再次提交同一张 DAG。

## 每两小时监控与扫描结论

执行者从 submission.json 取实际值，手动或用 Python 设置 `ROOT_CLUSTER`，只对本次 ID 执行：

```bash
condor_q -name "$SChedd" -dag "$ROOT_CLUSTER" -nobatch
python3 "$CORE/tools/dag_progress.py" "$ROOT_CLUSTER" --schedd "$SChedd" --structure=collapsed
```

root 离队后使用相同身份的 DAGMan 日志/metrics、每格结果与诊断；必要 history 仅查询确切 ID 并限制 `-limit 1 -scanlimit 10000`。有真实 worker 失败才诊断其持久日志并用原 DAG 的已有 Retry/AutoRescue 恢复；不能把排队或空队列当失败、另起一次扫描。有限任务结果齐全后不继续轮询。

```bash
python3 "$CORE/tools/helac_inspector.py" parse --root "$EXTRACTED_HELAC_ROOT" --pretty
python3 "$CORE/tools/prepare_three_stage_helac_sweep.py" summarize --run "$SWEEP" --output "$EXP/stage1/sweep-summary.json"
```

`EXTRACTED_HELAC_ROOT` 是对应诊断包在 `/tmp/chiw/` 的真实解包目录，不是旧 worker 目录。LHE 计数使用现有 gzip-aware counter，对每个新文件只完整计数一次并留凭据；不要从文件名、大小或 unwevt 推算产量。

summary 按 cell/replica 记录实际 LHE count、CPU 秒、wall 秒、events/CPU-hour、gzip bytes、sigma nb/error、最大权重警告、是否达到 cap、退出原因和证据路径；确实没有数据的值为 null 并说明原因，不填虚构数字。选 profile 看两份独立 seed 的产率、CPU成本、积分误差/警告与完整性；控制格达到 cap 时明确停止截断，不能当成自然产率。输入同 seed 的前缀重复不允许并入。

## 阶段一交接与停止条件

生成未来交接 `$EXP/stage1/handoff.json`，schema=`three-stage-stage1-v1`，至少包含：experiment_id、status、code_revision、analyzer_revision、runtime_release、cern_runtime_dir、ihep_runtime_dir（两站绝对路径与 descriptor）、site_proof_path、physics（完整 cuts/束流/scale/PDF/质量参数）、sweep_summary_path、selected_generator_profiles（每池完整 resolved profile 和选中理由）、counted_inventory_path、counted_inventory_sha256（沿现有库存校验方式，不新建散列机制）、existing_sources、seed_ranges、phase2_capacity（包含布尔 sufficient）、global_identity_contract（含全局job/EDM编号和merge_group偏移接口证据；明确 CMSSW RNG seed 不被覆盖，随机性来自 HELAC 输入事件）、IHEP 两核/十节点证据、storage（cern_staging/ihep_products/ihep_archive完整URL）、ihep 接收目录/代理路径约定、监控 anchor/deadline 和 evidence_paths。

`existing_sources` 每条必须是现有 mcprod 可接受的准确结构：`pool,source_id,seed,url,actual_events,size_bytes,planned_events`，`planned_events` 为正且不超过实际计数，不带 `generator_profile`（URL 与 profile 二选一）；原始 generator profile、卡和诊断由库存 provenance 保存。URL 指向本实验扫描保留下来的独立 LHE，不是旧池。阶段二可以复用这些新生成源，因为它们来自本实验第一阶段；不能重新生成额外源。

容量必须用 block_events=100 的实际物理 BLOCK 引用，按现有 `allocate_shard()` 模拟：SPS_G 每节点 pool_2jpsi_g:phi_mpi_off 200 LHE；DPS2_G 每节点 pool_2jpsi_g:normal 200 + pool_gg:phi_mpi_off 400 LHE；每 campaign 10 节点，shared pool 各自 stride=1/offset=0，跨 campaign 可共享，同 campaign 不重复。只有整数个完整 block 作为保守容量；不能简单把文件总数乘 hint 或把各源余数凑成虚拟 block。报告两池选定文件数、逐源 floor(planned_events/100)、两个 campaign 的完整 allocation 模拟结果。8+2 个源不保证新 cuts 下够用；若不足，将 status 写 insufficient 并报告实际缺口，停止后续阶段，不另开补产运行。

阶段二/三共用物理固定值：每 MIX 100 目标事件、merge_events=1000、shard_nodes=10、target_window_nodes=20；detector `run3_2022_legacy_premix_v1`，CMSSW_12_4_14、GT `124X_mcRun3_2022_realistic_v12`，RAW 实际两 thread/two stream、请求 2 CPU。ntuple CMSSW_15_0_15、analysis JJP、efficiency ntuple=true、tree `mkcands/X_data`。phi 模式 ISR/FSR on、MPI off、phi pT>3 GeV、repeat=5000、J/psi→μμ 与 phi→KK 强制衰变，保持既有 Pythia 配置。

后续默认隔离容量 staging_bytes=5000000000、reserve_bytes=1000000000，可用 4GB；每 source 原始 LHE ceiling=100000000、其全部 BLOCK 合计 ceiling=100000000，MIX ceiling=64000000。两个互补源加完整十节点 MIX shard 最低可用量 1040000000 bytes。用扫描实际大小确认上限，若不够在冻结阶段二之前明确调高 ceiling/预算并记理由，不自动突破用户隔离预算。归档成功不释放 staging credit，只有现有接受的实际 reclaim 删除凭据才能释放。保留既有 AFS 锁/单权威 accounting、冻结物理 BLOCK 流，不重构。

为阶段三预留 pool_2jpsi_g seeds 从 94000 起、不超过 94999；gg 从 96000 起、不超过 96999。阶段三源数由所选有效 replica 的保守最小实际产率、阶段二实测 shower 产率和整块分配模拟推导，再明确加 25% 有限安全余量，不能照抄历史 597 hint、5/3 或 495/248 文件数。所需源数超出范围则停止并选另一段已证明不交叠的合法区间，不能静默回绕。阶段二全局 job 0..9，阶段三 10..999，stage1 benchmark 不占用这些 detector 编号。

阶段一完成标准为：十个有限任务的结果已解释、诊断/计数可追溯、选择和阶段二容量成立、必要接口已测试且最终新双站运行包干净一致、交接已冻结。若达到 48h 或真实缺口，只报告实际完成量与剩余项，不宣称达成。最终只回传结论、退出码（完成 0，否则非零）和交接/证据路径；不附整段日志。
