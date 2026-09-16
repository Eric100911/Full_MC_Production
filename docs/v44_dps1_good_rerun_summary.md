# v4_4 DPS1 good-LHE rerun — 批次汇总（2026-08-29）

JJP_DPS1（`J/ψ+J/ψ+φ`，双源均为 `pool_jpsi_CSCO_g`）good-LHE 重跑，
Split 工作流（CERN MIX → IHEP MERGE+NTUPLE），2026-08-28 提交、2026-08-29 全部完成。

## 批次身份

| 项 | 值 |
|---|---|
| Campaign | `JJP_DPS1`（`inputs=(pool_jpsi_CSCO_g, pool_jpsi_CSCO_g)`，`modes=(normal, phi_mpi_off)`） |
| Job spec | `common/campaign_job_specs/jjp_dps1_good_full_v2.json` |
| Good LHE | `pool_jpsi_CSCO_g` **seed 100–189**（90 文件，8,924,389 events） |
| 依据 | 2026-08-28 pz 对称性审计（seed 100–1099 全 good，1100–2099 全 bad） |
| 输出前缀 | `/store/user/chiw/JpsiJpsiPhi_MC_Production_v4_4_full` |

## 全链路结果（Split 工作流，3 个 gate 全通过）

| 阶段 | 站点 | 集群 / 证据 | 结果 |
|---|---|---|---|
| CERN MIX | HTCondor | cluster 4085477 @ bigbird101 | 4439 blocks（JOB0:2000 / JOB1:2000 / JOB2:439），exit 0 |
| MIX gate | IHEP | `split-manifest.json`（schema full-mc-production-split-v1） | pass（4439 components / 888 merge / 888 ntuple） |
| IHEP MERGE | HepJob | cluster 80867181 | 888/888 完成，EOS 全匹配 |
| MERGE gate | IHEP | `gates/merge_JJP_DPS1.json` | **888/888 passed, 0 failed** |
| IHEP NTUPLE | HepJob | 每 job 一 cluster（80868835+） | ok=888 fail=0，钉 11 台 AVX2 安全节点，无 `Illegal instruction` |
| NTUPLE gate | IHEP | `gates/ntuple_JJP_DPS1.json` | **888/888 passed, 0 failed** |

手稿 workspace：
`/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production/handoffs/jjp_dps1_v4_4_20260829/`

## 事例数账目（逐层）

| 层次 | 事例数 | 来源 / 方法 |
|---|---|---|
| 输入 good LHE | **8,924,389** | 计数 inventory（seed 100–189） |
| 最终混合事例（MiniAOD） | **3,812,832** | 888 个 `merge_manifest` 的 `actual_events` 求和 |
| **≥1 重建候选组合的事例** | **221,766** | 888 个 ntuple 的 `Pri_passAny` 非空计数（见下） |
| 候选总数（含一事件多候选） | 563,513 | 同上，`n_cand` 求和 |
| 候选事例占比 | **5.82 %** | 221,766 / 3,812,832 |

- 转换效率 LHE→最终混合：3,812,832 / 8,924,389 ≈ **42.7 %**
- 每个候选事例平均候选数：563,513 / 221,766 ≈ **2.54**
- 对比：v3 全量 JJP 样本此前测得候选 yield ~4.9%（bimodal）；本批次 5.82%，同一量级，
  差异来自 LHE 子集 / 分析配置差异，非问题。

## "至少一个候选组合" 的定义（依据分析器源码）

候选由 `external/TPS-Onia2MuMu`（MultiLepPAT EDAnalyzer）重建，主候选 `Pri_*`
为 `J/ψ(μμ)+J/ψ(μμ)+φ(KK)` 组合。`src/MultiLepPAT.cc` 的
`evaluatePriCandidateDiagnostics()`：

```
fitPass    = priFitValid && (PriVtxProbCut_ < 0.0 || priVtxProb >= PriVtxProbCut_)
assocPVPass = 6 个末态子（4μ+2K）共用同一关联 PV
passAny    = fitPass || assocPVPass
```

主候选**仅在 `passAny` 为真时入库**（builder 中 `if (!passAny) continue;`）。
因此 `X_data` 的 `Pri_passAny` vector **非空** ⇔ 该事例含 ≥1 个通过重建判据的候选组合。
X_config 记录 `AnalysisMode = JpsiJpsiPhi`、`PriRequireCommonAssocPV=True`、
`PriRequireTrackPVCompatibility=True`、`JpsiDecayVtxProbCut=5e-4`、
`PriVtxProbCut=-1.0`（禁用）。

## 统计脚本

`tools/count_ntuple_candidates.py`：读 workspace 的 `ntuple_tasks.json`
（权威 URL 列表），经 xrootd + proxy 并行（默认 16 进程）逐文件用 ROOT 统计
`events`、`cand_events`（`Pri_passAny` 非空）、`n_cand`，输出 JSON 汇总。

```bash
export X509_USER_PROXY=<handoff>/proxy
python3 tools/count_ntuple_candidates.py \
    --ntuple-tasks <run>/tasks/JJP_DPS1/ntuple_tasks.json \
    --jobs 16 --out ntuple_candidate_count.json
```

结果存 IHEP：`<run>/logs/ntuple_candidate_count_20260829T222250.json`
（888/888 文件读取成功，0 失败）。

## 产物与存储

`/store/user/chiw/JpsiJpsiPhi_MC_Production_v4_4_full/output/JJP_DPS1/`：

- 888 merged MiniAOD（样本 276 MB，~245 GB）+ `merge_manifest`
- 888 ntuple ROOT（样本 43.8 MB，~39 GB）+ `split_ntuple_manifest`
- 4439 个 block MiniAOD 已由 `finalize-split --apply` 删除（回收 ~300 GB）；
  block 目录残留孤儿 `processing_manifest` sidecar（~15 MB，未清）

## 经验与坑（回填）

- `finalize-split --apply` **必须带 `X509_USER_PROXY`**；不带时 `xrdfs` 用本地
  `wangchi` 身份删 `cmsuser` 属主的目录 → `Operation not permitted`。
- 长审计/统计一律在 IHEP lxlogin 上 detached（`setsid nohup`）跑 + 轮询日志；
  ssh config 在 AFS 上，AFS token 过期会致 `ihep` 别名解析失败（`aklog` 修复）。
