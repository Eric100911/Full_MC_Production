# DPS1 single-J/psi LHE 纵向单边影响核查：新会话交接

> 更新：2026-08-27。目标：对已经产出的 `JJP_DPS1_MC_v4_1`
> processing block、merge 和 ntuple 做只读 provenance 审计，精确计算历史
> `pool_jpsi_CSCO_g` 单边 LHE 对 DPS1 样本的影响。TPS 根因背景见
> `docs/tps_lhe_rapidity_followup.md` 和 `docs/tps_sampling_anomaly.md`。

## 1. 当前已确认事实

IHEP 上确实已有 DPS1 输出，旧文档中“DPS1 尚未运行”的状态已经过时：

```text
root://cceos.ihep.ac.cn:1094///store/user/chiw/JpsiJpsiPhi_MC_Production_v4_2_full/output/JJP_DPS1_MC_v4_1/
```

2026-08-27 查询父目录时得到：

| campaign | `xrdfs ls -l` 报告大小 | mtime |
|---|---:|---|
| `JJP_DPS1_MC_v4_1` | 236,559,247,133 bytes | 2026-08-19 00:34:24 |
| `JJP_TPS_MC_v4_1` | 826,767,270,387 bytes | 2026-08-09 03:59:19 |

目录大小不能替代事件清点；它只能证明 DPS1 已经在 8 月 13 日旧容量文档之后继续生产。

`pool_jpsi_CSCO_g` 的 authoritative counted inventory 是：

```text
generated/lhe_inventory_jjp_20260717_184812.json
sha256 9c393a706aa3ad4368a3ca3c10a822cdd65f7c4daee86950df3e4e9044369e68
```

其中：

| source seed | 文件数 | counted LHE events | 当前判断 |
|---|---:|---:|---|
| `100-1099` | 1000 | 88,688,717 | 好批次候选；仍须逐文件审计 authoritative 副本 |
| `1100-2099` | 1000 | 52,723,831 | 暂定整批隔离；抽查均为全正 `pz` |
| 合计 | 2000 | 141,412,548 | — |

已经完整扫描的坏文件包括 seed 1227、1322、1357、1585、1659、1674、1811、
1969；另一次跨区间抽查的 1100、1150、1200、1300、1400、1500、1600、1700、
1800、1900、2000、2099 也全部满足 `N(-pz)=0`。seed 1227 的上游未压缩文件与
authoritative 压缩副本解压后的 SHA-256 完全相同。

当前 xcheng 路径中的 seed `100-1099` 在 2026-06-15 被重新生成/覆盖，事件数与此前
转存到 chiw authoritative pool 的副本不一致。新文件的对称性可以验证新配置，但不能
代替对 chiw authoritative 副本的审计。

## 2. 本次核查必须回答的问题

1. EOS 上实际存在多少 DPS1 `BLOCK`、`MERGE` 和 `NTUPLE` 输出？
2. 每个 processing block 的两个 `pool_jpsi_CSCO_g` source slot 分别来自哪个原始
   LHE 文件、seed、group 和 block index？
3. 两个 source 都好、仅 slot 0 坏、仅 slot 1 坏、两个都坏的 processing blocks
   各有多少，实际最终事件数各是多少？
4. 哪些 merge 包含至少一个坏 component；相应 ntuple 是否存在？
5. 有多少输出缺少、冲突或无法闭合 provenance，因而必须 fail closed？
6. 可保留和必须重跑的实际事件数、文件数、字节数分别是多少？

最终报告必须使用实际 manifest 中的 `actual_events` 或 ROOT event count，不能从文件名、
目录大小或文件大小推断事件数。

## 3. 判定规则

优先使用逐文件 symmetry audit 生成的明确 good/bad source 清单。完整扫描尚未完成时，
可使用以下保守暂定规则做第一版影响清点：

```text
suspect source seed = 1100..2099
candidate-good seed = 100..1099
```

正式规则：

```text
bad processing block
    = source slot 0 或 source slot 1 最终解析到 bad LHE source
    = 任一 source provenance 缺失、冲突或无法解析

bad merge
    = 任一 component processing block 为 bad
    = component 集合或事件数与 merge manifest 不闭合

bad ntuple
    = 其 parent merge 为 bad
    = parent provenance 不明确
```

DPS1 的两个输入都来自同一个 pool，但必须消费不同、非重叠的确定性 blocks。不要把两个
slot 合并成一个 pool 级标签，也不要假定二者使用同一个 seed。

不能用这些量直接判定 source 好坏：

- HIGH/LOW 最终事件数；
- shower efficiency 或 `pythia.next()` failure 数；
- ROOT 中事后观察到的正负 `eta` 比例；
- LHE 文件大小或 output 目录大小。

这些量只用于交叉验证，不替代 source provenance。

## 4. 只读检查准备

所有完整 XRootD URL 保留显式 endpoint 和三斜杠：

```bash
export DPS1_ENDPOINT='root://cceos.ihep.ac.cn:1094/'
export DPS1_OUTPUT_LFN='/store/user/chiw/JpsiJpsiPhi_MC_Production_v4_2_full/output/JJP_DPS1_MC_v4_1'
export DPS1_OUTPUT_URL='root://cceos.ihep.ac.cn:1094///store/user/chiw/JpsiJpsiPhi_MC_Production_v4_2_full/output/JJP_DPS1_MC_v4_1'
export DPS1_AUDIT_DIR='/tmp/chiw/dps1_lhe_rapidity_audit'
mkdir -p "${DPS1_AUDIT_DIR}"

voms-proxy-info --timeleft
command -v xrdfs
command -v xrdcp
command -v jq
```

受限 Codex sandbox 可能使 XRootD 报 `[FATAL] Invalid address`。遇到这种情况，应在带同一
proxy 的正常 CERN/IHEP shell 或批准的非 sandbox 命令中重现；不要改写 storage URL。

本轮是只读核查。不要删除、覆盖、重提交或移动 EOS 文件。

## 5. 第一阶段：清点远端输出

先保存顶层 listing，并分别计算 BLOCK/MERGE 数量：

```bash
xrdfs "${DPS1_ENDPOINT}" ls -l "${DPS1_OUTPUT_LFN}" \
  > "${DPS1_AUDIT_DIR}/dps1_top_level_ls_l.txt"

awk '/_BLOCK[0-9]+/ {n++} END {print n+0}' \
  "${DPS1_AUDIT_DIR}/dps1_top_level_ls_l.txt"
awk '/_MERGE[0-9]+/ {n++} END {print n+0}' \
  "${DPS1_AUDIT_DIR}/dps1_top_level_ls_l.txt"
```

确认命名范围、job 分布和是否有异常目录：

```bash
awk '{print $NF}' "${DPS1_AUDIT_DIR}/dps1_top_level_ls_l.txt" \
  | sed 's#.*/##' \
  | sort \
  | head -n 40

awk '{print $NF}' "${DPS1_AUDIT_DIR}/dps1_top_level_ls_l.txt" \
  | sed 's#.*/##' \
  | sort \
  | tail -n 40
```

如需完整 sidecar 路径清单，在 IHEP shell 上执行递归 listing：

```bash
xrdfs "${DPS1_ENDPOINT}" ls -R "${DPS1_OUTPUT_LFN}" \
  > "${DPS1_AUDIT_DIR}/dps1_recursive_listing.txt"

rg 'processing_manifest_.*\.json$' \
  "${DPS1_AUDIT_DIR}/dps1_recursive_listing.txt" \
  > "${DPS1_AUDIT_DIR}/processing_manifest_lfns.txt"

rg 'merge_manifest_.*\.json$' \
  "${DPS1_AUDIT_DIR}/dps1_recursive_listing.txt" \
  > "${DPS1_AUDIT_DIR}/merge_manifest_lfns.txt"

rg '/output_ntuple\.root$' \
  "${DPS1_AUDIT_DIR}/dps1_recursive_listing.txt" \
  > "${DPS1_AUDIT_DIR}/ntuple_lfns.txt"

wc -l "${DPS1_AUDIT_DIR}"/*_lfns.txt
```

## 6. 第二阶段：先确认 manifest schema

不要先假定 JSON 字段名。选择一个 BLOCK 和一个 MERGE，读取完整 sidecar：

```bash
sample_processing_lfn=$(head -n 1 "${DPS1_AUDIT_DIR}/processing_manifest_lfns.txt")
sample_merge_lfn=$(head -n 1 "${DPS1_AUDIT_DIR}/merge_manifest_lfns.txt")

xrdfs "${DPS1_ENDPOINT}" cat "${sample_processing_lfn}" \
  | jq . \
  > "${DPS1_AUDIT_DIR}/sample_processing_manifest.json"

xrdfs "${DPS1_ENDPOINT}" cat "${sample_merge_lfn}" \
  | jq . \
  > "${DPS1_AUDIT_DIR}/sample_merge_manifest.json"

jq 'paths | map(tostring) | join(".")' \
  "${DPS1_AUDIT_DIR}/sample_processing_manifest.json" \
  | sort -u
```

在 processing sidecar 中确认并记录：

- campaign、job ID、processing block ID；
- 两个 source slot 的 pool 和 shower mode；
- 每个 slot 使用的 block URL/path、group ID、primary seed、source-job index 和 block index；
- attempted LHE、accepted HepMC、mixer consumption、actual output events；
- manifest 自身的成功/完整性状态。

如果 processing sidecar 没有直接给出原始 seed，则按下面的链解析：

```text
processing source block URL
  -> staged LHE block manifest / plan manifest
  -> group_id / primary_seed / selected source paths
  -> authoritative original LHE file
```

不要仅从最终 `JOBxxxxxx_BLOCKxxxxxx` output ID 推断 source seed；output ID 和 source seed
不是同一编号空间。

## 7. 第三阶段：建立机器可读审计表

新会话应优先实现一个只读脚本，而不是依赖大量 ad-hoc `jq`。建议脚本接口：

```bash
python3 tools/audit_dps1_lhe_provenance.py \
  --endpoint root://cceos.ihep.ac.cn:1094/ \
  --output-lfn "${DPS1_OUTPUT_LFN}" \
  --recursive-listing "${DPS1_AUDIT_DIR}/dps1_recursive_listing.txt" \
  --inventory generated/lhe_inventory_jjp_20260717_184812.json \
  --inventory-sha256 9c393a706aa3ad4368a3ca3c10a822cdd65f7c4daee86950df3e4e9044369e68 \
  --provisional-bad-seed-range 1100:2099 \
  --cache-dir "${DPS1_AUDIT_DIR}/manifest_cache" \
  --json-output "${DPS1_AUDIT_DIR}/dps1_lhe_impact.json" \
  --processing-csv "${DPS1_AUDIT_DIR}/dps1_processing_impact.csv" \
  --merge-csv "${DPS1_AUDIT_DIR}/dps1_merge_impact.csv"
```

如果该脚本尚不存在，应在新会话中先检查仓库是否已有可复用的 manifest fetch/cache
helper，再实现和测试。远端读取必须支持本地 cache 和可恢复重跑，避免每次重新下载全部
sidecars。

processing CSV 至少包含：

```text
campaign,job_id,processing_block_id,actual_events,
slot0_pool,slot0_mode,slot0_seed,slot0_group_id,slot0_block_index,slot0_source_path,slot0_verdict,
slot1_pool,slot1_mode,slot1_seed,slot1_group_id,slot1_block_index,slot1_source_path,slot1_verdict,
processing_verdict,reason,processing_manifest_lfn
```

merge CSV 至少包含：

```text
campaign,merge_id,actual_events,component_count,
bad_component_count,unknown_component_count,merge_verdict,
ntuple_exists,merge_manifest_lfn
```

每个 verdict 使用明确枚举，例如：

```text
good
bad_source
missing_provenance
conflicting_provenance
missing_manifest
invalid_manifest
```

除 `good` 外都不得进入保留集合。

## 8. 本地规划证据和交叉检查

现有规划文件可帮助恢复预期 source/block 顺序，但不能覆盖实际 sidecar：

```text
generated/JpsiJpsiPhi_MC_Production_v4_2_full/plan_subdags/campaign_shard_allocation.json
generated/JpsiJpsiPhi_MC_Production_v4_2_full/plan_subdags/JJP_DPS1_MC_v4_1/source_manifests.json
generated/JpsiJpsiPhi_MC_Production_v4_2_full/plan_subdags/JJP_DPS1_MC_v4_1/job_*/coord_manifest_*.json
generated/JpsiJpsiPhi_MC_Production_v4_2_full/node_configs/coordination/COORD_JJP_DPS1_MC_v4_1_*.json
```

规划摘要：

| 项目 | 数值 |
|---|---:|
| planned processing nodes | 4,490 |
| source budgets | `[860, 1000]` |
| planned LHE slot 0 | 4,513,486 |
| planned LHE slot 1 | 4,550,112 |
| pool block cursor | approximately `51179..60765` |

核查实际 sidecar 时验证：

- repeated pool 的两个 slot 不重叠；
- source block 顺序和 coordinator 预期一致；
- 每个 merge 的 component IDs 与固定的 5-block 分组一致；
- processing、merge、ntuple 的 actual event accounting 闭合；
- 已完成但离开 live `condor_q` 的节点没有被漏算；
- 失败重跑没有造成多个 sidecar/ROOT cycle 被错误重复计数。

## 9. 最终报告必须给出的数字

至少报告以下表格：

| 层级 | 总数 | good | bad source | unknown/invalid | good events | bad events |
|---|---:|---:|---:|---:|---:|---:|
| processing blocks | | | | | | |
| merges | | | | | | |
| ntuples | | | | | | |

另列 processing source-slot 组合：

| slot 0 | slot 1 | blocks | actual events |
|---|---|---:|---:|
| good | good | | |
| bad | good | | |
| good | bad | | |
| bad | bad | | |
| unknown | any | | |

还应给出：

- 逐文件 source verdict 清单及其 checksum；
- 受影响 seed/group 分布；
- 可保留 merge/ntuple 的精确 LFN 列表；
- 必须重跑的 processing block 或 merge ID 列表；
- 若决定全量重跑，说明选择性保留能节省的比例及 provenance 复杂度；
- 报告生成时间、proxy 环境、endpoint、inventory checksum 和脚本版本/commit。

目录大小比例曾给出约 1.3-1.5M DPS1 最终事件、其中约 80-90% 可能受影响的粗估。
这些数字只用于安排核查优先级，最终报告不得引用它们作为结果。

## 10. 新会话建议启动顺序

1. 阅读本文件、`docs/tps_lhe_rapidity_followup.md` 和
   `docs/tps_sampling_anomaly.md`。
2. 确认 proxy、endpoint 和 authoritative inventory checksum。
3. 只读清点 DPS1 BLOCK/MERGE/NTUPLE，并保存递归 listing。
4. 人工查看一个 processing manifest、一个 merge manifest，确认真实 schema。
5. 检查仓库中已有 manifest fetch/cache helper；实现或复用批量 provenance audit。
6. 先跑 10 个 block 的小样本，人工核对两个 source slot 的 seed 映射。
7. 批量审计全部 DPS1 sidecars，生成 JSON/CSV 和明确的 missing/invalid 列表。
8. 用规划 source manifests 和 coordinator allocation 做独立交叉验证。
9. 抽取若干 good/bad processing outputs 检查 ROOT `eta/pz`，仅作为结果交叉验证。
10. 给出可保留/必须重跑的精确事件数和 LFN；在得到明确批准前不做 EOS mutation。

