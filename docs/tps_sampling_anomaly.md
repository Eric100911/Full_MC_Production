# TPS 样本统计随机性异常（J/psi η 分层）

> **状态**：调查进行中（2026-08-27）。
> **范围**：仅 `JJP_TPS_MC_v4_1`（cap700 轮，输出前缀 `JpsiJpsiPhi_MC_Production_v4_2_full`）。
> 不涉及 CS/G（SPS_CS/SPS_G/DPS2_CS/DPS2_G）与 DPS1。

---

## 1. 背景

TPS（`J/psi + J/psi + phi`）全规模生产原计划 **4829 个 merge 组**（24142 个 MIX 块），
但在 07/29 被 `SYSTEM_PERIODIC_REMOVE` 中止，只完成了 **~24%**：

| 指标 | 计划 | EOS 实际（2026-08-26） | 完成率 |
|------|------|------------------------|--------|
| MIX block | 24,142 | 7,352 | 30.5% |
| MERGE 组 | 4,829 | **1,145** | **23.7%** |
| NTUPLE | 4,829 | **1,145** | **23.7%** |

已产出的 1,145 个 merge 组内部完整（每个 merge 目录都有 `output_MINIAOD.root` +
`output_ntuple.root`，merge==ntuple 齐套，无孤儿）。

**本文讨论的不是"完成率"，而是"已产出的这 ~4.8M 事例，统计上是否随机可代表全量"。**

---

## 2. 核心现象（一句话）

对已产出的 ~4.8M 事例做 J/psi 赝快度（η）检查，发现样本**不是均匀随机的**：
约一半 merge 的 J/psi η 分布是对称的（HIGH 群），另一半的 J/psi η 被截断在正半轴
[0, 2.5]、**完全缺失负 η 半球**（LOW 群）。异常是 **J/psi 特有**的——phi 在两个群里都对称。

---

## 3. 关键定义（先读这一节）

### 3.1 merge 组 / component 块

- **component 块（block）**：一个 MIX 处理节点的输出，编号 `JOB<job>_BLOCK<idx>`，
  对应 EOS 目录 `.../JJP_TPS_MC_v4_1/JOB0000XX_BLOCK0000XX/`（含 `output_MINIAOD.root`）。
- **merge 组（merged MiniAOD）**：把 **5 个连续的 component 块** 合并成一个 merged MiniAOD，
  编号 `JOB<job>_MERGE<idx>`，对应目录 `.../JOB0000XX_MERGE0000XX/`
  （含 `output_MINIAOD.root` + `output_ntuple.root` + `merge_manifest_*.json`）。

**固定映射**（已核实）：`MERGE<idx>` 由 `BLOCK[5·idx]` 到 `BLOCK[5·idx+4]` 组成。例如：

| merge | 5 个 component 块 |
|-------|-------------------|
| `JOB000000_MERGE000000` | `BLOCK000000`–`BLOCK000004` |
| `JOB000006_MERGE000005` | `BLOCK000025`–`BLOCK000029` |
| `JOB000006_MERGE000043` | `BLOCK000215`–`BLOCK000219` |
| `JOB000006_MERGE000233` | `BLOCK001165`–`BLOCK001169` |

### 3.2 HIGH 群 / LOW 群（精确判据）

按单个 merge 的 **merged MiniAOD 事件数** 二分（阈值 ~4400，两群之间有空隙）：

| 判据 | **HIGH 群** | **LOW 群** |
|------|-------------|------------|
| merged 事件数 | **≥ 4400**（实测 4620–4670） | **< 4400**（实测 3940–4310） |
| 每 component 块事件数 | ~930 | ~812 |
| 生成级 status-44 J/psi η | 对称，铺满 [-3.85, 4.70] | **截到 [0, 2.5]，无负 η** |
| 重建级 `SingleJpsi_eta` | mean ≈ 0，std ≈ 1.28 | mean ≈ +1.2，堆在 +1.5~2.0 |
| 生成级 status-44 J/psi 数/事件 | ~1.0 | ~0.54 |
| 生成级 J/psi（全 status）数/事件 | ~10.5 | ~4.2 |
| JJP 复合 candidate 产出 | ~6.5% | ~3.4% |

两群在**每个 job 内部交错出现**，且**在时间上也交错**（见 §5），不是 job 层面、也不是批次层面的差异。

### 3.3 生成级 status-44 J/psi

ntuple 的 `MC_GenPart_*` 分支存的是 `prunedGenParticles`。J/psi = `pdgId == 443`。
其 `MC_GenPart_status` 分布为：

```
status 44 : 49.8%   ← 本文所用的"主 J/psi"
status 2  : 29.4%
status 23 : 19.7%
status 52 : 1.1%
（无 status 22）
```

本文用 **`pdgId==443 且 status==44`** 作为"生成级主 J/psi"，每个事件取第一个，
再用 `MC_GenPart_eta` / `MC_GenPart_pz` 看其分布。HIGH 群每事件恰有 ~1 个 status-44 J/psi；
LOW 群每事件只有 ~0.54 个——即 LOW 群缺了约一半的"主 J/psi"。

### 3.4 JJP 复合 candidate（"存在 candidate"的含义）

ntuple 树 `mkcands/X_data` 里，最终被重建出来的信号候选是三条**锯齿数组**（jagged）分支：

- `Jpsi_1_pt`、`Jpsi_2_pt`、`Phi_pt` —— 三个都非空（长度 > 0）才算"存在完整 JJP 候选"。

单目标候选（single-object）另有标量计数分支：`nSingleJpsiCand`、`nSinglePhiCand`、
`nSingleUpsCand`（TPS 无 Upsilon，恒 0）。

### 3.5 η（赝快度）与 y（快度）

- **η = pseudo-rapidity**：分支名 `*_eta`（生成级 `MC_GenPart_eta`，重建级 `SingleJpsi_eta` / `SinglePhi_eta`）。
- **y = rapidity**：分支名 `*_y`（重建级 `SingleJpsi_y` / `SinglePhi_y`）。

二者结论一致（见 §4）。本文"η 分层"泛指赝快度/快度上的非随机。

### 3.6 重要坑：ntuple 多 cycle

被重跑过的 ntuple 文件含多个 cycle（`mkcands/X_data;1`、`;2`、…）。**必须取最新 cycle**
（`;N` 最大的那个），否则会读到旧的半截写。判据：

```python
cycles = [k for k in f['mkcands'].keys() if 'X_data' in k]
latest = sorted(cycles, key=lambda k: int(k.split(';')[-1]))[-1]
t = f['mkcands/' + latest]
```

---

## 4. 具体现象（数据证据）

### 4.1 η 分布（核心证据）

**生成级 status-44 J/psi η，单文件对比**（`MC_GenPart_eta`）：

| | HIGH `JOB000000_MERGE000000` | LOW `JOB000006_MERGE000005` |
|---|---|---|
| merged 事件数 | 4649 | 4062 |
| status-44 J/psi 数 | 4649（1.0/事件） | 2179（0.54/事件） |
| **η 范围** | **[-3.85, 4.70]** | **[-0.13, 2.49]** |
| η mean | +0.01 | **+1.15** |
| corr(事件序, η) | −0.001 | +0.055 |

LOW 群的生成级 J/psi η 最小值 ≈ −0.13，即**完全没有负 η 的 J/psi**。

**重建级 η，31 文件聚合**（`SingleJpsi_eta` / `SinglePhi_eta`）：

| 量 | 群 | N | mean | std | 判定 |
|----|----|---|------|-----|------|
| `SingleJpsi_eta` | HIGH | 30337 | **−0.004** | 1.284 | 对称，铺满 [-2.5, 2.5] |
| `SingleJpsi_eta` | LOW | 32069 | **+1.226** | 0.686 | 正偏，堆 +1.5~2.0 |
| `SingleJpsi_y` | HIGH / LOW | — | −0.003 / +1.180 | 1.247 / 0.665 | 同 η |
| `SinglePhi_eta` | HIGH / LOW | 189424 / 209179 | +0.083 / +0.104 | ~1.37 | **两群都对称** |
| `SinglePhi_y` | HIGH / LOW | — | +0.081 / +0.101 | ~1.32 | **两群都对称** |

> 合并的 `SingleJpsi_eta` mean ≈ +0.63，**完全由 LOW 群拉偏**；HIGH 群单独看是干净的。

### 4.2 事件数双峰（对应 shower 效率）

| 群 | 每块事件数 | 对应 shower 效率（见 `docs/capacity_plan_jjp_full_scale.md`） |
|----|-----------|----------------------------------------------------------------|
| HIGH | ~930 | ≈ normal（0.933） |
| LOW | ~812 | 低一档（~0.81） |

TPS 的 3 个 source 结构（见备份的 COORD config，§7.2）：

```text
shower_modes     = ['normal', 'normal', 'phi_mpi_off']
campaign_inputs  = ["pool_jpsi_CSCO_g", "pool_jpsi_CSCO_g", "pool_gg"]
source_lhe_budgets = [1000, 1000, 977]
```

即 **2 个 J/psi 源（normal）+ 1 个 phi 源（phi_mpi_off）**。

### 4.3 生成级 J/psi 每事件数（2.5× 差异）

| 群 | gen J/psi（全 status）/事件 | gen phi/事件 |
|----|----------------------------|--------------|
| HIGH | **10.53** | 3.04 |
| LOW | **4.17** | 2.75 |

LOW 群的生成级 J/psi 只有 HIGH 的 ~40%，而 phi 差不多——LOW 群像是**缺了一块 J/psi 源**。

### 4.4 candidate 产出

| 判据 | 合并 | HIGH | LOW |
|------|------|------|-----|
| JJP 复合 candidate（`Jpsi_1`+`Jpsi_2`+`Phi` 非空） | ~4.9% | ~6.5% | ~3.4% |
| ≥1 个 single J/psi（`nSingleJpsiCand>0`） | ~40.6% | ~46% | ~36% |

---

## 5. 已排除的假设

| 假设 | 结论 | 证据 |
|------|------|------|
| 生产批次差异 | ❌ 排除 | HIGH/LOW 在时间上交错：07-28 有 6 HIGH + 4 LOW，08-02 有 3+3。日期直方图见 §9 |
| merge 缺块 | ❌ 排除 | LOW merge 有完整 5 个 component（`BLOCK000025`–`000029`） |
| job 层面差异 | ❌ 排除 | 同一 job（job_6）内 HIGH/LOW 交错 |
| 单调 pz 排序 | ❌ 排除 | corr(事件序, η)≈+0.05（弱）；形态是"η 分层后只取到正 η 半边"，不是单调排序 |

merge 目录 mtime 日期直方图（8 个日期、4 个簇，各簇内 HIGH/LOW 交错）：

```
07-27 (80)  07-28 (336)  |  08-02 (201) 08-03 (128)  |  08-05 (53) 08-06 (110)  |  08-08 (186) 08-09 (51)
```

---

## 6. 待确认的根因假设

块级 η 分层 + 事件数双峰，最可能是 **shuffle-split 分层 / 块→merge 的 source 组合**，
确定性、和批次无关。两个方向：

1. **`lhe_generation/lhe_shuffle_split.cc`** 的 "stratified shuffle" 是否按 η/pz 分层，
   导致块内 η 不随机（块成了"正 η 块" / "负 η 块"）；
2. **`tools/coordinate_lhe_blocks.py`** 是否把同 η 区间的块拼进了同一个 merge。

---

## 7. 涉及文件

### 7.1 代码（按嫌疑排序）

| 文件 | 要查什么 |
|------|----------|
| `lhe_generation/lhe_shuffle_split.cc` | **首要嫌疑**：stratified shuffle 的 strata 变量、shuffle 是否真随机化 |
| `tools/plan_lhe_blocks.py` | 调用 shuffle-split 的参数/seed/分层方式、块如何 stage |
| `tools/coordinate_lhe_blocks.py` | 块→source→merge 映射（`MERGE N = BLOCK[5N]~[5N+4]` 在此决定） |
| `tools/allocate_campaign_shards.py` | shard 分配（TPS 24142 节点 / 13 shard / 3 slot） |
| `processing/pythia_shower/event_mixer_multisource.cc` | 3 source 如何混合成一块 |
| `processing/pythia_shower/shower_normal.cc` / `shower_phi.cc` / `shower_sps.cc` | normal vs phi_mpi_off 的效率差异 |
| `processing/run_chain.sh` | worker 处理链、块事件数如何定 |
| `processing/condor_wrappers/run_miniaod_merge.sh` | 5 块合并逻辑 |
| `dag_generator.py` | campaign/source/mode 定义（`shower_modes`、`canonical_mode()`、`--phi-consumption-mode`） |
| `common/cmssw_configs/ntuple_jjp_cfg.py` | ntuple 树 `mkcands/X_data` 的 candidate 分支定义 |

### 7.2 数据 / 配置

| 文件 | 说明 |
|------|------|
| `generated/JpsiJpsiPhi_MC_Production_v4_2_full/coordinator_recovery_backup_20260727T0540Z.tar.gz` | 13 个 `COORD_JJP_TPS_MC_v4_1_{0..12}.json`，内嵌 `shower_modes`/`campaign_inputs`/`source_lhe_budgets`/`source_manifests`（1900 条）——重建块→source 映射的关键 |
| `generated/JpsiJpsiPhi_MC_Production_v4_2_full/plan_subdags/campaign_shard_allocation.json` | shard 分配（`planned_blocks_by_slot=[25555,25622,26876]`） |
| `common/node_config_defaults.json` | EOS 存储配置 |

### 7.3 文档

| 文件 | 关联点 |
|------|--------|
| `CLAUDE.md` | "Keep shuffling deterministic"、shuffle-split、block SubDAG 不变量 |
| `docs/capacity_plan_jjp_full_scale.md` | shower 效率表（normal 0.933 / pool_jpsi_CSCO_g phi_mpi_off 0.802 / pool_gg phi 0.909） |
| `docs/tps_ihep_merge_cutover_plan.md` | TPS 切分转接历史（批次时间线） |
| `docs/directory_path_reference.md` | EOS 路径 / sidecar 位置 |
| `docs/testing.md` | 测试流程 |
| `skills/cms-continuous-production/references/production-ops.md` | sidecar schema（merge_manifest / processing_manifest 字段） |

---

## 8. 如何复现（验证命令）

### 8.1 前置

```bash
export X509_USER_PROXY=/afs/cern.ch/user/c/chiw/condor/x509up   # 或 IHEP 侧的 proxy
# 需要 uproot + fsspec-xrootd
```

### 8.2 读一个 merge 的 η 分布（判定 HIGH/LOW）

```python
import uproot, numpy as np
url = 'root://cceos.ihep.ac.cn:1094///store/user/chiw/JpsiJpsiPhi_MC_Production_v4_2_full/output/JJP_TPS_MC_v4_1/JOB000006_MERGE000005/output_ntuple.root'
f = uproot.open(url)
latest = sorted([k for k in f['mkcands'].keys() if 'X_data' in k], key=lambda k:int(k.split(';')[-1]))[-1]
t = f['mkcands/'+latest]
pdg = t['MC_GenPart_pdgId'].array(library='np')
eta = t['MC_GenPart_eta'].array(library='np')
st  = t['MC_GenPart_status'].array(library='np')
pz  = t['MC_GenPart_pz'].array(library='np')
etas = []
for p,s,e,z in zip(pdg,st,eta,pz):
    m = (p==443)&(s==44)
    if m.any(): etas.append(e[m][0])
etas = np.array(etas)
print('merged events =', t.num_entries)
print('status-44 J/psi =', len(etas), '=', len(etas)/t.num_entries, '/event')
print('eta min/max/mean =', etas.min(), etas.max(), etas.mean())
```

**判据**：`eta.min()` 若 ≈ 0（如 −0.13）→ LOW 群；若 ≈ −3 以下 → HIGH 群。

### 8.3 读 merge_manifest（看 5 个 component + 事件数）

```bash
xrdfs root://cceos.ihep.ac.cn:1094/ cat \
  /store/user/chiw/JpsiJpsiPhi_MC_Production_v4_2_full/output/JJP_TPS_MC_v4_1/JOB000006_MERGE000005/merge_manifest_JJP_TPS_MC_v4_1_JOB000006_MERGE000005.json
# 字段：actual_events, components[].job_id（5 个 BLOCK id）
```

### 8.4 merge 目录 mtime 直方图（排除批次假设用）

```bash
xrdfs root://cceos.ihep.ac.cn:1094/ ls -l \
  /store/user/chiw/JpsiJpsiPhi_MC_Production_v4_2_full/output/JJP_TPS_MC_v4_1/ \
  | grep '_MERGE' | awk '{print $5}' | sort | uniq -c
```

---

## 9. EOS 数据源

- 根：`root://cceos.ihep.ac.cn:1094///store/user/chiw/JpsiJpsiPhi_MC_Production_v4_2_full/output/JJP_TPS_MC_v4_1/`
- block 目录：`.../JOB<job>_BLOCK<idx>/output_MINIAOD.root`
- merge 目录：`.../JOB<job>_MERGE<idx>/`（`output_MINIAOD.root` + `output_ntuple.root` + `merge_manifest_*.json`）
- LHE 源块：`.../lhe_pools/{pool_gg,pool_jpsi_CSCO_g}/lhe_blocks/`（各 950 组，共 1900）

> EOS 批量查询/遍历建议走 IHEP lxlogin（见记忆 `ihep-eos-query-fast`），比从 lxplus 直查快一个量级。

---

## 10. 2026-08-27 调查结论与防复发修复

### 10.1 根因定位

异常在 shower、mixer、merge 和 ntuple 之前已经存在。对 provenance 追溯到的
代表性原始 LHE block 直接统计 outgoing quarkonium 的 `pz` 符号，结果为：

| 样本 | 来源 | `+pz` | `-pz` | 绝对不对称度 |
|------|------|------:|------:|-------------:|
| LOW | `pool_jpsi_CSCO_g` seed 1322, block 43 | 1000 | 0 | 1.000 |
| HIGH | `pool_jpsi_CSCO_g` seed 133, block 13 | 481 | 519 | 0.038 |

LOW 的 processing sidecar 同时显示两个 normal J/psi source 都实际参与了处理；因此不是
TPS 丢失一个 J/psi source。LOW 中较多的 `pythia.next()` failure 会降低最终共同事件数，
但不能制造输入中已经存在的单边 `pz` 分布。

代码复核还确认：

- `lhe_shuffle_split.cc` 不读取 `eta` 或 `pz`；当前 stratified 模式按原始事件序号分层，
  不可能把一个对称输入变成全正 `pz`；
- coordinator 对重复 pool 使用不同、连续的 block，未把同一 block 重复喂给两个 slot；
- 连续 block 分配和每 5 个 processing block 合并会让同一坏 seed 的异常集中出现，
  是放大因素，不是源头。

因此，现有证据支持的直接根因是：`MC_Production_v3` 复用 inventory 中至少一个历史
`pool_jpsi_CSCO_g` LHE source family（已确认 seed 1322）本身纵向单边。历史 HELAC /
CrystalBall 生成时究竟是哪一项配置造成该 source family 单边，现有 LHE header 不足以
唯一还原，不能在缺少证据时归因到某个具体参数。

### 10.2 已实现的保护

planner 现在会在 shuffle/split、stageout 和 existing-block reuse **之前**，对每个输入
LHE 文件独立审计目标 quarkonium 的 longitudinal momentum：

- quarkonium pool 默认 `policy=fail`；`pool_gg` 等无关 pool 不启用；
- 统计 outgoing J/psi/Upsilon 及仓库使用的相应 octet PDG ID；
- 每文件至少 500 个目标粒子后，要求
  `abs(N(+pz)-N(-pz))/(N(+pz)+N(-pz)) <= 0.20`；
- 一个文件失败就拒绝整个 planner group，防止好文件在 aggregate 中稀释坏文件；
- 足够多事件却没有预期粒子也 fail-closed；小于 500 个目标粒子的测试输入记录为
  `insufficient_statistics`，不以低统计涨落阻断小 pilot；
- 无论成功或失败都写出 `<plan_manifest>.input_symmetry.json`；成功 plan manifest 也内嵌
  同一审计结果。

回归测试覆盖对称输入、全正输入、好坏文件混组、错误 PDG 内容、小统计输入，以及确认
失败发生在 fake splitter 被调用之前。

### 10.3 现有数据处置

这个修复防止异常 LHE 再进入新 DAG，但不会改变已经生成的 ROOT 文件。现有 LOW merge
不应通过对 `eta`/`pz` 做随机镜像来“修复”：这会制造未生成事件，并改变样本归一化和
beam-side 相关性。生产恢复应先对 authoritative inventory 逐文件运行同一审计，隔离失败
source（至少 seed 1322），从经过验证的对称 LHE 重新规划/生成受影响 block 和 merge；
已有 HIGH 样本也应按 provenance 审计通过后再保留。
