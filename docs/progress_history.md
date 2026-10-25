# 物理研究与生产链路进展（2025-12—2026-10）

本文按时间串联物理问题、样本证据和支撑它们的代码演进。注明为**提交日期**的日期来自 Git；**运行/审计日期**来自相应记录。代码实现、已完成批次和历史配置分别表述，不把计划当作结果，也不据此推断当前生产状态。

## 2025-12—2026-01：建立 LHE 到 MiniAOD 链路并加入颜色八重态

**2025-12-23（提交）**，`20a0c9d` 建立首版 LHE→MiniAOD 工作流；**2026-01-04（提交）**，`d99134b` 完成 DAG 版本，将 HELAC LHE、Pythia shower、HepMC 混合和 CMSSW 处理组织成链路。此阶段着重把生成样本送到可分析的重建产物。

**2026-01-20（提交）**，`50e658e` 将 color-octet quarkonia 纳入考虑，扩展物理过程和对应的 pool/campaign 表达。以上是代码提交日期，不代表某一批次的完成日期；可从 [README 项目结构](../README.md) 和 Git 提交记录追溯。

## 2026-03—05：拆分物理过程并采用 CrystalBall pT 模型

**2026-03-23（提交）**，`58ffe12` 记录首版 CO MC；**2026-04-29（提交）**，`cc141e1` 拆分双 J/ψ campaign 与 pool 处理，使 SPS、DPS 和不同生成源可以分别描述、规划与核算。

**2026-05-23（提交）**，`e1b170b` 将 J/ψ 与 Υ LHE pool 切换到 CrystalBall pT 模型。之后的运行调查发现 CrystalBall addon 未读取 job seed，导致不同 seed 标签曾生成相同随机流。这个问题于 **2026-09-19（提交）**由 `931c933` 修复：addon 从 `seed.input` 初始化 RNG，相关独立性和产额检查记录为通过。这是后续的随机数修正，不应视为 5 月模型迁移时已具备的性质。详见 [CrystalBall 修复记录](crystalball_seed_fix_refactor_catchup_2026-09-19.md)。

## 2026-06：引入 LHE 分块、block SubDAG 与 MiniAOD 合并

**2026-06-09（提交）**，`1c4d165` 加入分层 shuffle 与 LHE block splitter，`e665522` 加入可跳过 LHE 生成的 block SubDAG 工作流；同日开始压缩和 XRootD 传输工具。**6 月 21 日（提交）**，`c37c0c6` 建立按组处理既有 LHE 的 block planning，`3de3b74` 加入 TPS campaign 并修正重复 pool 的 block 混用。分块让大源文件可以按确定、互不重叠的事件块供下游消费。

**2026-06-10—11（提交）**，`b2b6cf2`、`11996df` 更新 TPS-Onia2MuMu 与 ntuple 配置，并支持独立重跑 ntuple。**6 月 30 日—7 月 1 日（提交）**，`a3cc309`、`1b97b53` 增加 block SubDAG pilot 的 MiniAOD 合并收尾。代码里具备这些功能不等于所有生产配置都已完成。架构和验证入口见 [testing 文档](testing.md) 与 [README 的 CERN/IHEP 分段流程](../README.md#cern-mix--ihep-mergentuple-split)。

## 2026-07：用计数库存和事件 ID 约束规划

**2026-07-18（提交）**，`1cddfa2` 加入并行 LHE 库存计数，`0250b99` 让 block 规划使用库存中的实际事件数；**7 月 19 日（提交）**，`49e0591` 加入效率感知的 block 处理和确定性 EDM EventID。规划由文件数或文件名估算转向使用计数事件，并把输入块、处理效率、最终事件和来源连接起来。规模估算见 [JJP capacity plan](capacity_plan_jjp_full_scale.md)；这是基于指定清点和 pilot 效率的规划，不表示六个 campaign 均已完成。

## 2026-08：完成有明确边界的 DPS1 重跑，拆分 CERN 与 IHEP 阶段

**2026-08-05（提交）**，`56b78f6` 实现 CERN MIX 与 IHEP merge/ntuple 分段，随后补充 split 审计、清单冻结和重试保护。

**2026-08-28 提交、8 月 29 日全部完成（运行记录）**的 v4_4 `JJP_DPS1` good-LHE 重跑，输入为 `pool_jpsi_CSCO_g` seeds 100–189：90 个文件、8,924,389 个计数事件。记录显示 CERN MIX 完成 4,439 个 blocks；IHEP merge 888/888、ntuple 888/888 通过 gate；MiniAOD 实际事件总数为 3,812,832。上述数值及日期均与 [v4.4 DPS1 批次总结](v44_dps1_good_rerun_summary.md) 一致。该完成记录仅适用于此批次。

## 2026-08—09：记录 rapidity 疑虑并修正 HELAC 配置

历史 `pool_2jpsi_g` 样本出现正负 `pz` 不平衡。**2026-09-16（提交）**，`6a47717` 恢复绝对 rapidity cut，并加入 HELAC infrared-safety 质量切。调查将配置问题定位为旧模板的 `absoluterap F` 配合首个末态粒子的 `0≤y≤30`：第一粒子可能因此受有符号 rapidity 区间限制。修正显式设置 `miny1c=0`、`maxy1c=30`、`absoluterap=T`，解释及适用范围见 [HELAC absolute-rapidity 说明](helac_absolute_rapidity_fix.md)。

该改动适用于使用修正后 runtime 配置**新生成的 LHE**；不会追溯修改既有 LHE inventory 或其下游产物。10 月的 payload 抽样和 seed-range 审核结果列在下文，不属于本阶段的已知结论。

## 2026-09：完成 CrystalBall seed 修复与 CERN/IHEP cutover

**2026-09-03（提交）**，`f93ace9` 实现 TPS CERN/IHEP merge cutover；**9 月 19 日（提交）**，`931c933` 修复 CrystalBall seed 输入，并将新生成 LHE 与不可变 inventory 分开。相关调查指出 `unwevt` 不能单独预测 CrystalBall 实际产额，样本规模应以逐文件计数为准。详见 [seed 修复记录](crystalball_seed_fix_refactor_catchup_2026-09-19.md)。

## 2026-10-12：六 campaign DAG 的历史配置

临时 [engineering memory](prompt-20261017-temp/MEMORY.md) 记载了一次 **2026-10-12 历史 run**：包含 `JJP_SPS_CS`、`JJP_SPS_G`、`JJP_DPS1`、`JJP_DPS2_CS`、`JJP_DPS2_G`、`JJP_TPS` 六个 campaign；该笔记所载配置为每个 campaign 1,000 个 processing nodes、每节点目标 1,000 个 mixed events、`block_events=1000`、`merge_events=5000`。这些数字来自该临时笔记对历史 run 的记录；它们描述配置和观察，不证明六个 campaign 均已完成，也不反映当前生产状态。

## 2026-10-22—25：核对库存、抽样 rapidity 与 seed 区间

**2026-10-22（库存更新记录）**，获准纳入的 `v4_5_full` `pool_2jpsi_g` 批次有 6,500 个 canonical source、6,631,194 个实际事件；另有 10 个已符合条件的 latest-stage1 source、12,722 个实际事件。合计 6,510 个文件、6,643,916 个事件。**2026-10-23（清单发布）**，跨 pool 全量清点以 whole-stream event hash 去重后为 13,326 个文件、212,732,098 个事件。获准范围仅包括上述 6,500 个 v4_5_full 文件和 10 个 stage1 来源；其他可读 cohort 仍为候选或待 provenance 复核。详见 [10 月 22 日 NLO 更新](../generated/lhe-nlo-inventory-update-20261022/README.md) 和 [10 月 23 日 consolidated inventory summary](../generated/lhe-inventory-consolidated-20261023/SUMMARY.md)。

**2026-10-23（分析日期）**，固定分层抽样的 33 个文件解析出 475,124 个 LHE 事件和 343,769 个末态 J/ψ。报告中的 `pool_2jpsi_g` v4_5_full 组是 3 个文件、2,958 个事件，A_FB = −0.0132±0.0169，反射 rapidity 检验 p=0.631；`three_stage` 组是 3 个文件、3,754 个事件，A_FB = −0.0250±0.0150、p=0.00346。后者在报告路径中标为 `archive/stage1-sources/pool_2jpsi_g`。这两组的样本结果不同，且每组只抽了三个文件；报告未据此给出全池对称性结论。数值与批次标签见[抽样审计原报告](../generated/lhe-rapidity-audit-20261023/rapidity-symmetry.md)。

报告还记录 `pool_jpsi_CSCO_g` 的一个 `nmc2000k` sweep 文件有 A_FB=+0.0279±0.0088、未校正 p=0.00424；多重检验校正后仍属探索性抽样信号。它与 `pool_2jpsi_g` 的 rapidity 问题是不同 pool 和批次，不能合并解读。所有抽样结果都限于所选文件，不能代表完整生产池。

**2026-10-25（审计日期）**，跟进确认 `pool_jpsi_CSCO_g` seeds **100–1099 可用、1100–2099 可隔离**；该范围判定依据先前审计，并非由当前 consolidated inventory 的事件计数推导。当前 inventory 实际覆盖 seeds 102–2099：其中 102–1099 为 **998 files、88,477,567 个 inventory-counted events**，1100–2099 为 1,000 files、52,723,831 个事件。因而“100–1099 可用”是范围质量结论，“当前 102–1099 有 998 个文件”是 inventory 计数，两者口径不同。结论仅限这两个 seed 区间，不外推到区间外文件；也不要与 `pool_2jpsi_g` 的抽样 rapidity 结果混淆。详见[10 月 25 日跟进](../generated/lhe-rapidity-audit-20261023/rapidity-symmetry.md)及 [DPS1 rapidity 审计交接](dps1_lhe_rapidity_audit_handoff.md)。
