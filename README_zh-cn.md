# MC Production 统一工作流

本仓库为重味物理 MC 生产生成 HTCondor DAGMan 工作流，覆盖完整链路：

`LHE(HELAC-Onia) → Pythia8 shower → HepMC mixing → CMSSW GEN-SIM → RAW → RECO → MiniAOD → Ntuple`

`lxplus_t2_ihep` profile 在 CERN lxplus 提交 HTCondor/DAGMan 作业，将 LHE/output 写入 IHEP T2（通过 XRootD）。其他 machine environment 支持 hepthu、本地 Condor 和 IHEP/lxlogin HepJob 提交。

支持两类分析：**JJP**（`J/psi + J/psi + phi`）和 **JUP**（`J/psi + Upsilon + phi`）。TPS 事例（三 J/psi）也支持 ntuple-only 重处理。

## 当前验收标准

- 代码接口保留全链路能力，包括 Ntuple 步骤。
- 小批量 HTCondor 测试默认以跑通 MiniAOD 并完成远端 stage-out 为准。
- Ntuple 步骤保留在接口中；如需执行，请确保 `external/TPS-Onia2MuMu` submodule 已初始化且指向所需的分析代码版本。
- Ntuple 的输入文件、输出文件和 `maxEvents` 通过 `cmsRun` CLI 切换；分析行为（`analysisMode`、MC truth tree、acceptance gating）固定在 `common/cmssw_configs/ntuple_*.py` 中。
- 所有程序、文件、证书统一打包上传后在 worker 解压运行——worker 运行时不再回读 AFS 业务目录。
- Worker 启动时将打包证书复制到 `/tmp/x509up_u$UID`；后续程序不引用解压目录中的本地文件。
- 使用 `dag_generator.py --machine-env ...` 选择 submit/storage profile，不再有 `VtxSmeared`、`ihep`、`hepthu` 等分支。
- `pool_jpsi_CSCO_g` 和 `pool_upsilon_CSCO_g` 使用 HELAC-Onia 的 CrystalBall pT model。

## 目录结构

- **`dag_generator.py`** — 主 CLI 入口。定义 `LHEPool`、`Campaign`、`MachineEnv` dataclass 及全部子命令（`list`、`validate`、`generate`、`generate-test`、`generate-helac-matrix`、`generate-ntuple-only`、`prepare-runtime`）。Campaign/pool 定义以 Python 字面量写在该文件中。
- **`hepjob_workflow.py`** — IHEP/lxlogin HepJob 后端适配器。生成 bash 作业脚本代替 HTCondor submit 文件。
- **`common/node_config_defaults.json`** — 集中式存储路径和 pool 目录映射。
- **`common/octet_pdg.py`** — HELAC 八重态 PDG 编码转换/扫描工具。
- **`common/compression_util.py`** / **`common/compression_helpers.sh`** — Python 和 bash 的 gzip 辅助函数，支持透明 `.lhe.gz` 处理。
- **`common/cmssw_configs/`** — CMSSW Python 配置片段（GEN-SIM 及各分析类型的 ntuple 配置）。
- **`common/node_config_defaults.json`** — 集中的存储和处理配置（EOS 主机、路径基址、pool 子目录映射）。
- **`common/paths.sh`** — 工作区相对路径定义，不硬编码用户名。
- **`common/packages/`** — 预构建 tarball：`helac_package.tar.gz`（必需）、`cmssw15_tpsonia2mumu_runtime.tar.gz`（可选）。
- **`external/TPS-Onia2MuMu`** — Git submodule，ntuple 分析器源码（v2.0_patch2）。无预编译 CMSSW15 runtime tarball 时作为回退。
- **`lhe_generation/run_helac.sh`** — Worker 端 HELAC-Onia 执行脚本。支持压缩输出、shuffle-split、block staging 和 `TARGET_EOS_BASE` 覆盖。
- **`lhe_generation/lhe_shuffle_split.cc`** — C++14 分层 LHE 洗牌与定长分块工具。在 `cmssw/el7` 容器内预编译。
- **`lhe_generation/condor_wrappers/run_lhe_gen.sh`** — LHE 作业 wrapper，使用 JSON 配置（3 个位置参数）代替旧版多参数方式。
- **`tools/plan_lhe_blocks.py`** — 单 pool LHE 分块规划器：压缩、shuffle-split、stage 分块、写出 `plan_manifest_<pool>_<seed>.json`。作为 Condor 作业在 HELAC 生成后运行。
- **`tools/coordinate_lhe_blocks.py`** — 多源 campaign 协调器：读取各 pool 的 plan manifest，按 strict-min 策略匹配分块，生成 `blocks_processing.dag` SubDAG。
- **`tools/compress_existing_lhe.py`** — 回填工具：压缩已有的未压缩 LHE pool。
- **`tools/compile_node_config.py`** — 在生产配置生成前编译并验证每个 pool 的精确 LHE 路径。
- **`tools/transfer_compress_lhe.py`** — Condor worker 脚本，用于批量 LHE 压缩及 XRootD 传输。
- **`processing/run_chain.sh`** — Worker 端处理链：shower → mix → CMSSW 步骤 → 可选 ntuple → stage-out。在 worker 上重新编译 Pythia shower 工具以避免 glibc/ABI 不兼容。
- **`processing/condor_wrappers/`** — Submit 模板调用的轻量 bash wrapper（`run_processing.sh`、`run_ntuple_only.sh`、`run_plan_lhe_blocks.sh`、`run_coordinate_lhe_blocks.sh`）。
- **`processing/pythia_shower/`** — C++ Pythia8+HepMC3 shower 工具（`shower_normal.cc`、`shower_phi.cc`、`shower_sps.cc`、`event_mixer_multisource.cc`）及 Makefile。
- **`processing/templates/`** — 各 machine environment 和 DAG 节点类型的 HTCondor submit 描述文件（包括 `plan_lhe_blocks.sub`、`coordinate_lhe_blocks.sub`、`compress.sub`、`transfer_compress.sub`）。
- **`tests/`** — Shell 测试套件：`run_all_tests.sh`（主入口）、`submit_tests.sh`（按 campaign 的 smoke DAG）、`submit_lhe_matrix.sh`（LHE pool 矩阵）、`test_lhe_shuffle_split.sh`（shuffle-split 单元测试）、`test_octet_pdg_tool.sh`（PDG 映射自检），以及 `generate_synthetic_lhe.py` 和 `check_y_symmetry.py`。
- **`docs/testing.md`** — 静态检查、本地 mock、组件测试、pilot、输出验证和清理的规范流程。
- **`docs/`** — 设计笔记、调查报告和评审文档。

## 环境准备

### 1. 代理

```bash
source /cvmfs/cms.cern.ch/cmsset_default.sh
./check_proxy.sh --init
./check_proxy.sh --status
```

lxplus 上的 DAGMan 使用 AFS 持久代理副本，确保 `condor_dagman` direct-submit 不依赖 submit host 的 `/tmp`：

```bash
export X509_USER_PROXY=/afs/cern.ch/user/c/chiw/x509up_u$(id -u)
```

### 2. 必需包

硬依赖：

- `common/packages/helac_package.tar.gz` — HELAC-Onia 与 HepMC 源码，用于 LHE 生成。

可选但推荐：

- `external/TPS-Onia2MuMu` git submodule — ntuple 分析器源码。缺失时 validate 仅警告；只有启用 ntuple 时才需要。

```bash
git submodule update --init --recursive
```

- `common/packages/cmssw15_tpsonia2mumu_runtime.tar.gz` — 预编译的 CMSSW15 runtime（ntuple 优先使用，优于源码打包回退）。

## 主 CLI 用法

所有命令共享 `--machine-env` 以选择 submit/storage profile。

### Machine environment

| 名称 | 后端 | 存储 |
|------|------|------|
| `lxplus_t2_ihep`（别名：`t2_cn_beijing`） | CERN lxplus HTCondor DAGMan | IHEP T2（XRootD） |
| `hepthu` | hepthu HTCondor DAGMan | 本地文件系统 |
| `local_condor` | 本地 HTCondor | 本地文件系统 |
| `ihep` | IHEP/lxlogin HepJob | IHEP T2（XRootD） |

`lxplus_t2_ihep` 将 MiniAOD 和 ntuple 拆分为独立 DAG 节点；`hepthu` 保持 ntuple inline 以避免跨节点本地文件访问。

### 列出可用配置

```bash
python3 dag_generator.py list --kind all
python3 dag_generator.py list --kind campaigns
python3 dag_generator.py list --kind pools
```

### 校验环境

```bash
python3 dag_generator.py validate --machine-env lxplus_t2_ihep --campaign JJP_DPS2_CS --scan-existing
python3 dag_generator.py validate --machine-env hepthu --campaign JUP_DPS1 --scan-existing
```

严格分析包检查：

```bash
python3 dag_generator.py validate --campaign JJP_DPS1 --strict-analysis-packages
```

### 生成正式 DAG

```bash
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep \
  --campaign JJP_DPS1 \
  --jobs 20 \
  --output-dir generated/jjp_dps1 \
  --output jjp_dps1.dag \
  --max-events -1
```

### 生成 smoke test DAG

```bash
# 最小 smoke test（1 作业、5 事例、禁用 ntuple）
python3 dag_generator.py generate-test \
  --machine-env lxplus_t2_ihep \
  --campaign JJP_DPS2_CS \
  --output-dir tests/generated/smoke \
  --output smoke.dag

# 带 ntuple 和 efficiency manifest
python3 dag_generator.py generate-test \
  --machine-env lxplus_t2_ihep \
  --campaign JJP_DPS1 \
  --jobs 1 --max-events 5 --enable-ntuple --efficiency-ntuple \
  --output-dir tests/generated/jjp_efficiency_smoke --output mc_test.dag

# 多 campaign smoke test
python3 dag_generator.py generate-test \
  --machine-env lxplus_t2_ihep \
  --campaign JJP_DPS2_CS --campaign JJP_DPS2_G --campaign JUP_DPS1 \
  --jobs 1 --max-events 5 \
  --output-dir tests/generated/manual_test \
  --output mc_test.dag
```

### 生成 HELAC Fock-state 矩阵 DAG

```bash
python3 dag_generator.py generate-helac-matrix \
  --output-dir generated/helac_matrix \
  --output helac_matrix.dag \
  --stageout-dir helac_matrix/jpsi_upsilon_fock_scan \
  --seed-base 92000 \
  --maxjobs-lhe 20
```

仅运行 HELAC-Onia（无下游 shower/CMSSW）。生成 162 个作业，覆盖 9 个 `cc~` Fock state、9 个 `bb~` Fock state，以及 born / `+ g` 两种过程。输出 tarball stage 至目标 EOS 基址。

### 准备 worker runtime bundle

```bash
python3 dag_generator.py prepare-runtime \
  --machine-env lxplus_t2_ihep \
  --output-dir tests/generated/runtime_bundle_check
```

生成：`lhe_runtime_bundle.tar.gz`、`processing_runtime_bundle.tar.gz`、`summary_runtime_bundle.tar.gz`、`proxy_bundle.tar.gz`。Submit 模式下 bundle 输出目录必须位于 AFS 工作区，不能使用 `/tmp`。

---

## 精确 LHE pool 路径

`common/node_config_defaults.json` 为每个已有 LHE pool 保存完全展开的
`path`。Worker 不会在运行时猜测 `LHE_pool`、`lhe_pools`、大小写或旧目录布局。

```bash
mkdir -p /tmp/chiw
python3 tools/compile_node_config.py \
  --pool-paths common/node_config_defaults.json \
  --pool pool_2jpsi_cs --pool pool_gg \
  --output /tmp/chiw/node_config_defaults.verified.json
```

验证器对选中的远端目录执行 `xrdfs ls`，并要求目录中至少有一个 `.lhe` 或
`.lhe.gz` 文件。按当前 campaign 选择 pool；尚未生产的无关 pool 不阻塞
pilot。IHEP endpoint 使用显式端口
`root://cceos.ihep.ac.cn:1094/`。

### LHE 压缩

LHE 文件可以压缩（`.lhe.gz`）或未压缩（`.lhe`）形式存储。Pool 扫描优先尝试 `.lhe.gz`，回退到 `.lhe`。HepMC 中间文件始终为纯文本以兼容 CMSSW。

```bash
# 生成时启用 LHE 压缩输出（默认 gzip 级别 1）
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_DPS1 \
  --jobs 20 --output-dir generated/jjp_dps1 --output jjp_dps1.dag \
  --compress-lhe --lhe-compression-level 3

# 回填压缩已有 LHE pool
python3 tools/compress_existing_lhe.py --pool-dir /path/to/lhe_pool --dry-run
python3 tools/compress_existing_lhe.py --pool-dir /path/to/lhe_pool --keep --level 3
```

### LHE shuffle-split

`--lhe-shuffle-split` 启用分层洗牌并将 LHE 输出切分为定长分块。原始单个 LHE 始终保留以确保向后兼容。

```bash
# 生成 1000 事例分块
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_DPS1 \
  --jobs 20 --output-dir generated/jjp_dps1 --output jjp_dps1.dag \
  --lhe-shuffle-split --lhe-events-per-block 1000

# Smoke test 带 shuffle-split
python3 dag_generator.py generate-test \
  --machine-env lxplus_t2_ihep --campaign JJP_DPS2_CS \
  --output-dir tests/generated/shuffle_smoke --output smoke.dag \
  --lhe-shuffle-split --lhe-events-per-block 100
```

### Block SubDAG 工作流

`--enable-lhe-block-subdags` 启用两阶段分块级工作流：

1. **Planner**（`tools/plan_lhe_blocks.py`）— 每个 HELAC 作业完成后运行，压缩并 shuffle-split LHE 为分块，stage 分块，写出 plan manifest。
2. **Coordinator**（`tools/coordinate_lhe_blocks.py`）— 对于多源 campaign，读取所有 pool 的 plan manifest，按 strict-min 策略匹配分块，生成 `blocks_processing.dag` SubDAG 含 `MIX_BLOCK` 处理节点。

分块文件命名为 `block_<seed>_<NNNNNN>.lhe.gz` 以保证跨 seed 唯一性。

```bash
# Block SubDAG — 单源 SPS
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_SPS_CS \
  --jobs 10 --enable-lhe-block-subdags --no-scan-existing \
  --output-dir generated/jjp_sps_cs_subdag --output mc_sps_subdag.dag

# Block SubDAG — 多源 DPS 含 coordinator
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_DPS2_CS \
  --jobs 10 --enable-lhe-block-subdags --no-scan-existing \
  --output-dir generated/jjp_dps2_subdag --output mc_dps_subdag.dag

# 兼容模式 — 即使启用 block SubDAG 仍使用扁平 DAG
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_SPS_CS \
  --jobs 10 --enable-lhe-block-subdags --keep-legacy-single-processing-path \
  --output-dir generated/legacy --output mc_legacy.dag
```

### Ntuple-only 重处理（基于已有 MiniAOD）

`generate-ntuple-only` 子命令生成仅运行 ntuple 步骤的 DAG，通过 XRootD 从已有生产输出区读取 MiniAOD 文件。

```bash
# 远程发现 MiniAOD 文件，生成 ntuple DAG
python3 dag_generator.py generate-ntuple-only \
  --machine-env lxplus_t2_ihep \
  --campaign JJP_SPS_CS --campaign JJP_DPS1 \
  --miniaod-base-url root://cceos.ihep.ac.cn:1094//store/user/chiw/MC_Production_v3/output \
  --jobs 50 --dry-run

# 使用子过程命名输出
python3 dag_generator.py generate-ntuple-only \
  --machine-env lxplus_t2_ihep \
  --campaign JJP_SPS_CS --campaign JJP_SPS_G --campaign JJP_DPS2_CS --campaign JJP_DPS2_G --campaign JJP_DPS1 \
  --miniaod-base-url root://cceos.ihep.ac.cn:1094//store/user/chiw/MC_Production_v3/output \
  --jobs 50 --use-subprocess-naming \
  --output-dir generated/ntuple_from_v3_miniaod
```

### 基于已有 LHE 重处理

使用 `--skip-lhe-generation` 配合 `--existing-lhe-base` 将 LHE pool 扫描重定向至其他存储区域：

```bash
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_SPS_CS \
  --skip-lhe-generation \
  --existing-lhe-base root://cceos.ihep.ac.cn:1094//store/user/chiw/MC_Production_v3/lhe_pools \
  --jobs 10 --output-dir generated/reprocess --output reprocess.dag
```

### 覆盖目标 EOS 基址

`--target-base-url` 覆盖所有 worker 脚本的默认 EOS 输出基址：

```bash
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_DPS1 \
  --target-base-url root://cceos.ihep.ac.cn:1094//store/user/chiw/MyTestArea \
  --jobs 20 --output-dir generated/custom_eos --output custom.dag
```

---

### 常用选项参考

| 选项 | 适用于 | 说明 |
|------|--------|------|
| `--disable-ntuple` | `generate`、`generate-test` | 运行至 MiniAOD |
| `--enable-ntuple` | `generate-test` | smoke test 中启用 ntuple |
| `--efficiency-ntuple` | `generate`、`generate-test` | 写出 `ntuple_manifest.json` 供效率工具使用（仅 JJP） |
| `--force-generate-lhe` | `generate`、`generate-test` | 不复用远端已有 LHE |
| `--no-scan-existing` | `generate`、`generate-test` | 不扫描远端已有 LHE |
| `--test-mode` | `generate`、`generate-test` | HELAC 快速测试模式 |
| `--compress-lhe` | `generate`、`generate-test` | 输出压缩 `.lhe.gz` |
| `--lhe-compression-level` | `generate`、`generate-test` | Gzip 压缩级别（默认：1） |
| `--lhe-shuffle-split` | `generate`、`generate-test` | 分层洗牌切分 LHE 为分块 |
| `--lhe-events-per-block` | `generate`、`generate-test` | 每分块事例数（默认：1000） |
| `--enable-lhe-block-subdags` | `generate`、`generate-test` | Block SubDAG 工作流（planner + coordinator） |
| `--keep-legacy-single-processing-path` | `generate` | 即使启用 `--enable-lhe-block-subdags` 仍使用扁平 DAG |
| `--skip-lhe-generation` | `generate`、`generate-test` | 不复用已有 LHE，直接跳过生成 |
| `--existing-lhe-base` | `generate`、`generate-test` | 已有 LHE pool 扫描的基址 URL |
| `--target-base-url` | `generate`、`generate-test` | 覆盖所有 worker 的 EOS 输出基址 |
| `--local-output-base` | `generate`、`generate-test` | 本地 LHE/output 根目录（hepthu） |
| `--local-log-dir` | `generate`、`generate-test` | HTCondor stdout/stderr/log 目录（hepthu） |
| `--use-subprocess-naming` | `generate-ntuple-only` | 子过程命名的 ntuple 输出目录结构 |

## Shower 模式

支持三种规范 shower 模式：

| 模式 | 说明 |
|------|------|
| `normal` | 标准 Pythia8 shower，无 phi enrichment |
| `phi_mpi_off` | Phi-enriched 模式，关闭 MPI，循环 hadronize 直到出现目标 phi |
| `phi_mpi_on_gluon` | Phi-enriched 模式，开启 MPI，基于胶子来源的 phi 分类 |

兼容别名：`phi`、`phi_mode1`、`sps` → `phi_mpi_off`；`phi_mode2` → `phi_mpi_on_gluon`。规范化由 `dag_generator.py` 中的 `canonical_mode()` 处理。

## JJP 双 J/psi 拆分

- `JJP_SPS_CS` 与 `JJP_SPS_G` 分别生产 `gg → J/psi + J/psi` born/color-singlet 与 `gg → J/psi + J/psi + g` 源，不在 worker 端混合。
- `JJP_DPS2_CS` 与 `JJP_DPS2_G` 分别将 `pool_2jpsi_cs`/`pool_2jpsi_g` 与 `pool_gg` 组合，输出路径按 campaign 名独立分开。
- `pool_gg` 使用 `minptq = 4.0`；其他真实 pool 统一使用 `minptq = 0.0`。

## Ntuple 配置

JJP ntuple 配置（`common/cmssw_configs/ntuple_jjp_cfg.py`）是对上游 TPS-Onia2MuMu 参考（`external/TPS-Onia2MuMu` submodule）的薄适配层。原来的独立 efficiency 配置已合并——efficiency 模式现由统一配置中的 `analysisMode` VarParsing 参数控制。

`run_chain.sh` 中的 `--efficiency-ntuple` 现在只控制是否为外部 `run-multileppat-efficiency` 工具写出 `ntuple_manifest.json`，不再选择 cmsRun 配置。

更新 submodule 时，将 `external/TPS-Onia2MuMu/test/ConfFile_cfg.py` 与 `common/cmssw_configs/ntuple_jjp_cfg.py` diff，并重新应用 campaign 适配（`keepAllSingleObjectCandsInMC=True`、正确的 MC GlobalTag、相关的 VarParsing 默认值）。

### 故障排查

| 症状 | 检查 |
|------|------|
| `MC_GenPart_*` 数组全部为空 | `inputGEN` 必须为 `prunedGenParticles`，不能是 `genParticles`。MiniAOD 会丢弃 `genParticles` 集合。 |
| Efficiency 中 HLT 匹配 muon 数为零 | `FiltersForJpsi` 必须为 `["hltJpsiMuonL3Filtered3p5", "hltDoubleMu43LowMassL3Filtered"]`。旧标签匹配不到任何 trigger 对象。 |

## 运行测试

完整验收流程见 [`docs/testing.md`](docs/testing.md)，包括事例数口径、按
schedd 监控、远端 stage-out 验证、ROOT 事例数检查和旧 pilot 清理。

```bash
# 仅静态校验 + smoke DAG 生成（不提交）
./tests/run_all_tests.sh

# 生成并提交至 HTCondor
./tests/run_all_tests.sh --submit
./tests/run_all_tests.sh --submit --wait

# 启用 ntuple
./tests/run_all_tests.sh \
  --enable-ntuple \
  --cmssw15-runtime-tarball common/packages/cmssw15_tpsonia2mumu_runtime.tar.gz

# LHE pool 矩阵测试
./tests/submit_lhe_matrix.sh --submit --wait

# LHE shuffle-split 单元测试
./tests/test_lhe_shuffle_split.sh

# PDG 编码自检
./tests/test_octet_pdg_tool.sh

# 本地 HTCondor 测试
./run_local_test.sh --submit --wait
./run_local_test.sh --campaign JJP_DPS1 --jobs 2 --max-events 10 --submit --enable-ntuple
```

默认测试覆盖：`JJP_DPS2_CS`、`JJP_DPS2_G`、`JUP_DPS1`，外加 `test_octet_pdg_tool.sh`。

单作业 `JJP_DPS2_CS` pilot 使用 `--max-events 5` 时，两个输入 source
分别最多 shower 5 个事例，mixer 最终产生 **5 个输出事例**，不是 10 个。
提交前必须记录 `myschedd show`，因为 cluster ID 只在对应 schedd 上有效。

LHE 矩阵测试覆盖：`pool_jpsi_CSCO_g`、`pool_upsilon_CSCO_g`、`pool_gg`、`pool_2jpsi_cs`、`pool_2jpsi_g`、`pool_jpsi_upsilon_CSCO`，并自动扫描残留的 `9900xxxx` 旧 PDG 编码。

## 当前已知限制

- 即使 ntuple submodule 已初始化，小批量 Condor 验证仍建议默认使用 `--disable-ntuple`，优先验收 MiniAOD 与远端 stage-out。
- `phi_mpi_on_gluon` 通过 Pythia 事例记录中 hardest-process 胶子祖先关系（status 21-29）判定 phi 来源，已比旧占位接口更接近 workbook 要求，但正式大样本前建议额外物理抽查。
- `condor_submit` 对 submit 模板中的 `MaxRetries` 给出"unused"警告——这仅是提示，真正的重试控制以 DAGMan `RETRY` 指令为准。
- `<header>` 块内含有 `<event>` 或 `<init>` 子串（如 `<event_info>`）的 LHE 文件可被 `lhe_shuffle_split.cc` v2.1+ 正确处理，但可能误导简单解析器。
- Ntuple-only DAG（`generate-ntuple-only`）通过 XRootD 列举发现 MiniAOD 文件。如果远端目录结构与预期的 `<campaign>/<job_id>/` 模式不匹配，可能遗漏文件。

## 典型工作流

```bash
# 1. 检查代理与环境
python3 dag_generator.py validate --machine-env lxplus_t2_ihep --campaign JJP_DPS2_CS --scan-existing

# 2. 生成 smoke test DAG
python3 dag_generator.py generate-test \
  --machine-env lxplus_t2_ihep \
  --campaign JJP_DPS2_CS --campaign JJP_DPS2_G --campaign JUP_DPS1 \
  --output-dir tests/generated/smoke --output smoke.dag

# 3. 记录 schedd 并提交
myschedd show
condor_submit_dag tests/generated/smoke/smoke.dag

# 4. 在记录的 schedd 上监控
condor_q -name bigbirdNN.cern.ch <dag-cluster> -nobatch

# 5. 使用 Block SubDAG 工作流进行生产运行
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_SPS_CS \
  --jobs 20 --enable-lhe-block-subdags --compress-lhe \
  --output-dir generated/production --output mc_production.dag
```

## 旧测试脚本

`tests/test_lhe_generation.sh`、`tests/test_shower_chain.sh`、`tests/test_cmssw_chain.sh` 和 `tests/test_pipeline.sh` 保留用于组件级调试。推荐的提交流程以 `dag_generator.py` 配合 `tests/run_all_tests.sh` 或 `tests/submit_tests.sh` 为准。

## 开发者参考

详见 `CLAUDE.md`，包含完整架构参考、编码规范和 Claude Code 在此仓库中工作时使用的详细不变量。`docs/` 目录包含设计笔记和调查报告。
