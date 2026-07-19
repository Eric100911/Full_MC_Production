# 2026-06-08 Integration Conversation Archive

> **历史对话和分步计划，不是当前操作手册。** 分支、路径、配置文件和测试建议
> 反映 2026-06-08 的仓库状态，其中部分内容已被统一工作流取代。当前说明见
> `README_zh-cn.md`、`docs/testing.md` 和 `docs/directory_path_reference.md`。

明白了，你的本地 Git 配置是：

* `chiw` → 你有 push 权限，指向 `Eric100911/Full_MC_Production`
* `origin` → pull-only，指向 `Endymion2288/Full_MC_Production`

目标是把 `master` + `ihep` 作为基础，把 `VtxSmeared` 的 deterministic shuffle mixer 和 shower guarded settings，以及 `hepthu` 的本地 HTCondor wrapper/cleanup/logging 功能整合进主线，同时保留 `master` 的 Vtx smearing、HELAC matrix、TPS-Onia2MuMu submodule、runtime bundle、ntuple bundle、CMSSW15 runtime。

下面是我根据你最新说明，针对 Codex 执行的**分步整合计划大纲**（精确区分远程别名）：

---

## **分步整合计划大纲（针对 Codex 执行）**

### **Phase 0：准备本地集成环境**

1. 本地建立集成分支，以 `master` 为基础：

```bash
git checkout chiw/master
git pull chiw master
git checkout -b integrate-production-workflows
```

2. 拉取 `ihep` 最新更新：

```bash
git fetch chiw ihep
git merge chiw/ihep --no-ff -m "Merge ihep latest updates into integration branch"
```

目标：保证 `ihep` 的 HepJob workflow、MachineEnv、efficiency ntuple、cleanup/logging 已在分支上。

3. 拉取 `VtxSmeared` 用作参考：

```bash
git fetch origin VtxSmeared
```

4. 拉取 `hepthu` 用作参考：

```bash
git fetch origin hepthu
```

---

### **Phase 1：保护 GEN-SIM vertex smearing**

1. 确认 `common/cmssw_configs/hepmc_to_GENSIM.py` 保留：

   * `VtxSmearedCommon.src = cms.InputTag("source", "generator")`
   * `process.VtxSmeared = cms.EDProducer("BetafuncEvtVtxGenerator", ...)`
   * `g4SimHits` 与 `genParticles` 使用 `generatorSmeared`
2. Codex 任务：

```text
生成静态检查脚本 tools/check_gensim_vtxsmeared_config.py，验证上述字段存在
并集成到 tests/run_all_tests.sh
Commit message: test: guard GEN-SIM vertex smearing wiring
```

---

### **Phase 2：整合 VtxSmeared 功能**

1. **event_mixer_multisource.cc**

   * 核心目标：deterministic shuffle + shortest source length
   * 保留 CLI 参数控制：

     * `--shuffle-sources`
     * `--shuffle-seed-base <uint64>`
   * 默认 sequential fallback

2. **pythia_shower**：

   * Port guarded settings函数：

     * `setFlagIfExists`, `setModeIfExists`, `setParmIfExists`
   * 保留 master/ihep physics defaults
   * 标注 normal/phi/sps 模式行为

3. Codex 任务：

```text
- 比较 origin/VtxSmeared 与当前集成分支
- port deterministic shuffled mixer逻辑，如果尚未存在
- port guarded Pythia setting函数
- 添加 CLI 参数控制
- Commit message: feat: add deterministic shuffled mixer and guard Pythia settings
```

---

### **Phase 3：整合 hepthu 本地 HTCondor wrapper/cleanup/logging**

1. 检查差异：

   * 文件：`run_local_test.sh`, `run_chain.sh`, `condor_wrappers/*`, `templates/*`
   * 不覆盖 `dag_generator.py`，保持 master/ihep 逻辑

2. port 特性：

   * cleanup trap / rm 中间文件
   * `run_logged` 日志记录
   * LOCAL_OUTPUT_BASE 支持
   * 本地 LHE pool / premix 本地 cache

3. Codex 任务：

```text
- 比较 origin/hepthu 与集成分支
- port缺失本地/HTCondor便利功能
- 保证功能 opt-in，通过 machine/env 或 CLI 参数控制
- Commit message: feat: add missing hepthu local Condor workflow utilities
```

---

### **Phase 4：整合效率/acceptance ntuple模式**

1. 检查：

   * `dag_generator.py` 是否支持 `--efficiency-ntuple`
   * `hepjob_workflow.py` 是否支持 `--efficiency-ntuple`
   * `run_chain.sh` 是否传递 `EFFICIENCY_NTUPLE`
   * `common/cmssw_configs/ntuple_jjp_efficiency_cfg.py` 配置正确
2. Codex 任务：

```text
- 确认 efficiency ntuple流程正确工作（JJP only）
- 生成 manifest JSON 文件
- Commit message: feat: stabilize efficiency ntuple workflow
```

---

### **Phase 5：DAG生成/本地/远端提交集成**

1. dag_generator.py:

   * 支持 `--shuffle-mixing/--no-shuffle-mixing`
   * 支持 `--local-output-base`
   * 支持 `--local-condor`
   * 保留 `ihep` 的 MachineEnv profiles
2. Codex 任务：

```text
- 确认 DAGBuilder 与 runtime_assets 兼容
- 确认 efficiency_ntuple 与 local_condor 配置正确
- Commit message: feat: integrate DAG generation options for local and shuffled workflows
```

---

### **Phase 6：本地测试 & smoke tests**

1. 静态检查：

```bash
python3 -m py_compile dag_generator.py hepjob_workflow.py
bash -n processing/run_chain.sh
```

2. DAG dry-run：

```bash
python3 dag_generator.py generate-test \
  --campaign JJP_DPS2_CS \
  --jobs 1 \
  --max-events 5 \
  --enable-ntuple \
  --efficiency-ntuple \
  --shuffle-mixing \
  --dry-run
```

3. 本地模式：

```bash
./run_local_test.sh --campaign JJP_DPS2_CS --jobs 1 --max-events 5
```

4. GEN-SIM vertex smearing debug：

```bash
cmsRun common/cmssw_configs/hepmc_to_GENSIM.py maxEvents=1 debugDump=True
```

5. MiniAOD & efficiency ntuple smoke tests

---

### **Phase 7：GitHub 推送与 PR**

1. push 集成分支：

```bash
git push chiw integrate-production-workflows
```

2. PR 设置：

   * Base: `master`
   * Compare: `integrate-production-workflows`
   * 标题：`Integrate IHEP HepJob, efficiency ntuple, VtxSmeared mixer, and hepthu local utilities into master`
3. PR checklist：

```markdown
- [ ] py_compile/dag_generator & hepjob_workflow
- [ ] bash -n run_chain.sh
- [ ] DAG dry-run
- [ ] GEN-SIM vertex smearing guard
- [ ] Mixer shuffle test
- [ ] Shower guarded Pythia test
- [ ] MiniAOD smoke test
- [ ] Efficiency ntuple smoke test
```

---

**总结层级：**

| 分支/方向      | 作用                                                                       |
| ---------- | ------------------------------------------------------------------------ |
| master     | 稳定主线，HELAC matrix, TPS-Onia2MuMu submodule, runtime bundle               |
| ihep       | IHEP HepJob + MachineEnv + efficiency ntuple + cleanup/logging（基础层，优先合入） |
| VtxSmeared | deterministic shuffle mixer + shower guarded settings                    |
| hepthu     | 本地 HTCondor wrapper/cleanup/logging查漏补缺，不覆盖 master/ihep                  |

下一步可以直接按这个分步大纲交给 Codex 执行集成任务。
