# 执行提示词二：SPS_G、DPS2_G 各 1000 个事件

执行本阶段：使用本实验第一阶段新生成并精确计数的 LHE，为 `JJP_SPS_G` 和 `JJP_DPS2_G` **各完成 1000 个 mixed/MiniAOD 输入事件**，各 10 个 MIX 节点、每节点目标 100。完成 CERN 处理与 IHEP merge→ntuple→selection/efficiency，交付实际计数和第三阶段所需产率。原始 HELAC 扫描事件不计入这 1000。用户已授权提交、IHEP 两核及全局 EDM 编号方案；不得开补产运行、延长已完成运行或降低目标冒充完成。若实际不足，准确报告 shortfall。

## 边界、前置证据和执行分工

工作目录 `/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/v06_00-staging-scheduler`，分支 `feat/v06_00-staging-scheduler`。读取该目录 `AGENTS.md`、`docs/technical_language_style.md`、`/eos/home-c/chiw/.codex/skills/stop-that-shit/SKILL.md`；实际操作前读 `skills/cms-ihep-condor-pilot/SKILL.md`、`skills/cms-condor-job-operator/SKILL.md` 及操作参考；容量分析读 `skills/cms-lhe-capacity-planner/SKILL.md`。只读本实验 `$EXP/stage1/handoff.json` 及其证据，不要求前一提示词上下文。

协调者不执行命令；简单执行/两小时监控交给 Luna，跨文件实现给 6.1。代理只回结论、退出码、证据路径，不回大段日志。每个主要阶段一次合并评审，充分证明后不重复审核。同一代理同一工作扇区超过三次真实失败即暂停该扇区，完成独立工作后人工评审；排队、PRE defer、vacate、未完成不算失败。不要引入 daemon、多运行账本、新协调器、散列框架或依赖重构。

旧 production-core/wrapper、旧 G5、旧 LHE 池、记忆库存和既有生产路径不动；旧 G5 已完成，不监控它们。阶段一必须已经保留并纳入八项原有分桶改动，完成有限 HELAC 扫描、诊断保留、IHEP -n 2/十节点修正、`global_job_index_start` 和 CMSSW RNG seed 保持原配置、不覆盖的约定。这些是起始版本 `1c4d008` 所缺的实现，不能靠添加未知 CLI 参数代替。若交接未证明实现和测试完成，停在前置检查并报告；不要再跑扫描、重做全站验证或就地改变旧 runtime。

读已有 AFS 证明 `generated/v06_00_site_validation_20261017T104551Z/cern-afs-lock-proof.json`：bigbird11/22 的锁与 G4 项均已通过。不为旧文档 pending 文字重跑探针。阶段一的新 CERN/IHEP runtime 必须来自包含必要修正的同一干净本地提交，descriptor 的 source_dirty=false、git_revision/release/analyzer 一致。可以为必要修正做有范围的本地提交并重建**新路径**运行包；没有 tag、push 或公开发布授权。不得用缺少分桶修正的旧 G5 包。

## 固定参数

| 项 | SPS_G | DPS2_G |
|---|---|---|
| campaign / analysis | JJP_SPS_G / JJP | JJP_DPS2_G / JJP |
| 源与模式 | pool_2jpsi_g:phi_mpi_off | pool_2jpsi_g:normal；pool_gg:phi_mpi_off |
| 每 MIX 源 LHE 预算 | 200 | 200；400 |
| 每 MIX 目标 | 100 | 100 |
| max_processing_nodes | 10 | 10 |
| global_job_index_start / merge_group_index_start（阶段一新增字段） | 0 / 0 | 0 / 0 |
| 全局编号 / 本地编号 | 0..9 / 0..9 | 0..9 / 0..9 |
| merge_events / 预计组数 | 1000 / 1 | 1000 / 1 |
| shard_nodes / target_window_nodes | 10 / 20 | 10 / 20 |

block_events=100；每 campaign/池 stride=1、offset=0。同 campaign 每次输入都用不同且不交叠的物理 BLOCK；两个 campaign 允许共享 pool_2jpsi_g 的块。源计数、planned_events 和 BLOCK refs 从本实验阶段一交接读取，不用 597 hint 或历史 5/3 源文件数。阶段二不产生额外 HELAC 源。实际源文件数等于交接 `existing_sources` 按池的记录数；逐源预计完整 block 数为 floor(planned_events/100)，必须在提交说明中输出真实数值。

物理冻结：HELAC 2.7.6 main，2jpsi 过程 `generate g g > cc~(3S11) cc~(3S11) g`，min_pt_conia=6、min_pt_bonia=4、min_pt_q=0 GeV；两个质量 cuts 最后记录均为 3.0969 GeV，以阶段一冻结值为准，只有用户在扫描前提供并冻结的新值才能替换。gg 过程 `generate g g > g g`，min_pt_q=4，两个质量 cut 和 conia/bonia pT=0。pp=6800+6800 GeV，effectiveScale=0、muR=muF=12.8、ScaleFactor=muR_over_ref=muF_over_ref=1、pdf=10000/cteq6m、lhapdf=F、cmass=1.54845 GeV。nmc/unwevt 必须用阶段一选中 profile 与源的实际 provenance；基线是 1m/100k，非控制源 unwevt=10m，不能重置为旧 500k/2000。preunw=nopt=nopt_step=nmc/10、noptlim=nmc；nmc 是每 pass 预算，实际事件数只读实测计数。

detector=`run3_2022_legacy_premix_v1`，simulation/merge CMSSW_12_4_14，GT=`124X_mcRun3_2022_realistic_v12`，RAW 2 threads/2 streams、CERN miniaod 请求 2 CPU。ntuple CMSSW_15_0_15，analysis JJP、efficiency ntuple=true、tree=`mkcands/X_data`。phi shower 保持 ISR/FSR on、MPI off、phi pT>3 GeV、repeat=5000、J/psi→μμ、phi→KK；不改现有 Pythia 物理。

IHEP combined allocation 必须 `hep_sub -n 2`，轮转 ams017/018/019、cws047/048/049/051/052/053/054 共十节点；排除 ams020（Start policy）、cws050/055（rc=132）。独立 merge-only 不绑节点。LHE 3072MB、CERN processing 6000MB、scratch 至少 4GB、workday 默认；本阶段是 existing LHE，LHE 资源不会实际提交生成作业。IHEP CLI 没有 `--cpus` 或 `--memory`；实际提交必须来自阶段一修正，不编造 flags。若需要显式 IHEP memory，现有 Python `prepare_batch(..., memory_mb=6000)` 支持，但先确认是否必要，不能假称下面 CLI 自动设置了它。

## 冻结目录与完整 run spec

从用户给定或阶段一报告路径定位唯一实验。以下 EXP_ID 必须改成阶段一真实值，不重新选择新实验编号：

```bash
export CORE=/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/v06_00-staging-scheduler
export EXP_ID=three_stage_20261018T120000Z
export EXP="$CORE/generated/$EXP_ID"
export H1="$EXP/stage1/handoff.json"
export S2="$EXP/stage2"
export RUN="$S2/cern"
export SPEC="$S2/spec.json"
export SITE="$S2/ihep-site.json"
```

新路径统一继承阶段一隔离根：CERN `root://eosuser.cern.ch///eos/user/c/chiw/Full_MC_Production/three_stage/<EXP_ID>/staging`；IHEP `root://cceos.ihep.ac.cn:1094///store/user/chiw/Full_MC_Production/three_stage/<EXP_ID>/products` 和 `/archive`。run_id=`<EXP_ID>_s2_1k`，使 runtime 创建的 run 子目录也隔离。交接 destination 独立到 `/handoffs/stage2`，避免现有共享 handoffs/handoff.json 被覆盖。默认 staging 5GB、reserve 1GB=4GB usable；原 LHE/整源 BLOCK ceiling 各 100MB，单 MIX 64MB。两互补源+十节点 MIX 下界 1.04GB，实际大小已由扫描确认；若超上限，冻结前声明所需 ceiling/用户预算，不能运行中静默加额。只接受实际 reclaim 删除后回收 credit，不能以归档成功抵扣。

用 Python 写 JSON，不把 JSON 解析结果送入 eval。下面示例使用现有 run schema v2，新 campaign 字段 `global_job_index_start`、`merge_group_index_start` 必须已由阶段一实现：前者控制全局 EDM 编号，后者防止两阶段外部 merge task/ref 和 efficiency tar shard 名冲突；内部 ledger ordinal 仍从0。所有动态值都来自冻结交接：

```bash
python3 - <<'PY'
import copy, json, os
from pathlib import Path
exp = Path(os.environ['EXP'])
h = json.loads(Path(os.environ['H1']).read_text())
assert h['schema'] == 'three-stage-stage1-v1' and h['status'] == 'complete'
assert h['experiment_id'] == os.environ['EXP_ID']
assert h['phase2_capacity']['sufficient'] is True
sources = copy.deepcopy(h['existing_sources'])
assert sources and all(0 < s['planned_events'] <= s['actual_events'] for s in sources)
assert all('url' in s and 'generator_profile' not in s for s in sources)
campaigns = []
for name, budgets in [('JJP_SPS_G', [200]), ('JJP_DPS2_G', [200,400])]:
    campaigns.append(dict(name=name, analysis='JJP', source_lhe_budgets=budgets,
        target_mixed_events=100, max_processing_nodes=10, merge_events=1000,
        global_job_index_start=0, merge_group_index_start=0,
        detector_variants=['run3_2022_legacy_premix_v1']))
storage = {k: h['storage'][k] for k in ('cern_staging','ihep_products','ihep_archive')}
storage.update(staging_bytes=5000000000, reserve_bytes=1000000000)
spec = dict(schema_version=2, run_id=os.environ['EXP_ID']+'_s2_1k',
    core_revision=h['code_revision'], runtime_release=h['runtime_release'],
    runtime_dir=h['cern_runtime_dir'], orchestration='campaign_shards',
    inventory_path=h['counted_inventory_path'], inventory_sha256=h['counted_inventory_sha256'],
    block_events=100, sources=sources, campaigns=campaigns,
    consumption_streams=[dict(campaign=c, pool=p, stride=1, offset=0) for c,p in [
        ('JJP_SPS_G','pool_2jpsi_g'), ('JJP_DPS2_G','pool_2jpsi_g'), ('JJP_DPS2_G','pool_gg')]],
    normal_shortfall_policy='report-and-truncate', unused_hepmc_warning_fraction=0.15,
    limits=dict(shard_nodes=10, target_window_nodes=20, source_lhe_bytes=100000000,
        source_block_bytes=100000000, mixed_hepmc_bytes=64000000,
        source_files_per_step=2, processing_nodes_per_wave=10),
    storage=storage,
    site_validation=dict(proof_path=h['site_proof_path'], isolated_budget_bytes=4000000000),
    scheduler=dict(schedd='bigbird11.cern.ch', max_jobs_submitted=200, max_jobs_idle=100,
        max_submits_per_interval=20, submit_delay=0, max_pre_scripts=20, max_post_scripts=20,
        maxjobs=dict(lhe=20,lhe_planning=20,processing=20,block_processing=20,lhe_coordination=20)),
    resources=dict(lhe=dict(cpus=1,memory_mb=3072,disk_bytes=4000000000,flavour='workday'),
        miniaod=dict(cpus=2,memory_mb=6000,disk_bytes=4000000000,flavour='workday')))
out = Path(os.environ['SPEC']); out.parent.mkdir(parents=True,exist_ok=True)
assert not out.exists()
out.write_text(json.dumps(spec,ensure_ascii=False,indent=2)+'\n')
site = dict(name='ihep',destination='root://cceos.ihep.ac.cn:1094///store/user/chiw/'
    'Full_MC_Production/three_stage/'+os.environ['EXP_ID']+'/handoffs/stage2')
Path(os.environ['SITE']).write_text(json.dumps(site,indent=2)+'\n')
PY
```

若阶段一因测量更改过 storage ceiling/预算，以上数值必须一次替换成已批准并冻结的数值，同时更新 isolated_budget_bytes 和 limits，不照抄失真的示例。提交前执行者完成实际 allocate_shard 模拟，验证足够 10+10 节点并使用真实 refs，保留计数、HELAC/shower/MIX seed 与 EDM 编号证据。shower 源 seed 与 MIX seed 中包含 source_id；输入必须来自阶段一新源（pool2 93000..93007；gg 95000..95001），不得复用旧全局 80000..92459。仅换 run_id 不独立。

## CERN 规划、提交与交接

```bash
cd "$CORE"
./mcprod run plan --spec "$SPEC" --output-dir "$RUN"
```

一次合并检查 run.json、root DAG、子 DAG 模板、运行包身份及输入容量：根 source 类 MAXJOBS lhe_planning=2、shards block_processing=6、cleanup lhe_coordination=20；worker 类默认 MAXJOBS=20；DAGMan queue width 200、idle100、每 interval20、delay0、MAX_PRE/POST20、AutoRescue=true、AlwaysRunPost=false。确认不会全局等待所有 PLAN、无前一 shard 完成链，保留完整 MIX shard 同时预留和既有 AFS 单账务机制。既有 `global_job_index_start` 扩展应在实际任务和 CMSSW cfg 生效，不能仅看 spec。

执行者选 schedd 用 `myschedd bump`、`myschedd show`，如实际选中不同于示例，应在首次 plan 前把 scheduler.schedd 固定为真实值。网络命令需正常获准环境；受限 sandbox 不做预期失败的 live probe。将初次提交代理固定到本实验 `credentials/x509_user_proxy`，剩余寿命 >48h，worker 私有安装路径同名；绝不用 `/tmp/x509up_u$UID`。不要将代理内容入库或打印。

提交前报告：每 campaign 1000 目标、实际按池源文件数/逐源完整 blocks、block100、预算200/200+400、各10 MIX、各1 merge group、上述 subprocess 覆盖。

```bash
./mcprod run submit --run "$RUN" --site cern
```

保存 submission.json 的 schedd 与 top DAGMan cluster ID，首次提交 UTC 为整个阶段的唯一 48h anchor。提交结果不明确先核对该次身份，不盲重提。CERN `.complete`、完成状态、匹配 root 的 DAGMan 成功/metrics 和已接受输出/归档凭据一致时即可判定完成；root 离开 live 队列是正常行为。

```bash
./mcprod handoff export --run "$RUN" --ready-only
./mcprod handoff transfer --run "$RUN" --site ihep --site-config "$SITE"
```

campaign_shards 现有 export 要求 run 已完成。核对正式 handoff.json rejected=[]、共 2 个 combined task、每 campaign 一个 1000 事件组，runtime/core 身份相符；实际不足仍记录真实组数和输入和，不能随意截断或补齐。现有 merge worker 要求输出事件数等于所有输入之和，不支持任意 max-events 截断。

## IHEP 接收、批次提交

阶段一交接应提供实际 IHEP 接收目录；缺少则按已有本用户持久布局创建新目录 `/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production/runs/<EXP_ID>/stage2`。不要接管旧 run。将**本实验同一 clean commit**运行包复制到 IHEP 持久独立目录，并验证匹配，而不是在 IHEP 重建不同身份的包。可由执行者在已配置的 `ihep` 登录别名中操作；不要 SSH 到 CERN schedd。本地持久 handoff 从上步唯一 destination 下载，不扫描所有 JSON。下面传输由 CERN 执行代理运行一次；若阶段一已经传好同身份运行包，仅核对其记录，不重传：

```bash
python3 - <<'PY'
import json,os,re,shlex,subprocess
from pathlib import Path
h=json.loads(Path(os.environ['H1']).read_text())
eid=os.environ['EXP_ID']; assert re.fullmatch(r'[A-Za-z0-9_.-]+',eid)
base='/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production/runs/'+eid
subprocess.run(['ssh','ihep',shlex.join(['mkdir','-p',base+'/runtime',base+'/stage2/credentials'])],check=True)
subprocess.run(['ssh','ihep',shlex.join(['test','!','-e',base+'/runtime/ihep'])],check=True)
subprocess.run(['scp','-r',h['ihep_runtime_dir'],'ihep:'+base+'/runtime/ihep'],check=True)
PY
```

以下命令在 IHEP 登录 shell 执行，`IHEP_CORE` 固定为已接收运行包的 controller/core，`IHEP_RUNTIME_REMOTE` 固定为其目录；通过交接/Python读取真实路径后设置变量，不能从不可信 JSON 生成 shell：

```bash
export PATH=/cvmfs/common.ihep.ac.cn/software/hepjob/bin:$PATH
export IHEP_RUN="/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production/runs/$EXP_ID/stage2"
export IHEP_RUNTIME_REMOTE="/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production/runs/$EXP_ID/runtime/ihep"
export IHEP_CORE="$IHEP_RUNTIME_REMOTE/controller/core"
export BATCH="$IHEP_RUN/batch"
export IHEP_PROXY="$IHEP_RUN/credentials/x509_user_proxy"
export X509_USER_PROXY="$IHEP_PROXY"
export HANDOFF_URL="root://cceos.ihep.ac.cn:1094///store/user/chiw/Full_MC_Production/three_stage/$EXP_ID/handoffs/stage2/handoff.json"
xrdcp --force "$HANDOFF_URL" "$IHEP_RUN/handoff.json"
python3 "$IHEP_CORE/mcprod" batch prepare --handoff "$IHEP_RUN/handoff.json" --output-dir "$BATCH" --proxy "$IHEP_PROXY" --runtime-dir "$IHEP_RUNTIME_REMOTE"
python3 "$IHEP_CORE/mcprod" batch submit --manifest "$BATCH/batch_manifest.json"
```

先创建新持久目录并用 `install -m 600` 从当前合法代理安装 IHEP_PROXY；不输出秘密。runtime 复制通过获准文件传输保留完整 descriptor 和 bundles；不要只拷 `core.tar.gz`。batch prepare/submit 串行，提交前一次检查 manifest 恰好2任务、-n2、十节点轮转、private proxy、实际核心/运行包。记录每个 HepJob ID，不使用整个用户队列计数。核对 worker 实际打印的线程/stream/seed，与请求一致，已有证据后不重复追问。

## 两小时监控、恢复边界与完成

从首次 CERN 提交起，CERN+IHEP **合计**最多 48h。初始记录后每 2h 一批、最多24批；不是每换 backend 再给48h。完成或用户通知可立即检查；等待不失败。每批只调用当前 backend 的一次准确状态并核对必要新增证据：

```bash
python3 "$CORE/tools/mcprod_snapshot.py" --run "$RUN" --json
./mcprod run status --run "$RUN" --site cern
```

两条 CERN 命令选择一条通常足够，不每轮都重复；需要拓扑时再对 submission.json 记录的精确 root 用 `tools/dag_progress.py ROOT --schedd HOST --structure=collapsed`。history 必须指定相同 schedd 和 exact ID，`-limit 1 -scanlimit 10000`，不做 owner/time-window 广搜。

IHEP shell：

```bash
python3 "$IHEP_CORE/mcprod" batch status --manifest "$BATCH/batch_manifest.json"
```

该命令按 manifest 的精确 HepJob ID 查询。hep_q 或 xrdfs 空返回在 fork exhaustion 情况只重试一次，再看持久日志、checkpoint和准确产物；空队列不等于失败。**所有任务四阶段 checkpoint identity/status=ok、完整实际输出有效、计数/provenance 正确即可 DONE**；即使 hep_q/history 不再保留作业也停止监控，不继续 reconciliation 循环或补提。

只有确认失败、需要恢复原运行且未成功完成时，先检查原 outputs/checkpoints/rescue，才可执行下列既有恢复命令；不是补产，不能增加目标或新 source：

```bash
./mcprod run resume --run "$RUN" --site cern
python3 "$IHEP_CORE/mcprod" batch resume --manifest "$BATCH/batch_manifest.json"
```

CERN resume 需要确认 root 失败和显式 rescue；IHEP resume 只处理已确认 failed jobs。正常 queue wait、vacate 或产物已齐全不触发 resume。到48h仅记录剩余工作及准确身份，不 condor_rm、不自动重开。

成功后可用既有聚合命令整理本阶段两个 campaign 的 selected/efficiency 产品，按 campaign 自动分组。运行包的 controller/core 本身未展开 analyzer.tar.gz；用现有 unpack_runtime 在新的持久目录准备 analyzer 完整代码，不能直接在缺少 analyzer 的 controller/core 执行聚合，也不修改不可变运行包：

```bash
export AGG_CORE="$IHEP_RUN/aggregation-runtime/core"
PYTHONPATH="$IHEP_CORE/src" python3 - <<'PY'
import json,os
from pathlib import Path
from full_mc.runtime.bootstrap import unpack_runtime
r=Path(os.environ['IHEP_RUNTIME_REMOTE'])
d=json.loads((r/'runtime.json').read_text())
unpack_runtime(r,Path(os.environ['IHEP_RUN'])/'aggregation-runtime','analyzer-el9',d['runtime_release'])
PY
python3 "$AGG_CORE/mcprod" analysis aggregate --run "$IHEP_RUN" --prepare --handoff "$IHEP_RUN/handoff.json" --output "$IHEP_RUN/aggregation.json" --campaign JJP
python3 "$AGG_CORE/mcprod" analysis worker --task "$IHEP_RUN/aggregation.json" --work-dir "$IHEP_RUN/aggregation-work"
```

analysis_output 应在独立 handoff 中显式设置到本实验 stage2 analysis URL；未设置时是 IHEP_RUN 下持久本地产物。JJP 是 analysis 类型，不能换成 `--campaign JJP_SPS_G`；现有聚合只接受 JJP，内部按 campaign+detector 分开，绝不混合 SPS_G 与 DPS2_G。selected ntuple 条目因筛选可能少于1000；完成目标依据 mixed/MiniAOD精确数，不拿筛选数冒充全部事件。

写 `$EXP/stage2/handoff.json`，schema=`three-stage-stage2-v1`，包含 status、experiment_id、stage1_handoff_path、code/runtime/analyzer身份、run_dir/submission与精确 IDs、IHEP batch_manifest/IDs、每 campaign expected=1000 和 actual mixed/component/merged计数、accepted groups及完整四阶段产物/凭据路径、used_source_ids、逐 campaign used_block_refs、local/global_job_indices、实际 EDM 三元组范围及唯一性证据、全部已冻结 HELAC source/shower/MIX seeds、有效线程设置、每slot attempted_lhe/accepted_hepmc/used/unused/retries/wall、phase3_capacity_inputs（选中profile的逐replica实际LHE、stage2实测shower产率）、storage/retained bytes、anchor/deadline及 evidence_paths。0.15 unused-HepMC 只是 warning，不是硬目标或停机条件。

实际每 campaign 恰好1000、10节点100 each、一个1000 merge group、四阶段成功且编号/seed验证通过才写 complete。若任何 shortfall 则写真实不足，禁止承诺阶段三替阶段二补齐。最终只向协调者回结论、退出码和交接/证据路径；完成即停止。
