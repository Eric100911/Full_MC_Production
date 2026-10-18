# 执行提示词三：各新增 99000，按 campaign 合并为 100000

执行本阶段：为 `JJP_SPS_G`、`JJP_DPS2_G` **各新增 99000 个 mixed/MiniAOD 事件**，每 campaign 990 个 MIX 节点×100 目标事件。它们与本实验第二阶段同 campaign 的1000相加，各形成100000事件的数据集。第三阶段本身是99k，不是100k；不得跨 campaign 合并，不得另开补产运行、延长已完成运行或降低目标冒充完成。实际 shortfall 如实报告，不能让第三阶段替第二阶段凑数。用户已授权该实验、IHEP两核和全局EDM编号设计，普通执行无需重复征求同一授权。

## 独立工作上下文与前置检查

工作目录 `/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/v06_00-staging-scheduler`，分支 `feat/v06_00-staging-scheduler`。先读该目录 `AGENTS.md`、`docs/technical_language_style.md`、`/eos/home-c/chiw/.codex/skills/stop-that-shit/SKILL.md`；真实操作读 `skills/cms-ihep-condor-pilot/SKILL.md`、`skills/cms-condor-job-operator/SKILL.md`及其操作参考，容量分析读`skills/cms-lhe-capacity-planner/SKILL.md`。无需阅读前两份提示词，但必须读取本实验阶段一、二的冻结交接及其直接证据。

协调者不运行命令；简单执行和两小时监控用Luna，跨文件实现用6.1。只向协调者返回结论、退出码、证据路径。每个主要阶段结束后一次合并评审，不做重复审核。同一代理同一工作扇区超过三次真实失败则暂停该扇区，先做独立工作再人工评审；queue wait、PRE defer、vacate、正常未完成不计失败。不要引入daemon、多运行账本、通用协调器、散列机制或依赖重构。

旧production-core/wrapper、G5、旧LHE池、记忆库存和既有生产目录不动。旧G5已完成，不轮询。现有AFS证明 `generated/v06_00_site_validation_20261017T104551Z/cern-afs-lock-proof.json` 的bigbird11/22锁与G4项已通过，读取并复用；不因旧文档pending重跑现场证明。阶段一必须已完成：有限HELAC扫描和诊断保留、八项原有分桶改动、IHEP `-n 2`及十节点、全局EDM编号和外部merge组编号接口；CMSSW RNG seed 不覆盖，随机性由 HELAC 硬散射输入提供。起始提交1c4d008不具备这些全部功能，不得用未知flags绕过。

新接口为campaign.`global_job_index_start`与`merge_group_index_start`：阶段二0/0，阶段三10/1；后者只改变外部task/ref/merge_group/tar shard身份，内部ledger ordinal与counts仍本地从0。CMSSW RNG seed 保持其原配置；随机性来自 HELAC 硬散射输入事件。旧完成标记只读兼容，不迁移旧运行。读取阶段一的编号接口测试与输入 provenance，确认已实施，不能只看配置字段存在。

两站runtime须是同一干净本地提交，source_dirty=false，git_revision/release/analyzer一致，包含分桶和上述必要修正，不能用旧G5运行包。必要窄修正可以形成有范围本地提交并重建新路径双站包；没有tag/push/公开发布授权。阶段二必须每campaign实际完成1000且四阶段有效，才继续本阶段；否则报告前置缺口，不重跑阶段一/二。

## 固定规模、物理与资源

| 项 | SPS_G | DPS2_G |
|---|---|---|
| campaign / analysis | JJP_SPS_G / JJP | JJP_DPS2_G / JJP |
| source slots | pool_2jpsi_g:phi_mpi_off | pool_2jpsi_g:normal；pool_gg:phi_mpi_off |
| 每MIX LHE预算 | 200 | 200；400 |
| 每MIX目标 / 节点数 | 100 / 990 | 100 / 990 |
| 本阶段目标 | 99000 | 99000 |
| global_job_index_start | 10 | 10 |
| 本地 / 全局编号 | 0..989 / 10..999 | 0..989 / 10..999 |
| merge_events / 预计组数 | 1000 / 99 | 1000 / 99 |
| merge_group_index_start / 外部组号 | 1 / 1..99 | 1 / 1..99 |
| shard_nodes / 分片数 / window | 10 / 99 / 20 | 10 / 99 / 20 |

block_events=100；每campaign/池stride=1、offset=0。同campaign物理BLOCK不复用；跨campaign允许共享pool_2jpsi_g。stage3不能使用stage1/2源ID或seed，也不能重复其BLOCK。shower/MIX确定性seed原本包含BLOCK source_id，保留算法；仅换run_id不构成独立。阶段二全局0..9与本阶段10..999的同campaign EDM(run,lumi,event)必须完全不交叠。

HELAC2.7.6 main：pool2过程`generate g g > cc~(3S11) cc~(3S11) g`，min_pt_conia=6、min_pt_bonia=4、min_pt_q=0 GeV；质量cuts最后记录`min_m_qqp=min_m_q_beam=3.0969` GeV，只用阶段一统一冻结的值，不编造更严阈值。gg过程`generate g g > g g`，min_pt_q=4、conia/bonia pT和两个质量cut=0。pp6800+6800 GeV、effectiveScale0、muR=muF12.8、ScaleFactor=muR_over_ref=muF_over_ref1、pdf10000/cteq6m、lhapdfF、cmass1.54845 GeV；其他LDME/积分器/快度参数沿模板。

每池使用阶段一选中的生产profile；所有新非控制源unwevt=10000000，正整数，不能恢复2000。pool2的nmc选自扫描1m/2m证据；gg nmc固定500000，不扫这个轴。若所选pool2生产profile仍是控制unwevt100k，必须回到已有扫描B/D的同nmc放宽结果完成选择，不能无证据更换生产参数。preunw=nopt=nopt_step=nmc/10、noptlim=nmc；nmc是每pass预算，不能当作实际事件数。源行只选generator_profile ID，不允许inline nmc/unwevt/cuts。

detector=`run3_2022_legacy_premix_v1`，simulation/merge CMSSW_12_4_14，GT `124X_mcRun3_2022_realistic_v12`、RAW实际2threads/2streams、CERN miniaod请求2CPU；ntuple CMSSW_15_0_15、JJP、efficiency ntuple=true、tree `mkcands/X_data`，优先预构建`common/packages/cmssw15_tpsonia2mumu_runtime.tar.gz`。phi模式ISR/FSR开、MPI关、phi pT>3GeV、repeat5000、J/psi→μμ、phi→KK，不改Pythia物理。

LHE1CPU/3072MB，CERN processing2CPU/6000MB，scratch至少4GB、workday默认。扫描证明高nmc超workday时，在冻结前仅用现有`resources.lhe.flavour`显式指定合适flavour并说明证据。IHEP combined必须实际`hep_sub -n 2`，轮转10主机ams017/018/019+cws047/048/049/051/052/053/054；排除ams020 Start policy和cws050/055 rc132。独立merge-only不绑节点。不得编造`--cpus`、`--job-index-start`等现有CLI没有的选项。

## 从实测产率冻结有限源数

从阶段一报告定位唯一EXP_ID，例子编号需替换为本实验真实值。路径固定后不改变：

```bash
export CORE=/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/v06_00-staging-scheduler
export EXP_ID=three_stage_20261018T120000Z
export EXP="$CORE/generated/$EXP_ID"
export H1="$EXP/stage1/handoff.json"
export H2="$EXP/stage2/handoff.json"
export S3="$EXP/stage3"
export SPEC="$S3/spec.json"
export RUN="$S3/cern"
export SITE="$S3/ihep-site.json"
```

先用阶段一所选profile有效独立replica的**最小实际LHE产量**做保守值，结合阶段二逐slot attempted_lhe/accepted_hepmc/used/unused/retries/wall验证每MIX预算200/200+400能否支持100 mixed目标。只采用完整100-event物理块；逐源余数不跨文件合成虚拟块。用现有`allocate_shard()`模拟共享池、stride/offset和每campaign990节点，不能只总事件除预算。pool2被两个campaign各用198000 LHE、允许跨campaign共享，不能误加为396000独立需求；gg对应396000 LHE。实际模拟是容量判据。

为每池先求能够满足上述完整模拟的最小有限源文件数，再加25%明确安全余量（整数向上取整）并重新模拟；多余源可以被现有demand admission跳过，不能设置无限补源。将结果写`$S3/capacity.json`：schema=`three-stage-stage3-capacity-v1`、status=complete、per_pool中每池profile_id/完整resolved profile、eligible_replica_counts、conservative_events_per_source、whole_blocks_per_source、base_source_count、margin_fraction=0.25、source_count；另存measured_shower_yields、simulation_evidence、storage_check和finite_sources。

finite_sources每条严格为现有generated source结构：`pool,source_id,seed,generator_profile,planned_events`；无url/actual_events/size_bytes，实际计数必须等HELAC/PLAN。planned_events是正的保守prefix cap，不超过对应profile的planning_events_hint；hint必须来自已选扫描实际结果，不是旧597。少于cap的真实新产量由PLAN完整计数后按report-and-truncate报告，不能虚构actual_events。生产profile必须已在`common/profiles/generator_profiles.json`注册ready/compatible，完整resolved值冻结进run/task/metadata。

pool2 seed从94000起、不超过94999；gg从96000起、不超过96999。stage1使用93000..93007与95000..95001；旧全局80000..92459排除。每条source_id=`<EXP_ID>_s3_<pool>_seed<seed>`，不可与任何已有本实验源ID相同。若计算源数超出预留1000个/池，停止冻结，选择另一段已证明不交叠且在11..99999的合法范围，不能静默wrap。不要套用历史495/248或任何新cuts之前的固定文件数。

默认staging_bytes=5000000000、reserve_bytes=1000000000；每源LHE和全部BLOCK ceiling各100000000、单MIX64000000。至少1.04GB usable可容纳两互补源与十MIX完整分片。阶段一实测大小与阶段二峰值必须证明上限足够；必要增加ceiling/隔离预算要在plan前明确冻结，不在运行中自动扩容。归档不直接释放credit，只有现有accepted physical reclaim删除凭据释放；沿现有AFS锁和单权威accounting，不建新账本。

阶段三容量不足、seed不够或实测shower产率不支持预算时，报告并停止依赖工作，不隐式增加MIX目标、改cuts/预算或开始补产。参数变化需要用户新指令，不能悄悄换成另一实验。

## 完整run spec与CERN执行

下面示例沿现有schema_version2；两个offset是阶段一必须已实现的新字段，不是现成CLI flags。用Python写冻结JSON，动态值只来自交接/本阶段容量文件，不用eval：

```bash
python3 - <<'PY'
import copy,json,os
from pathlib import Path
h1=json.loads(Path(os.environ['H1']).read_text())
h2=json.loads(Path(os.environ['H2']).read_text())
c=json.loads((Path(os.environ['S3'])/'capacity.json').read_text())
assert h1['schema']=='three-stage-stage1-v1' and h1['status']=='complete'
assert h2['schema']=='three-stage-stage2-v1' and h2['status']=='complete'
assert h1['experiment_id']==h2['experiment_id']==os.environ['EXP_ID']
assert c['schema']=='three-stage-stage3-capacity-v1' and c['status']=='complete'
sources=copy.deepcopy(c['finite_sources'])
assert sources and all('generator_profile' in s and 'url' not in s for s in sources)
assert not ({s['source_id'] for s in sources} & set(h2['used_source_ids']))
campaigns=[]
for name,budgets in [('JJP_SPS_G',[200]),('JJP_DPS2_G',[200,400])]:
    campaigns.append(dict(name=name,analysis='JJP',source_lhe_budgets=budgets,
        target_mixed_events=100,max_processing_nodes=990,merge_events=1000,
        global_job_index_start=10,merge_group_index_start=1,
        detector_variants=['run3_2022_legacy_premix_v1']))
storage={k:h1['storage'][k] for k in ('cern_staging','ihep_products','ihep_archive')}
storage.update(staging_bytes=5000000000,reserve_bytes=1000000000)
spec=dict(schema_version=2,run_id=os.environ['EXP_ID']+'_s3_99k',
    core_revision=h1['code_revision'],runtime_release=h1['runtime_release'],
    runtime_dir=h1['cern_runtime_dir'],orchestration='campaign_shards',
    block_events=100,sources=sources,campaigns=campaigns,
    consumption_streams=[dict(campaign=n,pool=p,stride=1,offset=0) for n,p in [
        ('JJP_SPS_G','pool_2jpsi_g'),('JJP_DPS2_G','pool_2jpsi_g'),('JJP_DPS2_G','pool_gg')]],
    normal_shortfall_policy='report-and-truncate',unused_hepmc_warning_fraction=0.15,
    limits=dict(shard_nodes=10,target_window_nodes=20,source_lhe_bytes=100000000,
        source_block_bytes=100000000,mixed_hepmc_bytes=64000000,
        source_files_per_step=2,processing_nodes_per_wave=10),storage=storage,
    site_validation=dict(proof_path=h1['site_proof_path'],isolated_budget_bytes=4000000000),
    scheduler=dict(schedd='bigbird11.cern.ch',max_jobs_submitted=200,max_jobs_idle=100,
        max_submits_per_interval=20,submit_delay=0,max_pre_scripts=20,max_post_scripts=20,
        maxjobs=dict(lhe=20,lhe_planning=20,processing=20,block_processing=20,lhe_coordination=20)),
    resources=dict(lhe=dict(cpus=1,memory_mb=3072,disk_bytes=4000000000,flavour='workday'),
        miniaod=dict(cpus=2,memory_mb=6000,disk_bytes=4000000000,flavour='workday')))
out=Path(os.environ['SPEC']);out.parent.mkdir(parents=True,exist_ok=True)
assert not out.exists()
out.write_text(json.dumps(spec,ensure_ascii=False,indent=2)+'\n')
site=dict(name='ihep',destination='root://cceos.ihep.ac.cn:1094///store/user/chiw/'
    'Full_MC_Production/three_stage/'+os.environ['EXP_ID']+'/handoffs/stage3')
Path(os.environ['SITE']).write_text(json.dumps(site,indent=2)+'\n')
PY
cd "$CORE"
./mcprod run plan --spec "$SPEC" --output-dir "$RUN"
```

storage URL精确继承阶段一，形如CERN `root://eosuser.cern.ch///eos/user/c/chiw/Full_MC_Production/three_stage/<EXP_ID>/staging`、IHEP `root://cceos.ihep.ac.cn:1094///store/user/chiw/Full_MC_Production/three_stage/<EXP_ID>/products`和`/archive`；run_id本身生成独立run子目录。不存在可直接使用的cern.json；IHEP原site参考只有name/destination，新文件必须是stage3独立destination。若前面已批准不同预算/ceilings/flavour，示例一次改成真实冻结值，不能照抄错误数字。

执行者用myschedd bump/show选择实际schedd，首次plan前固定scheduler.schedd；示例bigbird11不能覆盖真实选择。网络操作使用获准正常CERN/IHEP环境，受限sandbox不做预期失败的live probe。提交代理为本实验私有credentials/x509_user_proxy、初次提交寿命>48h，worker私有安装到`${PWD}/credentials/x509_user_proxy`，绝不共享`/tmp/x509up_u$UID`。

一次合并检查实际生成结果：有限source数等于capacity.json冻结清单；每campaign99个shard、每shard10MIX，根类别source lhe_planning2、shards block_processing6、cleanup lhe_coordination20；worker MAXJOBS20。DAGMan max submitted200/idle100/interval20/delay0/MAX_PRE/POST20，AutoRescue true、AlwaysRunPost false。验证按物理BLOCK分配、全分片MIX一次预留、无全局PLAN屏障和前shard完成链；全局EDM10..999、外部merge组1..99、实际CMSSW线程/stream设置正确，且CMSSW RNG seed未被覆盖。没有新代码时不要重复全量测试；有必要窄修正才跑相关实际存在的scoped测试及必要AGENTS检查。

提交前报告真实两池source count、保守完整blocks/source及25%余量、各99000目标、990×100、block100、预算200/200+400、每campaign99个1000 merge组、全部subprocess覆盖。

```bash
./mcprod run submit --run "$RUN" --site cern
```

记录submission.json的schedd和top DAGMan cluster ID、UTC；首次CERN提交是全阶段唯一48h anchor。输出不明确先核对该次精确身份，不重复submit。CERN完成状态/.complete/匹配root的DAGMan成功证据及已接受产物/归档凭据一致，即可交接：

```bash
./mcprod handoff export --run "$RUN" --ready-only
./mcprod handoff transfer --run "$RUN" --site ihep --site-config "$SITE"
```

export现有campaign_shards要求run完成。正式handoff必须rejected=[]，正常共198个combined task、每campaign99组，逐组输入和1000、每campaign和99000。实际不足报告原数；merge worker要求输出数=输入数之和，不支持任意截断。不能把未实现的跨run append/autofill当工具使用。

## IHEP执行与两小时监控

复用本实验阶段一已接收且身份匹配的IHEP runtime（不是旧G5），新目录`/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production/runs/<EXP_ID>/stage3`；依阶段一交接用实际持久路径覆盖示例。CERN到IHEP的运行包/代码传输由执行代理通过既有ihep登录别名和获准文件传输完成，descriptor/bundles必须完整。不要SSH到CERN schedd。以下在IHEP登录shell执行，EXP_ID与CERN相同：

```bash
export PATH=/cvmfs/common.ihep.ac.cn/software/hepjob/bin:$PATH
export IHEP_RUN="/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production/runs/$EXP_ID/stage3"
export IHEP_RUNTIME_REMOTE="/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production/runs/$EXP_ID/runtime/ihep"
export IHEP_CORE="$IHEP_RUNTIME_REMOTE/controller/core"
export BATCH="$IHEP_RUN/batch"
export IHEP_PROXY="$IHEP_RUN/credentials/x509_user_proxy"
export X509_USER_PROXY="$IHEP_PROXY"
export HANDOFF_URL="root://cceos.ihep.ac.cn:1094///store/user/chiw/Full_MC_Production/three_stage/$EXP_ID/handoffs/stage3/handoff.json"
xrdcp --force "$HANDOFF_URL" "$IHEP_RUN/handoff.json"
python3 "$IHEP_CORE/mcprod" batch prepare --handoff "$IHEP_RUN/handoff.json" --output-dir "$BATCH" --proxy "$IHEP_PROXY" --runtime-dir "$IHEP_RUNTIME_REMOTE"
python3 "$IHEP_CORE/mcprod" batch submit --manifest "$BATCH/batch_manifest.json"
```

先创建独立持久目录、从合法代理源`install -m 600`到IHEP_PROXY；不打印凭据。准备/提交串行，不并发batch build。一次检查198任务、逐campaign99组、实际-n2/十节点轮转、private proxy和runtime身份，记录所有HepJob ID。CLI没有memory参数；必要时现有Python prepare_batch支持memory_mb，但不得把CLI默认描述成已经请求6000MB。实际有效CMSSW线程/streams以worker加载日志为准；CMSSW RNG seed沿用输入配置，已有充分证据后不反复检查。

本阶段CERN到IHEP及最终汇总**总计**最多48h；提交后初始记录，再每2h一批，最多24批，完成/用户通知允许立即检查。不是各backend再给48h。每批只查当前backend的一次状态；CERN任选其一即可：

```bash
python3 "$CORE/tools/mcprod_snapshot.py" --run "$RUN" --json
./mcprod run status --run "$RUN" --site cern
```

需要拓扑才按submission.json精确root追加`tools/dag_progress.py ROOT --schedd HOST --structure=collapsed`；history只查相同schedd与exact ID并加`-limit 1 -scanlimit 10000`，不查整个owner或最近时间窗。IHEP使用：

```bash
python3 "$IHEP_CORE/mcprod" batch status --manifest "$BATCH/batch_manifest.json"
```

hep_q/xrdfs空返回遇fork exhaustion只重试一次，再看持久结果。作业离开队列/history正常；四阶段checkpoint identity/status=ok、实际完整产品有效、精确计数正确即DONE，停止对scheduler unknown的重复追问。队列空而产品齐全绝不resubmit。排队/PRE容量等待/vacate不算失败；到48h记准确剩余IDs/状态/证据，保留运行，不自动移除或重开。

仅真实确认失败且原运行需要恢复时，先看已完成节点、精确outputs/checkpoints/rescue，再使用：

```bash
./mcprod run resume --run "$RUN" --site cern
python3 "$IHEP_CORE/mcprod" batch resume --manifest "$BATCH/batch_manifest.json"
```

CERN需确认root失败且已有rescue，IHEP只恢复明确failed jobs；不是新补产，不增加源、目标或已完成组。

## 同campaign的最终100k数据集与现有聚合API

最终物理数据集是每campaign阶段二1个1000组+阶段三99个1000组，共100个已验证组、100000事件。保留100个MiniAOD/原始ntuple组及逐组provenance是有效的100k合并数据集表示，不宣称产生一个巨大的100k MiniAOD单文件。用户没有要求必须单个物理文件；不发明跨run单文件merge CLI。selected/efficiency可用现有analysis API按campaign汇总。若用户后续明确要求单个100k MiniAOD，再用已有merge task输入结构构造准确任务并验证总和，不能本阶段偷偷增加该操作。

使用两个已完成正式handoff组成**只用于分析聚合**的输入清单；它不是新run、不得batch submit或触发再处理。首先把阶段二正式handoff复制到当前IHEP持久目录，且不覆盖阶段二原文件。下面命令要求环境H2_REMOTE是该文件绝对路径（例如`.../<EXP_ID>/stage2/handoff.json`）：

```bash
export H2_REMOTE="/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production/runs/$EXP_ID/stage2/handoff.json"
export FINAL_DIR="/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production/runs/$EXP_ID/final"
python3 - <<'PY'
import json,os
from pathlib import Path
a=json.loads(Path(os.environ['H2_REMOTE']).read_text())
b=json.loads((Path(os.environ['IHEP_RUN'])/'handoff.json').read_text())
assert a['schema']==b['schema']=='full-mc-handoff-v1'
assert a['runtime_release']==b['runtime_release'] and a['core_revision']==b['core_revision']
assert not a['rejected'] and not b['rejected']
tasks=a['tasks']+b['tasks']
groups={}; seen=set()
for task in tasks:
    p=task['subtasks']['merge']['parameters']; name=p['campaign']
    assert name in ('JJP_SPS_G','JJP_DPS2_G') and p['analysis']=='JJP'
    assert p['detector_profile_id']=='run3_2022_legacy_premix_v1'
    n=sum(int(r['events']) for r in task['subtasks']['merge']['inputs'])
    assert n==1000
    groups.setdefault(name,[]).append((task['task_id'],n))
    for stage in ('selection','efficiency'):
        for r in task['subtasks'][stage]['outputs']:
            assert r['artifact_id'] not in seen;seen.add(r['artifact_id'])
assert set(groups)=={'JJP_SPS_G','JJP_DPS2_G'}
assert all(len(v)==100 and sum(n for _,n in v)==100000 for v in groups.values())
out=Path(os.environ['FINAL_DIR']);out.mkdir(parents=True,exist_ok=True)
payload=dict(schema='full-mc-handoff-v1',run_id=os.environ['EXP_ID']+'_final',
    runtime_release=b['runtime_release'],core_revision=b['core_revision'],tasks=tasks,rejected=[],
    analysis_output='root://cceos.ihep.ac.cn:1094///store/user/chiw/Full_MC_Production/'
        'three_stage/'+os.environ['EXP_ID']+'/final-analysis')
assert not (out/'analysis-handoff.json').exists()
(out/'analysis-handoff.json').write_text(json.dumps(payload,indent=2)+'\n')
PY
export AGG_CORE="$FINAL_DIR/aggregation-runtime/core"
PYTHONPATH="$IHEP_CORE/src" python3 - <<'PY'
import json,os
from pathlib import Path
from full_mc.runtime.bootstrap import unpack_runtime
r=Path(os.environ['IHEP_RUNTIME_REMOTE'])
d=json.loads((r/'runtime.json').read_text())
unpack_runtime(r,Path(os.environ['FINAL_DIR'])/'aggregation-runtime','analyzer-el9',d['runtime_release'])
PY
python3 "$AGG_CORE/mcprod" analysis aggregate --run "$IHEP_RUN" --prepare --handoff "$FINAL_DIR/analysis-handoff.json" --output "$FINAL_DIR/aggregation.json" --campaign JJP
python3 "$AGG_CORE/mcprod" analysis worker --task "$FINAL_DIR/aggregation.json" --work-dir "$FINAL_DIR/aggregation-work"
```

这段Python只组装已存在schema和显式task列表；原task/ref不改名、不重写，artifact唯一性依阶段一实现的merge组偏移保证。随后用现有unpack_runtime展开analyzer包到新的聚合执行目录，因为不可变运行包controller/core没有external/NtupleAnalyzer；不改原运行包。prepare_aggregate按campaign+detector分组，内部调用既有ROOT合并与efficiency shard工具；JJP是analysis类型，现有`--campaign`只支持JJP，不能填物理campaign名。tar member也必须无重名，实际worker会验证，不能忽略异常。分析聚合不把两个campaign物理混合；selected条目数可能因选择而少于100000，不能拿它代替未筛选MiniAOD输入事件数。

在运行聚合前已接受产品计数和EDM唯一性证据必须证明：每campaign阶段二1000、阶段三99000、所有100组和1000节点输入有效、同campaign EDM三元组无重复、HELAC source/shower/MIX seed 与输入 provenance 按冻结记录独立。核对新增源和已用BLOCK与阶段二无交叠，不重算已有完整证据。0.15 unused-HepMC仅warning/规划指标，不是硬目标。

生成 `$EXP/stage3/handoff.json`（schema=`three-stage-stage3-v1`）及其中引用的最终汇总：experiment_id/status、阶段一二交接路径、code/runtime/analyzer身份、capacity/spec/run/submission、两站精确ID、source/profile/seed/实际计数、每campaignused_BLOCK、local/global编号与EDM唯一性证据、实际加载threads/streams、各阶段expected/actual、99新增组及最终100组完整provenance、四阶段checkpoint/输出、final-analysis产物、staging峰值/reclaim/归档/retained bytes、48h anchor/deadline、evidence_paths。以实际数标complete或shortfall，不把预计数复制成完成数。

完成标准是每campaign新增99000且与阶段二相加100000，四阶段和最终聚合有效、无编号/输入冲突；若48h时仍未完成，报告实际量和剩余项，不启动补产。向协调者只回结论、退出码和证据路径，完成后停止检查。
