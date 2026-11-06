# 9k pilot：重做 LHE 分块阶段与文件名长度限制

本次故障需要重做失败的 PLAN（将 LHE 确定性打乱并切成 blocks）及其尚未完成的下游任务。已成功生成并正式接受的原始 LHE 可以复用，无需重新运行 HELAC。修复工作区中的 C++ 文件不会改变已发布的运行包；这次恢复需要包含修复的新运行包和独立 run，不能直接在旧 run 上执行 `run resume` 来加载新代码。

本文给出后续操作步骤；以下恢复命令尚未执行。命令应在 CERN 提交主机运行，持久文件保留在 AFS，临时下载放在 `/tmp/chiw/`。队列与 XRootD 命令需要正常网络环境；受限 Codex 沙箱须先获得相应的非沙箱执行授权。

## 1. 故障范围与可复用产物

旧 run 的位置是：

```bash
cd /afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/v06_00-staging-scheduler
BASE="$PWD/generated/three_stage_20261018T022046Z"
OLD_RUN="$BASE/stage1/pilot9k/cern-g1"
```

本地记录中的提交身份为 `bigbird11.cern.ch` / 根 DAGMan `21008654`，冻结 core revision 为 `cbc4717de7fac836a8ee1b4adbcaa3c1e7aa6129`，运行包为 `$BASE/runtimes/pilot9k/cern`。以后检查时，以 `submission.json` 中 `current_attempt` 指向的身份为准。

`PLAN_000013` 对应 `pool_gg`、seed `96000`。其上游 `LHE_000013` 的完成凭据（证明产物已正式接受的记录）位于：

```text
ledger/accepted/LHE_/000000/LHE_000013.json
```

其中记录了 7,482 个实际事件、857,989 字节的 `source.lhe.gz`，以及 1,114 字节的伴随元数据文件 `source.lhe.gz.json`。完整源 URL 是：

```text
root://eosuser.cern.ch///eos/user/c/chiw/Full_MC_Production/three_stage/three_stage_20261018T022046Z/staging/three_stage_20261018T022046Z_s3_pilot9k_g1/sources/pool_gg/three_stage_20261018T022046Z_pilot9k_pool_gg_seed96000/source.lhe.gz
```

冻结的 PLAN 配置使用 `events_per_block=100`、`max_events_per_plan=0`、`shuffle_seed=96000037`，因此应产生 **75 个 blocks：74 × 100 + 82 = 7,482**。profile 中的 7,000 是规划容量提示；本任务没有实际设置 7,000 事件的截断上限。

旧 splitter 使用 `char fname[64]`，最多保留 63 字节可见文件名。54 字节的 group ID
`three_stage_20261018T022046Z_pilot9k_pool_gg_seed96000` 本应生成 71 字节的
`<group>_block_000000.lhe`。截断后，编号 `000000..000074` 都变成同一个
`<group>_block_00`，75 次写入互相覆盖。随后 planner 按完整文件名打开第一个 block 时失败；其 `finally` 会删除分块临时目录，不能依赖旧 worker scratch 找回这批 blocks。

本地状态在编写本文时只接受了 `LHE_000002`（seed `94002`，667 事件）和 `LHE_000013`；没有已接受的 PLAN 或物理处理结果，`allocations` 为空，两项 campaign 的 `assigned_progress` 均为 0。旧 run 的状态仍为 `running`，也没有根 rescue 文件。**这些是本地记录，不证明旧作业已经停止。**

## 2. 先确认停止位置，再准备新版本

在正常 CERN shell 检查旧 run 的准确身份、活跃子作业、已完成节点及日志；若 `submission.json` 已有新 attempt，下列手工查询也要换成其记录的 schedd 和根 ID：

```bash
python3 tools/mcprod_snapshot.py --run "$OLD_RUN"
condor_q -name bigbird11.cern.ch -dag 21008654 -nobatch
ls -l "$OLD_RUN"/run.dag.rescue*
```

最后一条在没有 rescue 时会报告文件不存在。仅在旧根 DAG 和子作业均已停止、可复用 LHE 不会再被旧流程删除时，才使用下面的新 run 步骤。停止或排空后重新读取完成凭据，不能把本文中的两份 LHE 当作永久固定的集合；尚未正式接受的任务也可能已经计算完成，需先核对其结果和日志。若已有新的完成结果，先重新界定剩余任务；不能重放已接受的物理产物，也不能复制旧完成凭据来假装新 run 已完成这些任务。

`./mcprod run resume --run "$OLD_RUN"` 只适用于**同一冻结运行包**下，根 DAG 已确认失败且有根 rescue 的续跑。它保留完成节点，但没有替换 runtime 或指定单个阶段重跑的参数。本次旧运行包仍含截断缺陷，因此不适用。不要改写旧 `core.tar.gz`、`controller/core`、任务 JSON 或历史凭据，也不要使用 `condor_submit_dag -force` 绕过原状态。

先将修复纳入一个干净、可追溯的新 core revision，再准备一个新名字的运行包。修复尚未提交时，下面的 `FIXED_CORE` 前提尚未满足；当前工作区还有其他未提交修改，不能用 `git add -A` 把它们一起打包。可使用只包含已审定修复的独立干净 checkout，并备齐原有的 HELAC 包及 pilot profile；未完成源的 HELAC 参数、cuts 和探测器配置应与旧 run 保持一致。

```bash
FIXED_CORE=/afs/cern.ch/user/c/chiw/condor/clean-planfix-core  # 改为实际干净 checkout
NEW_RELEASE=v06_00-pilot9k-planfix1
NEW_RUNTIME="$BASE/runtimes/pilot9k-planfix1/cern"
NEW_SPEC="$BASE/specs/pilot9k-planfix1.json"
NEW_RUN="$BASE/stage1/pilot9k/cern-planfix1"

git -C "$FIXED_CORE" status --porcelain
git -C "$FIXED_CORE" rev-parse HEAD
"$FIXED_CORE/mcprod" runtime prepare --site cern \
  --release "$NEW_RELEASE" --core "$FIXED_CORE" --output-dir "$NEW_RUNTIME"
```

`status --porcelain` 应无输出。`runtime prepare` 会记录源目录是否有未提交修改；调度器会拒绝 `runtime_source_dirty=true` 的 run。运行包输出目录必须是新目录。记录新 `runtime.json` 的 `git_revision`、`runtime_release`，并确认其中 `source_dirty=false`。

## 3. 新 run 复用成功 LHE，只补做未完成工作

本次 pilot 仍是 17 个有限源：13 个 `pool_2jpsi_g`，4 个 `pool_gg`；目标为 SPS 4,000 + DPS2 5,000 mixed/MiniAOD 事件，block 大小为 100，MiniAOD merge 目标为 1,000。覆盖 SPS 的 `phi_mpi_off`，以及 DPS2 的 `normal` 和 `phi_mpi_off`。每个源的 block 数按其实际计数计算，不能把所有源都当作 75 blocks。

为旧 run 的每个已接受 LHE 核实远端主文件及伴随元数据仍可读取，大小、事件数、`artifact_id`、URL、`runtime_release` 和 `complete` 标记与完成凭据相符。例如 seed `96000` 的只读远端大小检查为：

```bash
LHE_LFN=/eos/user/c/chiw/Full_MC_Production/three_stage/three_stage_20261018T022046Z/staging/three_stage_20261018T022046Z_s3_pilot9k_g1/sources/pool_gg/three_stage_20261018T022046Z_pilot9k_pool_gg_seed96000/source.lhe.gz
xrdfs root://eosuser.cern.ch/ stat "$LHE_LFN"
xrdfs root://eosuser.cern.ch/ stat "$LHE_LFN.json"
mkdir -p /tmp/chiw/pilot9k-planfix-check
xrdcp --nopbar "root://eosuser.cern.ch//$LHE_LFN.json" \
  /tmp/chiw/pilot9k-planfix-check/seed96000.sidecar.json
python3 -m json.tool /tmp/chiw/pilot9k-planfix-check/seed96000.sidecar.json
```

`LHE_LFN` 自带开头的 `/`，所以上面的复制 URL 在主机名后仍为 `///eos/...`。同样检查其他准备复用的 LHE。没有正式接受的源仍按原 profile 完成生成；不要仅凭远端出现同名文件就把它纳入输入。

下面只生成**新的** spec，不改旧 run。它适用于上述尚未分配下游工作的位置；若已有非 LHE 完成记录或已分配的下游任务，脚本会停止，需要根据已完成份额另行划分剩余工作。新 spec 保留源顺序、source ID、seed、打乱方式、消费顺序、事件目标和全局编号，把已成功源转成精确的 existing-LHE 输入。

```bash
python3 - "$OLD_RUN" "$NEW_RUNTIME" "$NEW_SPEC" <<'PY'
import json
import sys
from pathlib import Path

old, runtime, output = map(Path, sys.argv[1:])
spec = json.loads((old / "run.json").read_text())
state = json.loads((old / "state.json").read_text())
release = json.loads((runtime / "runtime.json").read_text())
if release["source_dirty"] or release["site"] != "cern":
    raise SystemExit("需要干净的 CERN runtime")
if (state["allocations"]
        or any(state["planner"]["assigned_progress"].values())
        or any(v["stage"] != "lhe" for v in state["accepted"].values())):
    raise SystemExit("旧 run 已有下游进度；请先划分剩余任务")
receipts = {p.stem: json.loads(p.read_text())
            for p in (old / "ledger/accepted").rglob("*.json")}
for index, source in enumerate(spec["sources"]):
    task_id = f"LHE_{index:06d}"
    if task_id not in state["accepted"]:
        continue
    receipt = receipts[task_id]
    ref, = receipt["outputs"]
    assert receipt["status"] == "ok" and ref["artifact_id"] == task_id
    assert ref["events"] > 0 and ref["size_bytes"] > 0
    source.pop("generator_profile", None)
    source.pop("resolved_generator_profile", None)
    source.update(url=ref["url"], actual_events=ref["events"],
                  size_bytes=ref["size_bytes"])
    print("复用", source["source_id"], ref["events"], ref["url"])
spec["run_id"] += "_planfix1"
spec.update(runtime_dir=str(runtime.resolve()),
            runtime_release=release["runtime_release"],
            core_revision=release["git_revision"])
spec.pop("core_root", None)
spec.pop("runtime_source_dirty", None)
output.parent.mkdir(parents=True, exist_ok=True)
with output.open("x") as handle:
    json.dump(spec, handle, indent=2)
    handle.write("\n")
PY

"$FIXED_CORE/mcprod" run plan --spec "$NEW_SPEC" --output-dir "$NEW_RUN"
```

这里使用 `mcprod` 的 existing-source JSON 字段；旧版 `dag_generator.py` 的 `--skip-lhe-generation` 参数不适用于此 CLI。必须同时移除已有源的 `resolved_generator_profile`，否则 source adapter 仍可能按生成源构造任务。仍需生成的源保留原 profile；新 core 必须能解析这些 pilot profile，并在新 `run.json` 中得到相同的生成参数。

新 `run_id` 将派生出新的 staging、archive 和产品路径。PLAN 读取旧 LHE，将保留的原始 LHE 和新 blocks 写入新 run 的命名空间；旧完成记录仍属于旧 run，不复制到新账本。旧源文件及元数据应保留到新流程完成读取；后续清理由各 run 的正式归档和回收流程负责。

提交前检查新 `run.json`、根 DAG 与运行包路径：两个 campaign 仍分别为 40、50 个 processing 节点，合并目标仍为 1,000，`global_job_index_start=10`、`merge_group_index_start=1` 均不变。确认这些编号没有被旧 run 的物理处理使用；如已使用，不能沿用本节。动态生成源 SubDAG 后，复用源应直接从 PLAN 开始，没有对应的 LHE 生成节点；其余源可有 LHE 节点。

在完成上述检查、确认旧作业停止和代理有效期满足提交要求后，才执行：

```bash
"$FIXED_CORE/mcprod" run submit --run "$NEW_RUN"
python3 -m json.tool "$NEW_RUN/submission.json"
python3 tools/mcprod_snapshot.py --run "$NEW_RUN"
```

新提交身份必须单独记录。若新 run 后来因可重试原因失败，且该新 run 的冻结运行包无需再改变，才按 [标准 rescue 续跑流程](README.md#resuming-a-stopped-cern-run) 使用 `run resume`。归档、暂存回收、处理分片和最终交接由新 DAG 按依赖继续，不能手工标记失败 PLAN 为成功。

本节启动的是完整剩余流程，并非只运行 PLAN 的诊断。继续到 IHEP 时，还需从**同一个干净 core revision**准备同名 release 的新 IHEP 运行包，不能沿用旧 pilot 的 IHEP 描述：

```bash
NEW_IHEP_RUNTIME="$BASE/runtimes/pilot9k-planfix1/ihep"
"$FIXED_CORE/mcprod" runtime prepare --site ihep \
  --release "$NEW_RELEASE" --core "$FIXED_CORE" --output-dir "$NEW_IHEP_RUNTIME"
```

`batch prepare --runtime-dir` 指定接收站点可用的这个新包；它会检查 handoff 的 `runtime_release` 与 IHEP 包同名、`core_revision` 与包的 `git_revision` 相同，以及合并、ntuple、分析所需 profile 齐全。具体交接、传输和 batch 命令沿用 [运行生命周期说明](README.md#run-lifecycle)，输入使用新 run 的 handoff 和新 batch 目录。

## 4. PLAN 重做后的验收

对相同 seed `96000` 的新 PLAN，检查 worker 日志、规划清单和新完成凭据：

- splitter 实际写入名与 `shuffle_split_manifest.json` 中的 `filename` 一致，编号 `000000..000074` 唯一，无 `<group>_block_00` 截断文件。
- planner 的压缩输出为 `block_<group>_000000.lhe.gz` 到 `block_<group>_000074.lhe.gz`，共 75 个不同文件；实际解压计数与每个清单条目一致，前 74 个各 100，末块 82，总计 7,482。
- PLAN 结果为 `status=ok`，`actual_input_events=planned_events=7482`，75 个输出引用的大小和暂存 URL 对应新 run；控制器已写入其正式完成凭据。单有 worker 退出码 0 或远端文件列表不够。
- 新 run 与 PLAN 日志确实使用新运行包及新 revision。成功后继续未完成的归档、processing、MiniAOD、合并和交接；IHEP 后续步骤使用新 run 正式导出的 handoff（下游交接文件）。

完成凭据和最终归档用于长期验收；planner 会删除自己的临时分块目录，不能把临时 manifest 或 scratch 当作唯一留存证据。9k pilot 的额度仍只计算一次，后续阶段三余额仍应扣除本次最终接受的 4k/5k 份额。

## 5. 修复后的长度限制

修复后的 `block_filename()` 用 `std::string` / `ostringstream` 构造名称，写文件和写 manifest 共用同一函数。程序不再有原来的 64 字节数组限制，也没有新增一个统一的 64/128/255 字符限制。`setw(6)` 表示编号**至少六位**，超过 `999999` 会继续增长。

限制来自实际文件系统的单个路径分量上限 `NAME_MAX` 和完整路径限制。按文件名编码后的**字节数**计算；本 pilot 使用 ASCII，字节数等于字符数。令 `G` 为 group ID 的字节数，`D=max(6, 最大 block index 的十进制位数)`：

| 实际生成的文件名 | 所需字节数 |
|---|---:|
| splitter：`<group>_block_<index>.lhe` | `G + D + 11` |
| splitter 临时文件：上述名称再加 `.tmp` | `G + D + 15` |
| planner：`block_<group>_<index>.lhe.gz` | `G + D + 14` |

mcprod 将 splitter 的 `--filename-prefix` 设置为 `<group>_`。若直接使用 splitter，令这个**完整 prefix** 的字节数为 `P`，则临时文件长度为 `P + D + 14`；不要把 `P` 与 `G` 混用。

在各相关目录均允许 255 字节、编号六位且完整路径也足够短的前提下，以上 mcprod 分块文件要求 `G ≤ 234`（临时 `.lhe.tmp` 最严格），对应传给 splitter 的 prefix 上限为 235 字节。不能只按最终 `.lhe` 计算出 `G ≤ 238`，也不能只按压缩文件计算出 `G ≤ 235`。本例 `G=54`，三种长度分别是 71、75 和 74 字节。

mcprod 显式把 planner manifest 命名为 `plan.json`，与 group 长度无关。若直接调用 `tools/plan_lhe_blocks.py` 且不传 `--manifest-output-path`，还会生成
`plan_manifest_<pool>_<group>.json.input_symmetry.json.tmp`，长度为
`G + pool 字节数 + 44`；这可能比 block 文件更早达到限制。自定义 manifest 路径也要计入 `.input_symmetry.json.tmp`。压缩步骤直接写 `.lhe.gz`，没有额外的 group 相关随机临时后缀；splitter 的 `.tmp` 已计入上表。

应分别在真正使用的 worker scratch 和输出目录查询限制，例如在对应机器上运行：

```bash
getconf NAME_MAX /实际已存在的目录
getconf PATH_MAX /实际已存在的目录
```

提交主机上的查询不能代表执行节点或远端 EOS。最终可用长度取所有生成名、临时名及完整路径约束中的最小值；远端存储还需满足该服务的命名规则。保留完整 source ID 和 block index，不能用截短或删除编号来迁就长度。

实现依据：[splitter](../lhe_generation/lhe_shuffle_split.cc)、[planner](../tools/plan_lhe_blocks.py)、[PLAN worker](../src/full_mc/workers/plan.py)、[源任务构造](../src/full_mc/application/shards.py)、[运行包准备](../src/full_mc/runtime/packages.py)、[Condor 续跑](../src/full_mc/backends/condor.py)。
