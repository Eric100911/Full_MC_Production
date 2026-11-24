# Full MC Production

`mcprod` 是 CERN 与 IHEP 联合生产的入口。运行配置必须显式声明
`"orchestration": "campaign_shards"`。生产控制器使用
`mcprod-shard-state-v3` 记录输入清单、任务分配、结果凭据和运行状态。

CERN 的 SOURCE 子 DAG 负责生成或读取原始 LHE、确定性分块，以及归档后回收
原始文件。SHARD 子 DAG 在所需的 PLAN 结果被接受后即可分配处理节点。
每个处理作业运行一次 MIX，将 HepMC 检查点发布到 IHEP，再运行所需的 MiniAOD
探测器配置。已经验证的检查点和 MiniAOD 产物可在重试时复用。所有 SOURCE 和
SHARD 完成后，每个输入池执行一次终端回收，删除本次运行发布的全部 CERN
LHE 分块。

## 准备运行

```bash
./mcprod catalog
./mcprod runtime prepare --site cern --release v07_00-pre2 \
  --output-dir /path/to/runtimes/v07_00-pre2/cern
./mcprod run plan --spec /path/to/pilot.json --output-dir /path/to/new-run
./tests/run_all_tests.sh
```

CERN 到 IHEP 的交接传递按原有顺序分组的 MiniAOD 引用。IHEP 批任务依次运行
`edmCopyPickMerge` 和 NTUPLE，JJP 还运行 SELECTION 和 EFFICIENCY。生产准备、
授权试运行、监控和恢复使用操作手册。旧生产的源码和历史操作流程可从原标签查阅。
[命令参考](docs/README.md) · [测试与运行](docs/testing.md) · [历史流程](docs/legacy-production.md)
