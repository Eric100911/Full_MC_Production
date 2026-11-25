# Full MC Production

`mcprod` 是 CERN 与 IHEP 联合生产的入口。运行配置使用
`"orchestration": "static_subdag"`。

各 SOURCE 作业生成或读取一份 LHE，在本地确定性洗牌、切块，保留尾块，
并将分块和清单上传至 CERN EOS。所有 SOURCE 完成后，PLAN 根据清单绑定
各物理子过程的输入，一次生成静态处理子 DAG。PROC 从 MIX 开始执行，
依次运行 GEN-SIM、RAW、RECO 和 MiniAOD。RECLAIM 在所有 PROC 成功后
回收暂存 LHE，并按事例数或文件大小生成交接清单。

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
