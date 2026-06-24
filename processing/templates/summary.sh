#!/bin/bash
# DAG 汇总节点只负责给出最小完成标记，详细结果以各 processing job 日志和远端输出为准。

echo "=========================================="
echo "DAG 末端汇总节点执行完成。"
echo "请结合 processing 日志、metadata.json 和节点 JSON 中的 storage 配置检查实际输出。"
echo "汇总节点不推断或硬编码远端输出路径。"
echo "=========================================="
exit 0
