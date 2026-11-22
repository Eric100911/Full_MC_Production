# HTCondor 资源调查与作业资源建议

> **时间点调查记录，不是长期有效的容量保证。** 下列 slot、CPU、内存和运行时间
> 数据来自 2026-06-16；提交前必须重新查询当前 schedd/collector 状态，并以
> `docs/testing.md` 的生产检查流程和当前生成配置为准。

**日期**: 2026-06-16
**环境**: CERN lxplus HTCondor 集群

---

## 1. 集群可用资源概览

### 1.1 总体统计

| 指标 | 数值 |
|------|------|
| 可用 (Unclaimed) slots 总数 | ~2,288 |
| 可用 CPU 总核数 | ~7,379 |
| 可用内存总量 | ~255 TB |
| 可用磁盘总量 | ~5.4 PB |

### 1.2 Slot 按 CPU 数分布

| CPU 数 | Slots 数 | 占比 | 平均内存 (GB) | 典型节点 |
|--------|----------|------|---------------|----------|
| 1 核 | 1,223 | 53.4% | 39 | 通用单核 slot |
| 2 核 | 205 | 9.0% | 229 | b9g 系列 |
| 3 核 | 152 | 6.6% | 205 | b9g 系列 |
| 4 核 | 97 | 4.2% | 197 | b9g 系列 |
| 5-7 核 | 97 | 4.2% | ~240 | b9g 系列 |
| 8 核 | 52 | 2.3% | 288 | b9g 系列 |
| 10-16 核 | ~90 | 3.9% | 170-311 | b9g 系列 |
| 24 核 | 10 | 0.4% | 1,285 | 大内存节点 |
| 48-64 核 | 14 | 0.6% | 144-281 | 大内存节点 |
| 119-384 核 | 5 | 0.2% | 515-1,512 | 超大节点 |

**关键特征**: 超过一半的可用 slot 是单核，但内存普遍充裕（单核 slot 平均有 39 GB）。所有可用 slot 均支持 Singularity 容器。

---

## 2. 当前资源请求与匹配情况

### 2.1 各作业类型当前配置

| 作业类型 | CPUs | Memory | Disk | MaxRuntime | 来源 |
|----------|------|--------|------|------------|------|
| LHE generation (生产) | 8 | 15 GB | 10 GB | 604800s (7天) | `dag_generator.py:1987` |
| LHE generation (测试) | 4 | 8 GB | 8 GB | 604800s | `dag_generator.py:1986` |
| Processing (生产) | 2 | 12 GB | 8 GB | 604800s | `dag_generator.py:1994` |
| Processing (测试) | 2 | 8 GB | 4 GB | 604800s | `dag_generator.py:1993` |
| Processing (premix localcache) | 2 | 12 GB | 80 GB | 604800s | `dag_generator.py:1991` |
| Ntuple (生产) | 2 | 12 GB | 8 GB | 604800s | `dag_generator.py:2003` |
| Ntuple-only | 1 | 2 GB | 2 GB | 604800s | `dag_generator.py:2001-2002` |
| Plan LHE blocks | 1 | 2 GB | 5 GB | 86400s (1天) | `plan_lhe_blocks.sub:24-28` |
| Coordinate LHE blocks | 1 | 2 GB | 5 GB | 86400s | `coordinate_lhe_blocks.sub:25-29` |
| Summary | 1 | 1 GB | 1 GB | 无 | `summary.sub:19-21` |

### 2.2 匹配可用 Slot 数量

| 作业类型 | 当前请求 | 可匹配 Slots | 调度难度 |
|----------|----------|-------------|----------|
| LHE generation | 8 CPU, 15 GB, 10 GB disk | **193** | 困难 — 8 核可用 slot 极少 |
| Processing | 2 CPU, 12 GB, 8 GB disk | **746** | 中等 |
| Processing (若改为 1 CPU, 8 GB) | 1 CPU, 8 GB, 8 GB disk | **~1,000+** | 容易 |
| Ntuple | 2 CPU, 12 GB, 8 GB disk | **746** | 中等 |
| Ntuple-only | 1 CPU, 2 GB, 2 GB disk | **~1,500+** | 非常容易 |
| Planner/Coordinator | 1 CPU, 2 GB, 5 GB disk | **~1,500+** | 非常容易 |

---

## 3. 问题分析

### 3.1 MaxRuntime 设置（影响最大）

所有生产作业 (`lhe_gen.sub`, `processing.sub`, `ntuple.sub`) 均硬编码 `+MaxRuntime = 604800`（7 天 = "nextweek" 级别）。

**问题**:
- 更短 MaxRuntime 的作业在 HTCondor 调度器中享有**更高优先级** — 系统会优先调度短作业
- 如果作业真的跑了 7 天，大概率已经 hang 住（死循环、I/O 阻塞等），应该更早被终止
- 不同作业类型的实际运行时间差异巨大（几分钟到数小时），统一 7 天不合理

**CERN 官方 JobFlavour**:

| Flavour | 最大墙钟时间 | 适用场景 |
|---------|-------------|----------|
| `espresso` | 20 分钟 | 快速测试、汇总 |
| `microcentury` | 1 小时 | 轻量脚本 |
| `longlunch` | 2 小时 | 中等计算 |
| `workday` | 8 小时 | 重计算 |
| `tomorrow` | 1 天 | 大型计算 |
| `testmatch` | 3 天 | 超长计算 |
| `nextweek` | 1 周 | 极端情况 |

> 默认无声明时为 `espresso` (20 分钟)，所以当前代码若不显式设置 MaxRuntime，作业会被过早杀死。

### 3.2 3 GB/core 内存缩放规则

CERN batch 文档：**"the system will scale the number of CPUs received to respect the 3gb / core limit"**。

这意味着：
- 如果请求 2 CPU + 12 GB (= 6 GB/core)，超过 3 GB/core 阈值，系统可能**自动增加到 4 CPU**
- 对于单线程为主的作业，这会浪费 CPU 配额，同时降低可匹配的 slot 数量
- **请求的内存应尽量接近 `CPU 数 × 3 GB`**，避免被动缩放

### 3.3 LHE 作业的 8 CPU 瓶颈

LHE 作业请求 8 CPU，但可用池中只有 135 个 slot 满足此条件（占总数 6%）。如果同时提交多个 LHE 作业，调度延迟会很大。建议确认 HELAC-Onia 是否有效利用了 8 核 — 如果并行度不高，可以减少 CPU 请求数以获得更多调度机会。

---

## 4. RAM/Disk 台阶分析

### 核心原则

匹配逻辑是 `TARGET.X >= RequestX`，所以**请求越少，匹配越多**。下面所有数据只统计 **Unclaimed（空闲）** 状态 slot（约 2,200-2,500 个，采样时略有波动）。

### 4.1 CPU 台阶

| request_cpus | 匹配 slots | 占比 | 丢失 |
|-------------|-----------|------|------|
| **≥ 1** | 2,218 | 100% | — |
| **≥ 2** | 766 | 34.6% | **-65%** |
| ≥ 3 | 419 | 18.9% | -16% |
| ≥ 4 | 287 | 13.0% | -6% |
| ≥ 6 | 152 | 6.9% | -6% |
| **≥ 8** | 135 | 6.1% | — |
| ≥ 16 | 43 | 1.9% | -4% |
| ≥ 32 | 24 | 1.1% | — |

**CPU 是最陡的台阶**：从 1 CPU 到 2 CPU 直接丢失 65% 的空闲 slot。每个额外 CPU 核都大幅收窄可选范围。

### 4.2 1-CPU Slot 内存台阶

1-CPU 空闲 slot 共 **1,468 个**。

| request_memory | 匹配 slots | 占比 | 变化 |
|---------------|-----------|------|------|
| ≥ 2 GB | 1,456 | 99.2% | — |
| ≥ 3 GB | 1,456 | 99.2% | 相同（仅 12 个 slot < 3 GB） |
| **≥ 4 GB** | **544** | **37.1%** | **暴跌 -912 个！** |
| ≥ 16 GB | 544 | 37.1% | 无变化 |
| ≥ 32 GB | 542 | 36.9% | 无变化 |
| ≥ 96 GB | 521 | 35.5% | -23 |
| ≥ 128 GB | 386 | 26.3% | -135 |
| ≥ 196 GB | 299 | 20.4% | -87 |
| ≥ 256 GB | 50 | 3.4% | -249 |

**关键结论**：1-CPU slot 只有两类：
- **912 个 (62%)**：恰好 3 GB 内存（CERN 默认 `1核 × 3GB`）
- **~520 个 (35%)**：96 GB 以上的大内存机器
- 3 GB 到 96 GB 之间**几乎没有 slot**

对于 1-CPU 作业：**请求 4 GB 和 96 GB 效果完全一样**（都匹配 544 slot），但请求 3 GB 匹配 1,456 slot。3→4 GB 是毁灭性台阶。

### 4.3 2-CPU Slot 内存台阶

2-CPU 空闲 slot 共 **371 个**。

| request_memory | 匹配 slots | 占比 | 变化 |
|---------------|-----------|------|------|
| ≥ 2 GB | 370 | 99.7% | — |
| ≥ 32 GB | 370 | 99.7% | — |
| ≥ 96 GB | 361 | 97.3% | — |
| ≥ 128 GB | 290 | 78.2% | -71 |
| ≥ 196 GB | 208 | 56.1% | -82 |
| ≥ 256 GB | 16 | 4.3% | -192 |

**关键结论**：2-CPU slot 内存极度充裕 — 97% 拥有 ≥ 96 GB。对于只需求 8-12 GB 的 processing 作业，内存不是瓶颈。真正限制因素是 2-CPU slot 本身只有 371 个。

### 4.4 组合条件（CPU + Memory）

这是最接近实际作业请求的视角：

| request | 匹配 slots | 占比 | 备注 |
|---------|-----------|------|------|
| **1 CPU, 3 GB** | **2,448** | **99.6%** | 几乎全部空闲 slot！ |
| 1 CPU, 4 GB | 1,550 | 63.1% | 3→4 GB 台阶丢失 900 slot |
| 1 CPU, 8 GB | 1,550 | 63.1% | 同 4 GB，中间无 slot |
| **2 CPU, 2 GB** | **1,002** | **40.8%** | CPU 台阶丢失 59% |
| 2 CPU, 8 GB | 1,004 | 40.9% | 内存不加限制 |
| 2 CPU, 16 GB | 994 | 40.5% | 内存仍不加限制 |
| **8 CPU, 16 GB** | **180** | **7.3%** | 非常受限 |
| 8 CPU, 24 GB | 166 | 6.8% | — |

### 4.5 磁盘台阶

| request_disk | 1-CPU 匹配 | 2-CPU 匹配 |
|-------------|-----------|-----------|
| ≥ 10 GB | 99.9% | 100% |
| ≥ 100 GB | 99.7% | 99.7% |
| ≥ 500 GB | 99.5% | 99.4% |

**磁盘基本不构成限制**。几乎所有空闲 slot 都有 500 GB+ 磁盘。

### 4.6 台阶总结

```
Slot 匹配数随请求变化:

1 CPU, ≤3 GB  ████████████████████████████████████████  2,448 (99.6%)
1 CPU, 4-96 GB ██████████████████████████  1,550 (63.1%)   ← 3GB 悬崖
2 CPU, any mem ██████████████████  1,002 (40.8%)          ← CPU 悬崖
8 CPU, any mem ███  166 (6.8%)                            ← 多核悬崖
```

**三大台阶（按重要性）**:
1. **CPU 台阶**：1→2 CPU 丢失 59% slot，是最陡的过滤器
2. **3 GB 内存台阶**：仅影响 1-CPU 作业；3→4 GB 丢失 62% 的 1-CPU slot
3. **多核台阶**：8+ CPU 只匹配 6% slot

---

## 5. 建议方案

### 5.1 MaxRuntime → JobFlavour（优先实施，风险最低）

| 作业类型 | 当前 MaxRuntime | 建议 Flavour | 最大墙钟时间 | 理由 |
|----------|----------------|-------------|-------------|------|
| LHE generation | 604800s | `tomorrow` | 1 天 | HELAC 大批次需数小时，留余量至 1 天 |
| Processing | 604800s | `workday` | 8 小时 | 1000 event block 通常 1-3 小时，8 小时足够容错 |
| Ntuple | 604800s | `longlunch` | 2 小时 | MiniAOD 跑 ntuple 通常 < 1 小时 |
| Plan LHE blocks | 86400s | `microcentury` | 1 小时 | 压缩 + shuffle-split，几分钟完成 |
| Coordinate LHE blocks | 86400s | `microcentury` | 1 小时 | 生成 SubDAG，几秒到几分钟 |
| Summary | 无 | `espresso` | 20 分钟 | 汇总脚本几秒完成 |

### 5.2 CPU/Memory 调整（结合台阶分析）

| 作业类型 | 当前 | 建议 | 匹配 slots | 变化 |
|----------|------|------|-----------|------|
| LHE generation | 8 CPU, 15 GB | **8 CPU, 24 GB** | ~166 | 不变；24 GB 满足 3 GB/core |
| Processing | 2 CPU, 12 GB | **2 CPU, 8 GB** | ~1,004 | 内存非瓶颈（2-CPU slot 97% ≥ 96 GB） |
| Ntuple | 2 CPU, 12 GB | **1 CPU, 3 GB** | ~2,448 | **+145%**，匹配几乎全部空闲 slot |
| Ntuple-only | 1 CPU, 2 GB | **1 CPU, 3 GB** | ~2,448 | 不变 |

### 5.3 1 CPU vs 2 CPU for Processing（核心权衡）

| 方案 | 匹配 Slots | 优点 | 缺点 |
|------|-----------|------|------|
| 2 CPU, 8 GB | ~1,004 (41%) | 单作业 wall time 更短，Pythia8+CMSSW 可并行 | 可选 slot 少一半 |
| 1 CPU, 3 GB | ~2,448 (100%) | 排队时间极短，几乎全部 slot 可用 | wall time 可能翻倍 |

**建议保持 2 CPU**。如果生产运行中 processing 排队时间显著长于 LHE，可以考虑改为 1 CPU。两个方案可以同时存在（不同 campaign 用不同配置）。

---

## 6. 涉及修改的文件

| 文件 | 修改内容 |
|------|----------|
| `dag_generator.py` (L1984-2003) | 调整 `*_resource_request()` 的返回值 |
| `processing/templates/lhe_gen.sub` | `+MaxRuntime` → `+JobFlavour = "tomorrow"` |
| `processing/templates/processing.sub` | `+MaxRuntime` → `+JobFlavour = "workday"` |
| `processing/templates/ntuple.sub` | `+MaxRuntime` → `+JobFlavour = "longlunch"` |
| `processing/templates/plan_lhe_blocks.sub` | `+MaxRuntime` → `+JobFlavour = "microcentury"` |
| `processing/templates/coordinate_lhe_blocks.sub` | `+MaxRuntime` → `+JobFlavour = "microcentury"` |
| `processing/templates/summary.sub` | 添加 `+JobFlavour = "espresso"` |

---

## 7. 实施顺序建议

1. **第一步**: 将所有模板的 `+MaxRuntime` 替换为 `+JobFlavour`（语义更清晰，对调度有正面影响）
2. **第二步**: `dag_generator.py` 中 ntuple 作业改为 `1 CPU, 3 GB`（匹配数从 ~1,000 → ~2,448，收益最大）
3. **第三步**: Processing 内存从 12 GB → 8 GB（避免 3 GB/core 缩放），LHE 内存从 15 GB → 24 GB
