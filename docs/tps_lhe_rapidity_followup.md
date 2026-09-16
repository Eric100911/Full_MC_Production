# TPS LHE 纵向单边问题：快速交接

> 更新：2026-08-27。范围：`JJP_TPS_MC_v4_1` 使用的历史
> `pool_jpsi_CSCO_g` 原始 LHE。详细背景见 `docs/tps_sampling_anomaly.md`。

## 已确认结论

问题在 shower、mixer、split block 和 ROOT 输出之前已经存在于部分原始 LHE 文件中。
完整扫描 seed 1322 得到 51,036 个 outgoing quarkonium 全部满足 `pz > 0`；对照
seed 133 的 137,533 个事件正负对称。

远端目录：

```text
root://cceos.ihep.ac.cn:1094///store/user/chiw/MC_Production_v3/LHE_pool/SPS-Jpsi/
```

已经完整下载并确认的五个独立坏原文件：

| seed | 事件数 | `+pz` | `-pz` | 绝对不对称度 |
|---:|---:|---:|---:|---:|
| 1227 | 51,078 | 51,078 | 0 | 1.0 |
| 1357 | 51,061 | 51,061 | 0 | 1.0 |
| 1585 | 50,966 | 50,966 | 0 | 1.0 |
| 1674 | 51,038 | 51,038 | 0 | 1.0 |
| 1969 | 51,020 | 51,020 | 0 | 1.0 |

另外 seed 1322、1659、1811 也已确认全正 `pz`。对称对照 seed 220、648、1002、
133 均有近似相等的正负计数。抽样是针对约 51k 事件文件的定向检查，不能用命中比例
估计整个 2000-file inventory 的坏文件比例。

本地临时副本位于：

```text
/tmp/chiw/tps_lhe_check/seed<seed>.lhe.gz
```

## 最明显的分类特征

- 所有已确认坏文件同时包含 `443`、`9940003`、`9941003`、`9942003`，四种态全部
  只有正 `pz`。
- 本轮未命中的对称文件只包含 `443`。
- 这指向 CS+CO/CrystalBall source family 的历史配置，而不是随机 seed、splitter 或
  单个 Pythia failure。

## 当前最高优先级假设

需要区分两层 rapidity 范围：

- `miny1c/maxy1c`：CrystalBall addon 对第一个 charmonium 的局部生成/抽样范围；
- `minyrapconia/maxyrapconia`：HELAC-Onia 对 charmonium 的全局 rapidity cut。

两层限制通常取交集，局部范围不会恢复已经被全局 cut 删除的事件。危险组合是：

```text
absoluterap F
miny1c        -2.4
maxy1c         2.4
minyrapconia   0.0
maxyrapconia   2.4
```

此时有效范围仍是 `[-2.4,2.4] ∩ [0,2.4] = [0,2.4]`，可以直接解释全正
rapidity/`pz`。当前仓库配置此前已看到 `absoluterap F` 以及
`miny1c/maxy1c = -2.4/2.4`；尚未完成的关键核查是坏文件生成时历史 package 中实际的
`minyrapconia/maxyrapconia`，不能只检查当前模板。

## 新会话立即执行

1. 比较坏 CS+CO package 与好 443-only package 内完整的 `input/user.inp`。
2. 同时追踪源码中 `absoluterap`、`minyrapconia/maxyrapconia`、
   `miny1c/maxy1c` 的读取位置、判断逻辑和 cut 先后顺序。
3. 搜索其他叠加的 `minyrap*`、`maxyrap*`、`mineta*`、`maxeta*` 限制。
4. 对 authoritative 2000-file inventory 做逐文件扫描，输出 seed、事件数、PDG 组成、
   `N(+pz)`、`N(-pz)`、不对称度和 checksum；按配置 family 隔离坏文件。
5. 不要用随机镜像修补现有 LOW ROOT 输出；坏 source 应隔离并重新生成。

快速搜索命令：

```bash
rg -n -i \
  'absoluterap|minyrap|maxyrap|miny[0-9a-z_]*|maxy[0-9a-z_]*|mineta|maxeta' \
  lhe_generation common generated external
```

检查 tarball 中 `user.inp`：

```bash
for archive in common/packages/*.tar.gz generated/**/*.tar.gz; do
    member=$(tar -tzf "$archive" 2>/dev/null |
        grep -E '(^|/)input/user\.inp$' | head -n 1)
    if [ -n "$member" ]; then
        echo "===== $archive :: $member ====="
        tar -xOzf "$archive" "$member" |
            grep -Ein 'absoluterap|minyrapconia|maxyrapconia|miny1c|maxy1c|minyrap|maxyrap'
    fi
done
```

所有 IHEP 完整 URL 必须保留 `:1094///store/...` 的三斜杠形式；XRootD 检查应在正常
CERN/IHEP shell 或批准的非 sandbox 命令中执行。
