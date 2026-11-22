# 色八重态MC DAG workbook

> **历史物理设计记录，不是当前操作手册。** 本文保留早期 process 定义和设计背景；
> 其中的 `MC_Production_v2` 路径、旧 IHEP URL、环境说明和作业步骤已经过时。
> 当前生产请使用 `README_zh-cn.md`、`docs/testing.md` 和
> `docs/directory_path_reference.md`。

## 简要流程
Helac-Onia 2.0进行矩阵元计算，生成LHE -> 独立编译的Pythia8进行shower（standard, phi enriched） -> CMSSW进一步模拟(GEN->...->MINIAOD)
## 具体细节
### Helac-Onia
批量生成基础 LHE 池：

pool_jpsi_CSCO_g 
(Helac-Onia命令: 
define jpsi_all = cc~(3S11) cc~(3S18) cc~(1S08) cc~(3PJ8)
generate g g > jpsi_all g)

pool_upsilon_CSCO_g 
(Helac-Onia命令: 
define upsilon_all = bb~(3S11) bb~(3S18) bb~(1S08) bb~(3PJ8)
generate g g > upsilon_all g)

pool_gg (gg->gg)

pool_2jpsi (gg->cc~(3S11)+cc~(3S11)和generate g g > cc~(3S11) cc~(3S11) g 两种过程按截面混合)

pool_jpsi_upsilon_CSCO 
(Helac-Onia命令：
generate g g > jpsi y(1s)
)

LDME 使用邵华圣老师Jpsi+Upsilon论文中set 3
具体cc~对应HELAC-Onia-2.7.6/analysis/onia/LDME_demo/jpsi/jpsi_arXiv14113300_mc1500_CTEQ6M_NLOCollinearPt11_Set2_MaxSet5.dat

bb~对应HELAC-Onia-2.7.6/analysis/onia/LDME_demo/Y_arXiv14108537/Y1S_LDME.dat

换算为Helac-Onia的格式如下：
LDMEcc1S08 0.0023125d0
LDMEcc3S18 0.0003528845833333333d0
LDMEcc3P08 0.0040024d0
LDMEcc3P18 0.004002404166666667d0
LDMEcc3P28 0.0040024d0
LDMEcc3S11 0.06444444444444444d0

LDMEbb1S08 0.000021266d0
LDMEbb3S18 0.001239275d0
LDMEbb3P08 0.10807425d0
LDMEbb3P18 0.1080741666666667d0
LDMEbb3P28 0.10807425d0
LDMEbb3S11 0.5155555555555555d0

LHE中粒子编号需要修改Helac-Onia配置文件以适配Pythia8：
现在有lhe_pythia6_pythia8.f，为样例程序，适当修改后在LHE生成后单独调用，完成粒子编号的更改。

DAG 节点命名示例：LHE_JpsiG_CSCO_0, LHE_GG_0 等。

生成后分类储存LHE文件

### 后续步骤与LHE对应以及shower模式设置
JJP Campaigns:

SPS: Inputs: pool_2jpsi | Modes: Phi

DPS1: Inputs: pool_jpsi_CSCO_g, pool_jpsi_CSCO_g | Modes: [Normal, Phi]

DPS2: Inputs: pool_2jpsi, pool_gg | Modes: [Normal, Phi]

TPS: Inputs: pool_jpsi_CSCO_g, pool_jpsi_CSCO_g, pool_gg | Modes: [Normal, Normal, Phi]

Analysis: 使用本仓库 `external/TPS-Onia2MuMu` submodule 自动打包生成的 `tpsonia2mumu_code.tar.gz`，通过 `HeavyFlavorAnalysis/TPS-Onia2MuMu/test/ConfFile_cfg.py` 配置 `analysisMode=JpsiJpsiPhi`

JUP Campaigns:

SPS：pool_jpsi_upsilon_CSCO | Modes: Phi

DPS1: Inputs: pool_jpsi_CSCO_g, pool_upsilon_CSCO_g | Modes: [Phi, Normal]

DPS2: Inputs: pool_jpsi_CSCO_g, pool_upsilon_CSCO_g | Modes: [Normal, Phi]

DPS3: Inputs: pool_jpsi_upsilon_CSCO, pool_gg | Modes: [Normal, Phi]

TPS: Inputs: pool_jpsi_CSCO_g, pool_upsilon_CSCO_g, pool_gg | Modes: [Normal, Normal, Phi]

Analysis: 使用本仓库 `external/TPS-Onia2MuMu` submodule 自动打包生成的 `tpsonia2mumu_code.tar.gz`，通过 `HeavyFlavorAnalysis/TPS-Onia2MuMu/test/ConfFile_cfg.py` 配置 `analysisMode=JpsiUpsPhi`

### Pythia8
输入LHE，输出hepmc2文件

tune使用CMSSW中CP5 tune
```python
Pythia8::Pythia pythia;

// beam energy
pythia.readString("Beams:idA = 2212");
pythia.readString("Beams:idB = 2212");
pythia.readString("Beams:eCM = 13600.");

// common settings close to CMSSW
pythia.readString("Tune:preferLHAPDF = 2");   // needs LHAPDF6 support
pythia.readString("Main:timesAllowErrors = 10000");
pythia.readString("Check:epTolErr = 0.01");
pythia.readString("SLHA:minMassSM = 1000.");
pythia.readString("ParticleDecays:limitTau0 = on");
pythia.readString("ParticleDecays:tau0Max = 10");
pythia.readString("HadronLevel:QED = on");

// only relevant if showering LHE input
pythia.readString("Beams:setProductionScalesFromLHEF = off");

// Run 3 13.6 TeV CP5
pythia.readString("Tune:pp = 14");
pythia.readString("Tune:ee = 7");
pythia.readString("MultipartonInteractions:ecmPow=0.03344");
pythia.readString("MultipartonInteractions:bProfile=2");
pythia.readString("MultipartonInteractions:pT0Ref=1.41");
pythia.readString("MultipartonInteractions:coreRadius=0.7634");
pythia.readString("MultipartonInteractions:coreFraction=0.63");
pythia.readString("ColourReconnection:range=5.176");
pythia.readString("SigmaTotal:zeroAXB=off");
pythia.readString("SpaceShower:alphaSorder=2");
pythia.readString("SpaceShower:alphaSvalue=0.118");
pythia.readString("SigmaProcess:alphaSvalue=0.118");
pythia.readString("SigmaProcess:alphaSorder=2");
pythia.readString("MultipartonInteractions:alphaSvalue=0.118");
pythia.readString("MultipartonInteractions:alphaSorder=2");
pythia.readString("TimeShower:alphaSorder=2");
pythia.readString("TimeShower:alphaSvalue=0.118");
pythia.readString("SigmaTotal:mode = 0");
pythia.readString("SigmaTotal:sigmaEl = 22.08");
pythia.readString("SigmaTotal:sigmaTot = 101.037");
pythia.readString("PDF:pSet=LHAPDF6:NNPDF31_nnlo_as_0118");
```

pythia shower中，Jpsi和Upsilon衰变为mu+ mu-对，phi衰变为K+ K-对。
因为有色八重态的夸克偶素，所以需要设置：
```python
OniaShower:octetSplit = 1
Onia:massSplit = 0.2
```

当shower为standard模式时，正常shower

当shower为phi enriched模式时，需要shower出的末态含phi
在pythia中产生phi我想要两种模式：
1. 关闭MPI，然后进行shower，寻找末态含phi或者phi衰变末态的事例
2. 开启MPI，然后进行shower，寻找含phi或者phi衰变末态且phi来自于LHE中的胶子的事例

以上两个模式都不一定能产生phi，所以需要完成PartonLevel后保存状态，然后使用forceHadronLevel循环进行shower，直至检测出预期的末态后进行下个事例。默认phi enriched使用模式1.

### event mixing
输入2个或3个hepmc2文件（对应2个或3个SPS过程），依次抽取出1个事例，合并在一起得到DPS或TPS事例
输入的文件可能事例数目不同，以数目最少的为准

### CMSSW中后续模拟
将mix过后的hepmc2输出CMSSW中进行GEN-SIM等后续模拟，生成MINIAOD
然后运行Ntuple Maker，用MINIAOD生成对应的Ntuple
最后储存MINIAOD和Ntuple

### HTCondor
#### 上传到节点
所有所需程序、文件包括证书均需要打包压缩后统一上传到节点，然后解压运行。不可以在此之后还依赖文件传输系统读取afs上的内容

#### storage
程序运行结束后，统一将输出文件储存至T2_CN_Beijing
日志储存至afs
`root://cceos.ihep.ac.cn//eos/ihep/cms/store/user/xcheng/MC_Production_v2`
产生的输出文件统一存在T2_CN_Beijing站点
需要voms-proxy证书认证

#### 环境配置
目前HTCondor中大部分节点的环境都不能与Helac-Onia或意向版本的CMSSW匹配，所以在运行脚本中需要特别注意环境设置，特别是一些程序是否在节点上的cvmfs上有需要确认。可以使用需要的Sigularity环境。只是注意在切换CMSSW版本时若之前打开了Sigularity环境，需要先exit再打开新的。
