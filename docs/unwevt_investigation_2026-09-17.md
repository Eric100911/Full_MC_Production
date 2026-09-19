# HELAC `unwevt` investigation — 2026-09-17

Updated: 2026-09-19

## Executive conclusion

`unwevt` is an output stop target in the main HELAC-Onia generation path. It
is not an event-generation budget, and it does not make the generated yield a
monotonic function of `nmc`. The main path stops when the accepted unweighted
event counter reaches `nwri_tot`; this is the direct `unwevt` check in
`HELAC-Onia-2.7.6/src/SinglePro.f90:2165-2168`.

The non-monotonic main-path yield is explained by the source, rather than by a wrapper
misreading `unwevt`:

1. `nmc` is the iteration cap for each integration pass, while the optimizer
   can run several passes and clear/restart the event loop
   (`src/SinglePro.f90:1741-1759`, `2236-2317`, `2396-2414`).
2. On the final pass, `preunw` accepted weights are used to estimate the
   unweighting bound. HELAC keeps only a top-500 weight sample and selects a
   histogram point at a cumulative 90% threshold, not the true maximum
   (`src/Helac_unwei.f90:8-70`).
3. Events above the selected bound are warned about but do not update the
   bound used for acceptance (`src/SinglePro.f90:2009-2016`). Consequently,
   changing `nmc` changes the optimization history, random stream, weight
   tail, estimated bound, and acceptance rate. Yield can therefore increase or
   decrease as `nmc` increases.

No current post-`6a47717` `nmc` sweep was launched for this investigation.
There is one measured post-fix `nmc=10^6` point and the previously recorded
post-fix `nmc=5\times10^5` pilot points, but these are not a controlled curve.
The evidence supports using `nmc=5\times10^5` as the provisional NLOstar
operating point and planning with measured per-seed output, not increasing
`nmc` by default.

## Verified parameter semantics

The current wrapper writes the following production values for the main path:
`preunw=nmc/10`, `nopt=nmc/10`, `nopt_step=nmc/10`, and `noptlim=nmc`; the
production default is `nmc=5,000,000` in `lhe_generation/run_helac.sh:1274-1291`.
The wrapper rewrites these values into the runtime `user.inp` at
`run_helac.sh:864-882`.

| Parameter | Verified meaning | Operational implication |
|---|---|---|
| `unwevt` | Read as `nwri_tot` by `SinglePro` (`src/SinglePro.f90:1587-1594`). The main path exits when `nwri == nwri_tot` (`2165-2168`). | A stop target/cap for accepted LHE events. It is not a budget for MC trials and cannot force a target if the run exhausts its integration passes first. |
| `preunw` | Read as `nunwei` (`1587-1594`). During the final unweighting pass it counts the weight samples sent to `Helac_put_unwei`; at equality HELAC computes `umax` and starts unweighting (`1991-2007`). | Controls how much of the final pass is spent estimating the weight bound. It is not the number of output events. |
| `nmc` | Read as the number of MC points (`1614-1646`). The event loop is `DO WHILE(iev.LE.nmc)` and increments `iev` before generating (`1741-1759`). | A per-pass integration/iteration cap. Total work also includes optimization passes and a final pass, so total CPU is not simply one fixed `nmc` sample. |
| `nopt` | Initial optimization interval is read as `nopti` (`1655-1663`); the local `nopt` is initialized to 100 and then reset to `nopti` after the first checkpoint (`1677-1687`, `2305-2309`). | Controls when the integration grid/channel optimization is attempted; it is not an output-event target. |
| `nopt_step` | Read at `1662`; added after each optimization, with multiplication by `optf` (`2305-2317`). | Changes the optimization schedule and therefore the pass history and random/weight distribution seen by final unweighting. |
| `noptlim` | Read at `1671-1675`; if the next scheduled optimization exceeds it, HELAC sets `number_opt=maxopt` (`2313-2317`). | Limits the optimization schedule; it changes how soon the run reaches the final pass. |
| `gener` | Chooses the channel setup. `gener=0` creates a multichannel setup; `gener>=1` uses one channel (`1614-1640`). One channel disables optimization (`1682-1685`). | Relevant to yield and cost because it changes whether the optimization machinery runs. |

The main LHE writer sets `IDWTUP=3` for unweighted output and reports the
accepted count as `Nevents=nwri` (`src/SinglePro.f90:2533-2561`). With the
current `lhewgtup=F`, the unweighted record carries the internal unit weight;
the equal-cross-section alternative is implemented separately when
`lhewgtup=T` in `src/unweight_lhe.f90:201-215`.

### The 500-weight bound estimator

`Helac_put_unwei` maintains a descending list of only 500 weights
(`src/Helac_unwei.f90:8-37`). `Helac_get_unwei` bins that list and returns the
bin edge where the cumulative count exceeds 90% of the list
(`src/Helac_unwei.f90:40-70`). This is an adaptive tail estimator, not a
guaranteed upper bound. The main path tests acceptance using the returned
`umax`; if a later weight exceeds it, HELAC increments a warning counter but
does not replace `umax` (`src/SinglePro.f90:2009-2016`). This is the concrete
source mechanism behind seed- and `nmc`-dependent output rates.

## CrystalBall addon behavior

The statement that the addon bypasses `user.inp` is too strong. The wrapper
prepares an addon-local `input/user.inp`, and the addon reads it. However, the
addon bypasses the main `ho_cluster`/`SinglePro` stopping logic and has
different semantics.

In `addon/pp_psiX_CrystalBall/src/pp_psiX_cb.f90`:

- `unwgt` and `preunw` are read at `405-416`.
- `itmax` is effectively overridden by `ITMX=1` at `426-431`.
- `nmc` is passed as `ncalm` and controls the doubling schedule beginning at
  `457-505`; a final unweighting VEGAS pass is run at `518-529`.
- `unwevt` is read into `nwri_tot` at `570-597`, but that value is not used in
  the addon unweighting procedure. The procedure increments `nwri` and writes
  accepted events without checking `nwri_tot` (`957-960`, `1683-1772`).

Therefore `unwevt` cannot size a CrystalBall output. Its count must be
measured from the LHE. `nmc`, cuts, cards, and the addon's effective RNG stream
determine the produced count. The current wrapper's nominal job seed does not
seed that addon stream.

## Measured evidence

All counts below are measured LHE event counts. The `\sqrt{N}` column is only
the nominal Poisson counting scale; seed-to-seed variation is visibly larger
for the NLOstar sample and must be treated separately.

| Pool/configuration | `nmc` | `unwevt` | Seed | LHE events | `sqrt(N)` | Cost/evidence |
|---|---:|---:|---:|---:|---:|---|
| `pool_gg`, LO pilot | 500,000 | 2,000 | pilot seeds | 2,000 per seed, target reached | — | Exact target reached. |
| `pool_2jpsi_cs`, LO pilot | 500,000 | 2,000 | pilot seeds | 2,000 per seed, target reached | — | Exact target reached. |
| `pool_jpsi_CSCO_g`, CrystalBall pilot | 500,000 | 2,000 | nominal seeds 100 and 101 | 5,316 in each, with identical decompressed output | — | `unwevt` did not cap the addon; the two jobs used the same addon RNG stream. |
| `pool_jpsi_CSCO_g`, CrystalBall seed check | 2,000,000 | 2,000 | nominal seeds 100, 101, 500, 5000, and 100 repeat | 12,926 in every leg, with one shared uncompressed SHA-256 | — | Five compressed hashes differed only through gzip metadata; the addon RNG stream was unchanged. |
| `pool_2jpsi_g`, NLOstar pilot | 500,000 | 2,000 | 70100 | 597 | 24 | Approximately 5.5 h wall per seed in the pilot record. |
| `pool_2jpsi_g`, NLOstar pilot | 500,000 | 2,000 | 70101 | 858 | 29 | Same nominal settings; different seed output. |
| `pool_2jpsi_g`, post-fix current cuts | 1,000,000 | 100,000 | 91051 | 478 | 22 | Runtime stdout records `preunw=100,000`, `nmc=1,000,000`, and 478 raw/final events at `generated/SPS_G_2mc_cutoff_minptq_0p0_0p8_nmc1e6_unwevt1e5_fulltrace_ready/logs/lhe_pool_2jpsi_g_minptq_0p0_20309160_0.stdout:15-31,54-84`. |

The two `nmc=500,000` NLOstar seeds average 728 events, but their observed
range is 597–858. The post-fix `nmc=1,000,000` point gives 478 events on a
different seed and cannot establish that doubling `nmc` improves yield. The
post-fix job used about 3.2 h of recorded CPU on the 8-CPU slot; the older
pilot timing and execution environment are not identical, so no reliable
yield-per-CPU fit should be made from these rows.

For context, the same seed and nominal `nmc=1,000,000`, `unwevt=100,000`,
`minptq=0` produced 170 events in the pre-fix trial and 478 events in the
post-fix trial. The pre-fix stdout records 170 at
`generated/SPS_G_minptq_fulltrace_trial_ready/logs/lhe_pool_2jpsi_g_nmc1e6_unwevt1e5_9570900_0.stdout:15-29,52-82`.
Commit `6a47717` changed more than one relevant condition, including restoring
absolute-rapidity cuts and adding the NLO mass cuts. Thus the 170-to-478
increase is real for the paired configurations, but it cannot be attributed
to one of those changes without a controlled factorial comparison.

The historical pre-fix sweep also showed non-monotonic output: measured rows
were 66 events for `nmc=5e6` with `unwevt=2e5` and `5e5` (identical files),
170 for `1e6/1e5`, 290 for `1e7/1e5` and `1e7/2e5` (identical files), and 258
for `2e7/4e5`. Those rows are useful evidence of the old behavior, but they
must not be used as a post-fix yield curve.

An attempted post-fix `nmc=2e7` pair was terminated before producing LHE
output, so it contributes no yield measurement.

## Recommendations for production planning

### Main-path LO pools

For the measured pilot configuration, use `nmc=500,000`, retain the current
ratio `preunw=nmc/10`, `nopt=nopt_step=nmc/10`, `noptlim=nmc`, and set
`unwevt` to the desired per-file cap. The two LO pools reached 2,000 exactly
under this setting. For a requested total of `B` events, the initial seed
count is therefore:

```text
pool_gg:       ceil(B / 2000)
pool_2jpsi_cs: ceil(B / 2000)
```

The output count must still be checked per file; the formula is based on the
measured pilot, not on `unwevt` semantics alone.

### NLOstar `pool_2jpsi_g`

The defensible provisional operating point is:

```text
nmc      = 500000
unwevt   = 2000       # stop cap, not an expected yield
preunw   = 50000
nopt     = 50000
nopt_step = 50000
noptlim = 500000
```

This is a planning point, not a proven optimum. The available data do not
support claiming that any `nmc` maximizes yield per CPU-hour. In particular,
the one `nmc=1,000,000` post-fix result is lower than both `nmc=500,000` pilot
results and is seed-confounded. Until a fixed-seed, post-fix sweep is funded,
do not increase `nmc` solely to obtain more LHE events.

For a requested NLOstar total of `B` events, use the lowest observed pilot
yield as a conservative first allocation:

```text
initial NLOstar seeds = ceil(B / 597)
```

The central two-seed estimate would be `ceil(B/728)`, but it should not be the
only allocation for production because two seeds do not characterize the
tail. Add a reserve seed or schedule incremental submission based on measured
LHE counts. Keep every source seed distinct and record the actual count; do
not treat `unwevt=2000` as evidence that a seed supplies 2,000 events.

### CrystalBall `pool_jpsi_CSCO_g`

Use the addon's `nmc` as the production control and continue to measure exact
LHE output counts. The 5,316 events at `nmc=500,000` are a valid deterministic
measurement for the current package/card/cut combination, but they are not a
multi-seed yield estimate.

The CrystalBall branch executes `HO_pp_psiX_CrystalBall` directly and never
feeds the main-path `run_config.ho` containing `set seed = ${MY_SEED}`. The
addon uses `MC_VEGAS:RANDA`, whose `DSEED` starts from the compiled constant
`1234567d0`; it does not read `input/seed.input`. This explains why nominal
seeds 100 and 101 produced identical files.

Do not freeze a production seed allocation from `ceil(B / 5316)`. First run the
source repair and forced addon-2 rebuild described in
`docs/crystalball_addon_seed_fix_plan_2026-09-18.md`, save the accepted package,
and run the fixed-seed `nmc=2.5e5, 5e5, 1e6, 2e6` sweep with that package.
The first source candidate is the author's stock `RANDA_init.inc`; accept it
only after repeated adjacent- and wide-skip seed comparisons detect no
operationally material observable correlation at a predeclared sensitivity.
Then measure multiple accepted seeds at the selected `nmc`, define a
conservative `Y_plan` from those counts, and use `ceil(B / Y_plan)` with
reserve or incremental submission.

The historical counted inventory has a median near 53.6k events/file, but it
was produced at the historical `nmc=5,000,000` default and potentially
different package/configuration conditions. It is context, not a substitute
for the controlled curve. Also note the arithmetic direction: using 5,316
instead of 53.6k as a denominator allocates about ten times more files, not
fewer.

## Unknowns and the next justified measurement

For the main path, the remaining uncertainty is quantitative, not semantic.
The CrystalBall addon also has a seed-plumbing correctness issue:

- A controlled sweep on one fixed RANDA skip seed from the accepted
  rebuilt package is needed to compare `nmc=2.5e5, 5e5, 1e6, 2e6` with
  identical cuts and measure both events and CPU time. Each point needs a
  unique output directory because
  fixed-seed filenames otherwise collide. The aborted `2e7` attempt should not
  be repeated as the first probe.
- Before a seed-spread study or production, the CrystalBall source must execute
  a seed initializer and the invocation must supply its `seed.input`. The
  current binary does neither. Test the author's stock draw-skipping include
  first; if its observable-correlation gate fails, repeat the same experiments
  with direct `DSEED` initialization or a replacement RNG. Validate same-seed
  reproducibility and different-seed nonidentity explicitly, but do not treat
  those two checks alone as proof of independence.
- A separate main-path NLOstar paired test is needed to separate the restored
  rapidity cuts from the `minmqqp/minmqbeam` cuts added by `6a47717`; this is
  not a CrystalBall sweep leg.
- More independent NLOstar seeds are needed to estimate the seed distribution;
  Poisson `sqrt(N)` errors alone are not an adequate uncertainty model for the
  observed 597/858 spread.
- The required total LHE budget per pool and the acceptable policy for
  overproduction/truncation were not specified here, so the seed formulas are
  intentionally parameterized by `B`.
- For the CrystalBall addon, the desired normalization/precision target is
  separate from the LHE count. `nmc` should not be reduced or increased based
  only on a requested event count without checking that physics requirement.

Until those measurements are available, the operational rule is: use
`unwevt` only as a main-path stop target, size campaigns from counted LHE
files, and use the observed per-seed NLOstar output with a reserve rather than
assuming monotonic scaling with `nmc`.
