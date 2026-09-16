# HELAC-Onia absolute-rapidity configuration

> Added: 2026-09-14. Applies to newly generated HELAC-Onia LHE files in this
> repository. Existing LHE inventories and downstream products are unchanged.

## Incident and conclusion

The `v4_4` `JJP_SPS_G`/`JJP_DPS2_G` workflow exposed a systematic longitudinal
asymmetry in `pool_2jpsi_g`. The planner rejected 1,226 files with

```text
A = |N(+pz) - N(-pz)| / (N(+pz) + N(-pz)) = 0.822 ... 0.923
```

For example, seed 71747 contained 1,956 selected outgoing quarkonia with
positive `pz` and 142 with negative `pz`, giving `A = 0.8646`. The event count
and consistency across files rule out a statistical fluctuation.

The generated runtime `user.inp` did not define `miny1c` or `maxy1c`. HELAC-Onia
therefore read the packaged `input/default.inp` values:

```text
miny1c 0d0
maxy1c 30d0
```

At the time, the repository template set `absoluterap F`. Consequently the
first-final-state rapidity interval was interpreted as the signed interval
`0 <= y <= 30`, then intersected with the particle-specific rapidity cut. For
the first charmonium in `gg -> J/psi J/psi g`, this became `0 <= y <= 2.4` and
created the observed forward bias.

The fix is to define the first-particle limits explicitly as nonnegative
magnitudes and enable absolute-rapidity interpretation:

```text
miny1c 0d0
maxy1c 30d0
absoluterap T
```

## Why the lower bound is zero

`miny1c` and `maxy1c` constrain the first final-state particle; they do not
mean the minimum and maximum signed detector rapidities when `absoluterap=T`.
HELAC-Onia treats them as absolute-rapidity magnitudes in that mode.

The relevant packaged HELAC-Onia 2.7.6 logic in `src/Colliders_PSI1.f90` first
intersects the special first-particle limits with the particle-specific limits
and then takes their absolute values:

```fortran
y1cup=MIN(y1cup,yycut(hadron2parton(3)))
y1clow=MAX(y1clow,yycutlow(hadron2parton(3)))
IF(absrap)y1cup=ABS(y1cup)
IF(absrap)y1clow=ABS(y1clow)
```

Later, the event cut evaluates the absolute rapidity:

```fortran
eta=rapidity(p(l,1:4))
IF(absrap)eta=ABS(eta)
IF(eta.GT.yycut(l).OR.eta.LT.yycutlow(l))THEN
   flag=1
ENDIF
```

Using `miny1c=-30d0` with `absoluterap=T` is therefore incorrect. For a
charmonium cut of `[-2.4, 2.4]`, the intersection gives `y1clow=-2.4` and
`y1cup=2.4`; applying `ABS` collapses both limits to `2.4`. The correct lower
absolute magnitude is `0d0`.

For equal beam energies HELAC-Onia retains the configured `absoluterap` value.
For unequal beam energies it forces `absrap=.FALSE.`. These production samples
use symmetric beams, so the absolute-rapidity mode is active.

## Repository implementation

The production settings live in:

- `lhe_generation/input_templates/user.inp`;
- `lhe_generation/run_helac.sh`, which copies the template into the unpacked
  HELAC runtime before launching generation.

The test card `lhe_generation/input_templates/user_test.inp` carries the same
settings for direct test generation.

The CrystalBall addon has its own runtime `user.inp`. Its preparation path now
also writes all three values explicitly:

```bash
set_user_inp_value "${addon_dir}/input/user.inp" "miny1c" "0d0"
set_user_inp_value "${addon_dir}/input/user.inp" "maxy1c" "30d0"
set_user_inp_value "${addon_dir}/input/user.inp" "absoluterap" "T"
```

The worker startup log includes `miny1c`, `maxy1c`, and `absoluterap` from the
effective runtime card. Use that output to verify what a worker actually
loaded; inspecting only the submit-side template is insufficient.

## Scope of newly generated samples

The production runtime bundle contains one shared `user.inp`, so the change is
not restricted to `pool_2jpsi_g`.

| LHE pool | Principal campaigns | Expected effect |
|---|---|---|
| `pool_2jpsi_g` | `JJP_SPS_G`, `JJP_DPS2_G` | Removes the diagnosed first-charmonium forward bias. |
| `pool_2jpsi_cs` | `JJP_SPS_CS`, `JJP_DPS2_CS` | Uses symmetric absolute-rapidity sampling for double-J/psi generation. |
| `pool_jpsi_upsilon_CSCO` | `JUP_SPS`, `JUP_DPS3` | Uses symmetric absolute-rapidity sampling for the first final-state onium. |
| `pool_jpsi_CSCO_g` | JJP DPS1/TPS and JUP DPS/TPS inputs | CrystalBall runtime receives the same explicit absolute-rapidity configuration. |
| `pool_upsilon_CSCO_g` | JUP DPS/TPS inputs | CrystalBall runtime receives the same explicit absolute-rapidity configuration. |
| `pool_gg` | DPS2/TPS background inputs | The first final-state particle is a gluon; the symmetric `|y|` cut replaces the equivalent signed symmetric interval. |
| `helac_matrix` | HELAC subprocess scans | Receives the shared production card and must be checked when producing physics samples. |

The particle-specific template limits remain symmetric, for example
`-2.4 <= y_conia <= 2.4`. With `absoluterap=T`, the corresponding event
selection is `|y_conia| <= 2.4`. Thus this change is intended to preserve the
accepted detector region while correcting HELAC's longitudinal sampling.

This change does not rewrite or repair:

- LHE files generated with an older runtime bundle;
- workflows using `--skip-lhe-generation` and an existing inventory;
- showering, mixing, CMSSW processing, MiniAOD, or ntuple outputs derived from
  an existing LHE file.

## Release and validation procedure

Do not release a full regenerated inventory based only on card inspection.
Generate a small independent pilot for every affected quarkonium pool and
confirm the effective settings in the worker log:

```text
miny1c 0d0
maxy1c 30d0
absoluterap T
```

For each produced LHE file:

1. Count the selected outgoing quarkonium states using the pool's configured
   longitudinal-QA PDG IDs.
2. Require the existing minimum selected-particle count before interpreting
   the result.
3. Compute `A = |N(+pz)-N(-pz)|/(N(+pz)+N(-pz))`.
4. Apply the planner's configured hard limit; do not bypass the gate for a
   statistically populated failure.
5. Record the runtime-bundle identity, seed, effective `user.inp`, counts, and
   audit result with the replacement inventory.

At minimum, validate pilots for `pool_2jpsi_g`, `pool_2jpsi_cs`,
`pool_jpsi_CSCO_g`, `pool_upsilon_CSCO_g`, and
`pool_jpsi_upsilon_CSCO`. A `pool_gg` pilot should verify that its kinematic
acceptance and yield remain consistent even though it has no quarkonium state
for the longitudinal quarkonium audit.

Only LHE files generated from a runtime bundle containing the corrected card
may replace the failed `pool_2jpsi_g` inventory. Existing asymmetric files must
remain quarantined rather than being mirrored or corrected after generation.

## Related material

- `docs/tps_lhe_rapidity_followup.md`: earlier CS+CO longitudinal-asymmetry
  investigation and operational handoff.
- `docs/dps1_lhe_rapidity_audit_handoff.md`: provenance-based impact audit for
  historical DPS1 products.
- `tools/plan_lhe_blocks.py`: fail-closed file-level longitudinal symmetry
  audit used by the planner.
