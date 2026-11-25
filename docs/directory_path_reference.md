# Directory and path reference

The run specification overrides the three roots in
`common/node_config_defaults.json` under `production_storage`. The run ID
defines a separate namespace below each root. Existing LHEs use exact counted
URLs recorded in the specification. IHEP URLs retain the form
`root://cceos.ihep.ac.cn:1094///store/...`.

| Location | Products |
| --- | --- |
| CERN staging | `<run>/sources/<pool>/<source>/source.json` and `blocks/block_<source>_<index>.lhe.gz` |
| IHEP archive | `<run>/SOURCE_ARCHIVE_<index>.tar` and `<run>/mixed/<identity>.hepmc.gz` |
| IHEP products | `<run>/<campaign>/<detector>/<job>.root` and downstream merged, ntuple, and analysis products |
| CERN AFS run directory | `run.json`, `run.dag`, `submission.json`, `submit/`, root task/result JSON, and logs |
| CERN AFS processing directory | `proc/processing.dag`, `proc/processing.sub`, `proc/plan.json`, `proc/results/`, `proc/results_bundle.tar.gz`, and `proc/logs/` |
| CERN AFS handoff | `handoff.json`, `merge_groups.json`, and `results/RECLAIM.json` |

IHEP HepMC and MiniAOD have JSON sidecars with the identity, event count,
size and Adler-32. RECLAIM deletes the exact CERN staging objects recorded
in `proc/plan.json` after processing.

See the [operator guide](testing.md) and
[historical production references](legacy-production.md).
