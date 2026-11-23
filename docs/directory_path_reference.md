# Directory and path reference

The run specification overrides the three roots in
`common/node_config_defaults.json` under `production_storage`. The run ID
defines a separate namespace below each root. Existing LHEs use exact counted
URLs recorded in the specification. IHEP URLs retain the form
`root://cceos.ihep.ac.cn:1094///store/...`.

| Location | v3 products |
| --- | --- |
| CERN staging | `<run>/sources/<pool>/<source>/source.lhe.gz` and `blocks/block_<source>_<index>.lhe.gz` |
| IHEP archive | `<run>/SOURCE_ARCHIVE_<index>.tar` and `<run>/mixed/<identity>.hepmc.gz` |
| IHEP products | `<run>/<campaign>/<detector>/<job>.root` and downstream merged, ntuple, and analysis products |
| CERN AFS run directory | `run.json`, `state.json`, `submission.json`, named SubDAGs, task/result JSON, and logs |
| CERN AFS ledger | `ledger/state.lock`, frozen SHARD manifests, accepted receipts, and committed merge groups |

Source reclaim verifies the IHEP archive before deleting the CERN original.
Terminal reclaim deletes the exact LHE block URLs from accepted PLAN records.
Fused HepMC and MiniAOD products have JSON sidecars containing the committed
identity, event count, size, and checksum.

Use the [operator guide](testing.md) for recovery and
[historical production references](legacy-production.md) for prior layouts.
