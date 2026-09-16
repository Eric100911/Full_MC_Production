"""Run the campaign-layer CMSSW15 ntuple configuration on local merged EDM."""
import json
from ._common import prepare, publish, result, run
from .chain import execute_chain


def execute(task, work_dir):
    work_dir = prepare(task, work_dir, "cmssw15-el9")
    execute_chain(task, work_dir)
    output = work_dir / "output_ntuple.root"
    # A ROOT header/positive byte count alone does not establish the expected tree.
    script = (
        "import ROOT,json; f=ROOT.TFile.Open(" + repr(str(output)) + "); "
        "assert f and not f.IsZombie(); t=f.Get('mkcands/X_data'); "
        "assert t; "
        "json.dump({'entries':int(t.GetEntries())},open("
        + repr(str(work_dir / "tree_count.json")) + ",'w'))"
    )
    run(task, "cmssw15-el9", ["python3", "-c", script], work_dir)
    entries = json.loads((work_dir / "tree_count.json").read_text())["entries"]
    return result(task, [publish(output, task, metadata={"entries": entries,
                         "tree": "mkcands/X_data"})])
