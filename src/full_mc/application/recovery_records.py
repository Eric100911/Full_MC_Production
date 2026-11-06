"""Reconstruct worker records from exact published products, without computation."""
from copy import deepcopy
import gzip
from pathlib import Path
import tarfile
from urllib.parse import urlsplit

from full_mc.storage.transfer import TransferClient, validated_record


def reconstruct(task, accepted, scratch, client=None):
    client = client or TransferClient()
    scratch = Path(scratch)
    scratch.mkdir(parents=True, exist_ok=True)
    stage = task['stage']
    outcome = dict(task_id=task['task_id'], stage=stage, status='ok',
                   runtime_release=task['runtime_release'], outputs=[])
    proof = []

    def published(ref):
        # Only producers using publish() have this sidecar convention.
        record = validated_record(ref, client)
        if (record is None or record.get('task_id') != task['task_id']
                or record.get('runtime_release') != task['runtime_release']):
            raise ValueError('invalid published metadata: ' + ref['url'])
        output = {k: v for k, v in record.items()
                  if k not in {'status', 'complete', 'task_id'}}
        output['sidecar_url'] = ref.get('sidecar_url', ref['url'] + '.json')
        output['sidecar_size_bytes'] = client.size(output['sidecar_url'])
        proof.append(output['sidecar_url'])
        return output

    def measured(ref, events):
        result = deepcopy(ref)
        result.update(size_bytes=client.size(ref['url']), events=events)
        if result['size_bytes'] <= 0:
            raise ValueError('empty published object: ' + ref['url'])
        proof.append(ref['url'])
        return result

    if stage == 'reclaim':
        reclaimed = []
        for ref in task['inputs']:
            objects = []
            for kind, url in [('payload', ref['url'])] + (
                    [('sidecar', ref['sidecar_url'])] if 'sidecar_url' in ref else []):
                try:
                    client.size(url)
                except FileNotFoundError:
                    objects.append(dict(kind=kind, url=url, disposition='already-missing'))
                else:
                    raise ValueError('reclaim still needs execution: ' + url)
            reclaimed.append(dict(artifact_id=ref['artifact_id'], objects=objects))
        outcome['reclaimed'] = reclaimed
    elif stage == 'plan':
        p = task['parameters']
        originals = []
        for ref in task['inputs']:
            original = accepted.get(ref['artifact_id'], ref)
            events = original.get('events')
            if type(events) is not int or events <= 0:
                raise ValueError('PLAN has no authoritative input count')
            originals.append(original)
        actual = sum(ref['events'] for ref in originals)
        limit = int(p.get('max_events_per_plan', 0)) or actual
        planned = min(actual, limit)
        width = int(p['events_per_block'])
        if width <= 0 or planned <= 0:
            raise ValueError('invalid PLAN bounds')
        for index in range((planned + width - 1) // width):
            url = (p['block_output_dir'].rstrip('/') + '/block_' + p['group_id']
                   + f'_{index:06d}.lhe.gz')
            local = client.copy_in(url, scratch / f'block_{index}.lhe.gz')
            with gzip.open(local, 'rt') as stream:
                count = sum(l.strip().startswith(('<event>', '<event ')) for l in stream)
            expected = min(width, planned - index * width)
            if count != expected:
                raise ValueError('PLAN block count mismatch: ' + url)
            outcome['outputs'].append(measured(dict(
                artifact_id=f"BLOCK:{p['pool_name']}:{p['group_id']}:{index:06d}",
                url=url, kind='lhe-block'), count))
            local.unlink()
        original = measured(p['original_output'], actual)
        outcome.update(actual_input_events=actual, planned_events=planned, originals=[original])
        # Generated source originals were settled by LHE, not PLAN.
        new_original = original['url'] not in {r['url'] for r in task['inputs']}
        refs = outcome['outputs'] + ([original] if new_original else [])
        outcome['staged_objects'] = [
            {k: r[k] for k in ('artifact_id', 'url', 'kind', 'size_bytes')} for r in refs]
    else:
        if stage not in {'lhe', 'mix', 'miniaod', 'archive'}:
            raise ValueError('unsupported recovery stage: ' + stage)
        outcome['outputs'] = [published(ref) for ref in task['outputs']]
        if stage == 'miniaod':
            outcome['partial'] = outcome['outputs'][0]['partial']
        if stage == 'archive':
            output = outcome['outputs'][0]
            archive_path = client.copy_in(output['url'], scratch / 'upstream.tar')
            if archive_path.stat().st_size != output['size_bytes']:
                raise ValueError('downloaded archive size mismatch')
            members = []
            with tarfile.open(archive_path, 'r:') as archive:
                entries = [entry for entry in archive.getmembers() if not entry.isdir()]
                if (any(not entry.isfile() for entry in entries)
                        or len({e.name for e in entries}) != len(entries)):
                    raise ValueError('archive contains unsafe or duplicate members')
                indexed = {entry.name: entry for entry in entries}
                for index, ref in enumerate(task['inputs']):
                    source = accepted.get(ref['artifact_id'])
                    if source is None:
                        raise ValueError('archive input has no accepted producer')
                    name = f"{index:06d}_" + Path(urlsplit(ref['url']).path).name
                    entry = indexed.pop(name, None)
                    if entry is None or entry.size != source['size_bytes']:
                        raise ValueError('archive member mismatch: ' + name)
                    members.append(dict(source, member=name))
                if indexed:
                    raise ValueError('archive has unexpected members')
            archive_path.unlink()
            outcome['members'] = members
        if 'staging_claim_bytes' in task['parameters']:
            objects = []
            for ref in outcome['outputs']:
                objects.append({k: ref[k] for k in ('artifact_id', 'url', 'kind', 'size_bytes')})
                objects.append(dict(artifact_id=ref['artifact_id'] + ':sidecar',
                                    url=ref['sidecar_url'], kind='sidecar',
                                    size_bytes=ref['sidecar_size_bytes']))
            outcome['staged_objects'] = objects
    return outcome, proof
