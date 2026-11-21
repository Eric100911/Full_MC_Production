"""Immutable physics payloads with recoverable XRootD publication."""
import re

from full_mc.io import atomic_json
from full_mc.storage.transfer import artifact_ref, file_checksum

SCHEMA = "mcprod-processing-checkpoint-v1"


def _candidate(ref, checksum):
    return ref["sidecar_url"] + "." + checksum + ".pending"


def validate_metadata(record, ref, provenance, max_bytes):
    if (not isinstance(record, dict) or record.get("checkpoint_schema") != SCHEMA
            or record.get("status") != "ok" or record.get("complete") is not True
            or any(record.get(key) != value for key, value in ref.items())
            or any(record.get(key) != value for key, value in provenance.items())
            or type(record.get("size_bytes")) is not int
            or not 0 < record["size_bytes"] <= max_bytes
            or not re.fullmatch("[0-9a-f]{8}", str(record.get("adler32")))):
        raise ValueError("invalid or mismatched processing checkpoint: " + ref["url"])
    return record


def _write_record(client, path, url, record):
    atomic_json(path, record)
    client.copy_out_exclusive(path, url, file_checksum(path))
    stored = client.read_json(url)
    if stored != record:
        raise ValueError("conflicting immutable checkpoint metadata: " + url)


def recover(client, ref, validate, scratch=None):
    """Resolve a committed checkpoint or finish a payload whose commit was interrupted.

    The checksum-addressed candidate is written before the exclusive POSC
    payload. After a crash, the stored payload checksum identifies its exact
    candidate. A candidate alone never constitutes a successful publication.
    """
    record = client.read_json_if_exists(ref["sidecar_url"])
    try:
        size = client.size(ref["url"])
        checksum = client.checksum(ref["url"])
    except FileNotFoundError:
        if record is not None:
            raise ValueError("checkpoint payload missing behind commit: " + ref["url"])
        return None
    pending = record is None
    if pending:
        record = client.read_json_if_exists(_candidate(ref, checksum))
    validate(record)
    if size != record["size_bytes"] or checksum != record["adler32"]:
        raise ValueError("checkpoint payload differs from metadata: " + ref["url"])
    if pending:
        if scratch is None:
            return None
        _write_record(client, scratch, ref["sidecar_url"], record)
    return record


def publish(client, path, ref, provenance, validate, *, events, metadata):
    record = dict(artifact_ref(path, ref["artifact_id"], ref["kind"], events, ref["url"]),
                  **{key: value for key, value in ref.items()
                     if key not in ("artifact_id", "kind", "url")})
    record.update(provenance, **metadata, checkpoint_schema=SCHEMA, status="ok",
                  complete=True, adler32=file_checksum(path))
    validate(record)
    scratch = path.with_name(path.name + ".checkpoint.json")
    _write_record(client, scratch, _candidate(ref, record["adler32"]), record)
    client.copy_out_exclusive(path, ref["url"], record["adler32"])
    accepted = recover(client, ref, validate, scratch)
    if accepted is None:
        raise RuntimeError("checkpoint publication has no durable payload: " + ref["url"])
    return accepted, accepted == record
