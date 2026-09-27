#!/usr/bin/env python3
"""CCF 0.3.0 validator.

    validate.py vectors            check the published hash and merge vectors
    validate.py export <dir>       check an export directory

Standard library only. Exit status is 1 if any error is found. Warnings
(missing link endpoints, unverifiable locations) do not fail the run.
"""

import datetime
import hashlib
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCHEMAS = ROOT / "schemas"
MAX_INT = 2**53 - 1
KIND_FILES = {"record": "records.jsonl", "link": "links.jsonl", "blob": "blobs.jsonl"}


# --- Canonical JSON (spec section 3.1) ---------------------------------------


class CanonicalError(ValueError):
    pass


def _reject_float(text):
    raise CanonicalError(f"non-integer number {text}")


def _reject_constant(text):
    raise CanonicalError(f"invalid number {text}")


def _no_duplicates(pairs):
    obj = {}
    for key, value in pairs:
        if key in obj:
            raise CanonicalError(f"duplicate key {key!r}")
        obj[key] = value
    return obj


def parse(text):
    """Parse JSON text under CCF rules: integers only, no duplicate keys."""
    return json.loads(
        text,
        object_pairs_hook=_no_duplicates,
        parse_float=_reject_float,
        parse_constant=_reject_constant,
    )


_ESCAPES = {'"': '\\"', "\\": "\\\\", "\b": "\\b", "\f": "\\f", "\n": "\\n", "\r": "\\r", "\t": "\\t"}


def _string(s):
    out = ['"']
    for ch in s:
        if ch in _ESCAPES:
            out.append(_ESCAPES[ch])
        elif ord(ch) < 0x20:
            out.append(f"\\u{ord(ch):04x}")
        elif 0xD800 <= ord(ch) <= 0xDFFF:
            raise CanonicalError("unpaired surrogate")
        else:
            out.append(ch)
    out.append('"')
    return "".join(out)


def _utf16_key(key):
    return key.encode("utf-16-be", "surrogatepass")


def canonical(value):
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        if not -MAX_INT <= value <= MAX_INT:
            raise CanonicalError(f"integer {value} out of range")
        return str(value)
    if isinstance(value, str):
        return _string(value)
    if isinstance(value, list):
        return "[" + ",".join(canonical(v) for v in value) + "]"
    if isinstance(value, dict):
        keys = sorted(value, key=_utf16_key)
        return "{" + ",".join(_string(k) + ":" + canonical(value[k]) for k in keys) + "}"
    raise CanonicalError(f"unsupported value {value!r}")


def sha256_bytes(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def hash_data(value):
    return sha256_bytes(canonical(value).encode("utf-8"))


# --- Merge rule (spec section 5) ---------------------------------------------


def merge_key(obj):
    device = obj.get("source", {}).get("device", "")
    return (obj["rev"], obj["updated"], device.encode("utf-8"), canonical(obj).encode("utf-8"))


def merge(a, b):
    """Return the winning copy of two objects with the same id."""
    return a if merge_key(a) >= merge_key(b) else b


# --- JSON Schema subset ------------------------------------------------------

_schema_cache = {}


def _load_schema(name):
    if name not in _schema_cache:
        _schema_cache[name] = json.loads((SCHEMAS / name).read_text("utf-8"))
    return _schema_cache[name]


def _resolve(ref, base):
    file_part, _, pointer = ref.partition("#")
    name = file_part or base
    node = _load_schema(name)
    for part in filter(None, pointer.split("/")):
        node = node[part]
    return node, name


def _type_ok(value, expected):
    if expected == "object":
        return isinstance(value, dict)
    if expected == "array":
        return isinstance(value, list)
    if expected == "string":
        return isinstance(value, str)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "boolean":
        return isinstance(value, bool)
    raise ValueError(f"validator does not support type {expected}")


def schema_errors(value, schema, base, path="$"):
    """Validate against the JSON Schema keywords used in schemas/."""
    errors = []
    if "$ref" in schema:
        target, name = _resolve(schema["$ref"], base)
        errors += schema_errors(value, target, name, path)
    for sub in schema.get("allOf", []):
        errors += schema_errors(value, sub, base, path)
    if "if" in schema:
        branch = "then" if not schema_errors(value, schema["if"], base, path) else "else"
        errors += schema_errors(value, schema.get(branch, {}), base, path)
    if "type" in schema and not _type_ok(value, schema["type"]):
        return errors + [f"{path}: expected {schema['type']}"]
    if "const" in schema and value != schema["const"]:
        errors.append(f"{path}: expected {schema['const']!r}")
    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{path}: expected one of {schema['enum']}")
    if isinstance(value, str):
        if "pattern" in schema and not re.search(schema["pattern"], value):
            errors.append(f"{path}: {value!r} does not match {schema['pattern']}")
        if len(value) < schema.get("minLength", 0):
            errors.append(f"{path}: empty string")
    if isinstance(value, int) and "minimum" in schema and value < schema["minimum"]:
        errors.append(f"{path}: below minimum {schema['minimum']}")
    if isinstance(value, dict):
        for key in schema.get("required", []):
            if key not in value:
                errors.append(f"{path}: missing {key}")
        for key, sub in schema.get("properties", {}).items():
            if key in value:
                errors += schema_errors(value[key], sub, base, f"{path}.{key}")
    return errors


def _timestamp_errors(obj):
    errors = []
    for field in ("created", "updated", "deleted", "start", "end"):
        value = obj.get(field)
        if isinstance(value, str):
            try:
                datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
            except ValueError:
                errors.append(f"$.{field}: not a real date/time")
    return errors


def object_errors(obj):
    kind = obj.get("kind") if isinstance(obj, dict) else None
    if kind not in KIND_FILES:
        return ["$.kind: expected record, link, or blob"]
    errors = schema_errors(obj, _load_schema(f"{kind}.schema.json"), f"{kind}.schema.json")
    return errors + _timestamp_errors(obj)


# --- Commands ----------------------------------------------------------------


def check_vectors():
    errors = []
    hashes = json.loads((ROOT / "vectors" / "hashes.json").read_text("utf-8"))
    for case in hashes["canonical"]:
        try:
            value = parse(case["input"])
            text = canonical(value)
            digest = hash_data(value)
        except CanonicalError as exc:
            errors.append(f"canonical/{case['name']}: {exc}")
            continue
        if text != case["canonical"]:
            errors.append(f"canonical/{case['name']}: got {text!r}")
        if digest != case["hash"]:
            errors.append(f"canonical/{case['name']}: got {digest}")
    for case in hashes["invalid"]:
        try:
            canonical(parse(case["input"]))
        except CanonicalError:
            continue
        errors.append(f"invalid/{case['name']}: accepted invalid input")
    for case in hashes["bytes"]:
        digest = sha256_bytes(bytes.fromhex(case["hex"]))
        if digest != case["hash"]:
            errors.append(f"bytes/{case['name']}: got {digest}")

    merges = json.loads((ROOT / "vectors" / "merge.json").read_text("utf-8"))
    for case in merges["cases"]:
        a, b = case["a"], case["b"]
        for first, second in ((a, b), (b, a)):
            winner = "a" if merge(first, second) is a else "b"
            if winner != case["winner"]:
                errors.append(f"merge/{case['name']}: got {winner}")
                break

    total = len(hashes["canonical"]) + len(hashes["invalid"]) + len(hashes["bytes"]) + len(merges["cases"])
    return errors, [], total


def check_export(directory):
    errors, warnings = [], []
    directory = Path(directory)
    try:
        manifest = parse((directory / "manifest.json").read_text("utf-8"))
    except (OSError, ValueError) as exc:
        return [f"manifest.json: {exc}"], [], 0
    errors += [f"manifest.json {e}" for e in schema_errors(manifest, _load_schema("manifest.schema.json"), "manifest.schema.json")]

    files_dir = directory / "files"
    files = {p.name: p for p in files_dir.iterdir()} if files_dir.is_dir() else {}
    for name, path in files.items():
        if not re.fullmatch(r"[0-9a-f]{64}", name):
            errors.append(f"files/{name}: name is not a SHA-256 hex digest")
        elif sha256_bytes(path.read_bytes()) != "sha256:" + name:
            errors.append(f"files/{name}: contents do not match name")

    objects, counts = {}, {}
    for kind, filename in KIND_FILES.items():
        path = directory / filename
        lines = path.read_text("utf-8").splitlines() if path.exists() else []
        counts[kind] = 0
        for number, line in enumerate(lines, 1):
            if not line.strip():
                continue
            where = f"{filename}:{number}"
            counts[kind] += 1
            try:
                obj = parse(line)
                canonical(obj)
            except (CanonicalError, ValueError) as exc:
                errors.append(f"{where}: {exc}")
                continue
            if not isinstance(obj, dict) or obj.get("kind") != kind:
                errors.append(f"{where}: kind must be {kind}")
                continue
            errors += [f"{where} {e}" for e in object_errors(obj)]
            if not isinstance(obj.get("id"), str):
                continue
            if obj["id"] in objects:
                errors.append(f"{where}: duplicate id {obj['id']}")
            objects[obj["id"]] = (where, obj)

    for where, obj in objects.values():
        if "deleted" in obj:
            continue
        if obj["kind"] == "record" and "hash" in obj and "loc" not in obj:
            if "data" not in obj:
                errors.append(f"{where}: hash without loc or data")
            elif hash_data(obj["data"]) != obj["hash"]:
                errors.append(f"{where}: hash does not match data")
        if obj["kind"] in ("record", "blob") and "hash" in obj and "loc" in obj:
            copy = files.get(obj["hash"].removeprefix("sha256:"))
            if copy is None:
                warnings.append(f"{where}: no copy of {obj['loc']} in files/")
            elif obj["kind"] == "blob" and copy.stat().st_size != obj.get("size"):
                errors.append(f"{where}: size does not match files/ copy")
        if obj["kind"] == "link":
            for end in ("from", "to"):
                if obj.get(end) not in objects:
                    warnings.append(f"{where}: {end} {obj.get(end)} not in export")

    expected = manifest.get("counts", {}) if isinstance(manifest, dict) else {}
    actual = {"records": counts["record"], "links": counts["link"], "blobs": counts["blob"], "files": len(files)}
    for key, value in actual.items():
        if key in expected and expected[key] != value:
            errors.append(f"manifest.json: counts.{key} is {expected[key]}, export has {value}")
    return errors, warnings, sum(actual.values())


def main(argv):
    if argv[:1] == ["vectors"] and len(argv) == 1:
        errors, warnings, checked = check_vectors()
        label = "vectors"
    elif argv[:1] == ["export"] and len(argv) == 2:
        errors, warnings, checked = check_export(argv[1])
        label = argv[1]
    else:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    for message in warnings:
        print(f"warning: {message}")
    for message in errors:
        print(f"error: {message}")
    print(f"{label}: {checked} checked, {len(errors)} errors, {len(warnings)} warnings")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
