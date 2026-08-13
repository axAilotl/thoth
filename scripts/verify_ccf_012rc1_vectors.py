"""Independent reproduction of CCF 0.1.2-rc1 vectors with the Thoth Python
implementation (ccf/hashng.py) — no package JS tooling involved."""
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from ccf.hashing import (
    blob_content_commitment,
    commit_signing_digest,
    compartment_commitment,
    decode_b64url,
    encode_b64url,
    load_private_key,
    load_public_key,
    merkle_root,
    object_hash,
    producer_batch_hash,
    producer_batch_signing_digest,
    sign_digest,
    submission_hash,
    verify_digest,
)

PKG = REPO_ROOT / "spec" / "ccf" / "0.1.2-rc1"
VEC = PKG / "vectors"
passed = failed = 0


def check(name, cond):
    global passed, failed
    if cond:
        passed += 1
    else:
        failed += 1
        print(f"FAIL {name}")


# object hashes (record/link/blob, commitments, blob content)
ov = json.load(open(VEC / "object-hashes.json"))
for kind in ("record", "link", "blob"):
    v = ov[kind]
    check(f"{kind} structural commitment",
          compartment_commitment(kind, "structural", v["structural"]) == v["expected_structural_commitment"])
    check(f"{kind} semantic commitment",
          compartment_commitment(kind, "semantic", v["semantic"]) == v["expected_semantic_commitment"])
    check(f"{kind} object hash", object_hash(v["header"]) == v["expected_object_hash"])
blob = ov["blob"]
salt = blob["semantic"]["content"]["content_salt"]
data = (PKG / "examples" / "thoth-capture" / "segment-1842.wav").read_bytes()
check("blob content commitment",
      blob_content_commitment(salt, data) == blob["expected_content_commitment"])

# submission hashes
bv = json.load(open(VEC / "producer-batch.json"))
batch = bv["batch"]
subs = {s["id"]: s for s in (*batch["records"], *batch["links"], *batch["blobs"])}
sv = json.load(open(VEC / "submission-hashes.json"))
entries = [*sv["records"], *sv["links"], *sv["blobs"]]
for e in entries:
    check(f"submission hash {e['id'][:40]}",
          e["id"] in subs and submission_hash(subs[e["id"]]) == e["expected_submission_hash"])

# merkle
mv = json.load(open(VEC / "merkle.json"))
check("merkle empty", merkle_root([]) == mv["empty_expected"])
check("merkle commit1", merkle_root(mv["commit1"]["members"]) == mv["commit1"]["expected_root"])
check("merkle commit2", merkle_root(mv["commit2"]["members"]) == mv["commit2"]["expected_root"])
check("merkle order-independent",
      merkle_root(list(reversed(mv["commit2"]["members"]))) == mv["commit2"]["expected_root"])

# producer batch hash + signature (reproduce exact Ed25519 signature)
check("batch hash", producer_batch_hash(batch) == bv["expected_batch_hash"])
priv = load_private_key(REPO_ROOT / "spec" / "ccf" / "0.1.1" / "vectors" / "TEST-ONLY-device-ed25519-private.pem")
sig = sign_digest(priv, producer_batch_signing_digest(producer_batch_hash(batch)))
check("batch signature reproduced", encode_b64url(sig) == batch["signature"])
pub = load_public_key(VEC / "device-ed25519-public.pem")
try:
    verify_digest(pub, decode_b64url(batch["signature"]),
                  producer_batch_signing_digest(batch["batch_hash"]))
    check("batch signature verifies", True)
except Exception:
    check("batch signature verifies", False)

# commit signing: digests, exact signatures, structural commitments, commit_hash, linkage
cv = json.load(open(VEC / "commit-signing.json"))
apriv = load_private_key(REPO_ROOT / "spec" / "ccf" / "0.1.1" / "vectors" / "TEST-ONLY-archive-ed25519-private.pem")
apub = load_public_key(VEC / "archive-ed25519-public.pem")
prev = None
for name in ("genesis", "commit1", "commit2"):
    v = cv[name]
    digest = commit_signing_digest(v["signing_header"], v["structural_content_without_signature"])
    check(f"{name} signing digest", "sha256:" + digest.hex() == v["expected_signing_digest"])
    check(f"{name} signature reproduced",
          encode_b64url(sign_digest(apriv, digest)) == v["signature"])
    try:
        verify_digest(apub, decode_b64url(v["signature"]), digest)
        ok = True
    except Exception:
        ok = False
    check(f"{name} signature verifies", ok)
    check(f"{name} structural commitment",
          compartment_commitment("record", "structural", v["structural"]) == v["header"]["structural_commitment"])
    check(f"{name} commit hash", object_hash(v["header"]) == v["expected_commit_hash"])
    payload = v["structural_content_without_signature"]["structural_payload"]
    parent = payload.get("parent_commit_hash")
    check(f"{name} parent linkage", parent == prev)
    prev = v["expected_commit_hash"]

print(f"\n{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
