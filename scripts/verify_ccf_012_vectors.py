"""Independent reproduction of CCF 0.1.2 vectors with Thoth's Python
implementation — no package JavaScript tooling involved."""
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from ccf.hashing import (
    blob_content_commitment,
    commit_signing_digest,
    compartment_commitment,
    canonical_digest,
    decode_b64url,
    domain_hash_bytes,
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
from ccf.erasure.suppression import (
    content_digest_for_payload,
    token_for_content,
    token_for_origin,
)

PKG = REPO_ROOT / "spec" / "ccf" / "0.1.2"
VEC = PKG / "vectors"
passed = failed = 0


def suppression_entries_root(tokens):
    """Independently reproduce the registry-pinned suppression Merkle tree."""
    ordered = sorted(tokens)

    def leaf(token):
        return domain_hash_bytes("ccf:suppression-leaf:v1", token.encode())

    def node(left, right):
        return domain_hash_bytes("ccf:suppression-node:v1", left, right)

    def split(size):
        value = 1 << (size.bit_length() - 1)
        return value >> 1 if value == size else value

    def root(hashes):
        if len(hashes) == 1:
            return hashes[0]
        pivot = split(len(hashes))
        return node(root(hashes[:pivot]), root(hashes[pivot:]))

    if not ordered:
        digest = domain_hash_bytes("ccf:suppression-empty:v1")
    else:
        digest = root([leaf(token) for token in ordered])
    return "sha256:" + digest.hex()


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
priv = load_private_key(VEC / "TEST-ONLY-device-ed25519-private.pem")
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
apriv = load_private_key(VEC / "TEST-ONLY-archive-ed25519-private.pem")
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

# suppression: stable content digest, separate origin/content domains,
# canonical token ordering, entries Merkle root, and sorted scope commitment
sp = json.load(open(VEC / "suppression-canonical.json"))
fixture = sp["content_fixture"]
content_digest = content_digest_for_payload(fixture["canonical_plaintext"])
check("suppression content digest", content_digest == fixture["expected_content_digest"])
key = bytes.fromhex(sp["key_hex"])
origin, content = sp["preimages"]
origin_token = token_for_origin(
    key,
    source_id=origin["source_id"],
    native_id=origin["native_id"],
    revision=origin["revision"],
    object_kind=origin["object_kind"],
)
content_token = token_for_content(
    key,
    content_class=content["content_class"],
    content_digest=content["content_digest"],
)
check("suppression origin token", origin_token == sp["entries"][0])
check("suppression content token", content_token == sp["entries"][1])
check(
    "suppression entries root",
    suppression_entries_root([content_token, origin_token])
    == sp["expected_entries_merkle_root"],
)
check(
    "suppression scope commitment",
    canonical_digest(
        "ccf:suppression-scope:v1", sorted(sp["scope_object_ids"])
    )
    == sp["expected_scope_commitment"],
)

print(f"\n{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
