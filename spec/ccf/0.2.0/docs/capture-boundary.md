# Capture boundary and durable batching

CCF begins at durable semantic artifacts. It is not a Bluetooth, codec-frame,
audio-buffer, or device transport protocol.

```text
Bluetooth or codec frames
  -> durable phone capture spool
  -> finalized chunks or VAD segments
  -> CCF Blob + capture/transcript Records + Links
  -> batched archive admission
```

Implementations should keep these units separate:

| Unit | Purpose |
|---|---|
| Transport frame | Move bytes from device to phone |
| Recovery chunk | Resume after crash with sequence, timestamps, checksum, and retry state |
| Canonical Blob | Retain finalized audio for interoperability and erasure planning |
| Semantic Record | Represent utterance, transcript segment, conversation, note, event, or insight |
| Admission batch | Atomically admit related objects and amortize archive work |

One canonical object per second produces 28,800 objects in eight hours or
86,400 objects per continuous day before transcript, relation, provenance, and
admission objects. Canonicalization, salts, commitments, hashes, reference
checks, and journal membership are appropriate for durable artifacts, not
transport frames.

Blob duration is an operational tradeoff. Tens of seconds or several minutes
are reasonable starting points. Shorter Blobs reduce erasure blast radius;
time-span selectors and utterance Records retain semantic granularity without
forcing one-second Blob boundaries.

For interruption testing, terminate the application independently during:

1. transport reception;
2. durable spool finalization;
3. upload;
4. canonical uplift;
5. archive admission.

Recovery must yield no duplicate stable origins and no missing finalized
semantic artifacts. A retry with the same origin and identical submission is
idempotent; the same origin revision with different submission content is an
explicit conflict.
