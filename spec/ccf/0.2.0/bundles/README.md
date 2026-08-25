# CCF 0.2.0 Working Draft bundles

These generated manifests define the installable artifact boundary for four cumulative
level distributions, one capability distribution, and three
independent semantic-pack distributions.

Level bundles are incremental:

```text
ccf-exchange-bundle-v1
  -> ccf-canonical-store-bundle-v1
  -> ccf-verified-archive-bundle-v1
  -> ccf-governed-archive-bundle-v1
```

The continuity, work, and agent bundles depend only on the Exchange bundle.
Their payload schemas are excluded from the Exchange, Canonical Store, and
Verified Archive manifests. Governed Archive remains the complete successor to
the 0.1.2 distribution.

Signed Producer Sync is a separate capability bundle containing its protocol,
receipt, and canonical credential Record/envelope schemas. Those credential
artifacts let an Exchange implementation verify explicitly trusted credential
lineages without claiming the full Canonical Store level for all content.

Each artifact entry names its source package and raw SHA-256 digest. Run
`make rebuild` after changing schemas, registries, or draft documentation.
The Exchange bundle includes the Capsule manifest and transfer streams; receipt
and higher-level downgrade fixtures stay in the conformance package. Distribution bundles do
not claim to be standalone conformance runners: the separate draft source
package contains the Makefiles, tools, private test keys, fixtures, and
input/expected-output vectors needed by the tiered suites. Keeping those test
dependencies out of installable bundles prevents test keys and the full
Governed fixture from silently becoming runtime requirements.
