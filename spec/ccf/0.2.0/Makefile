UV ?= uv
PYTHON ?= python
BASE := ../0.1.2

.PHONY: check check-exchange check-canonical check-verified check-governed check-capability-signed-producer-sync check-semantic-pack-continuity check-semantic-pack-work check-semantic-pack-agent rebuild

check: check-exchange check-canonical

check-exchange:
	$(UV) run --no-project --with-requirements requirements-checks.txt $(PYTHON) tools/validate-exchange.py

check-canonical: check-exchange
	node tools/verify-canonical.mjs

check-verified: check-canonical
	node tools/verify-verified.mjs

check-governed: check-verified
	PYTHON=$(PYTHON) $(UV) run --no-project --with-requirements $(BASE)/requirements-checks.txt bash $(BASE)/tools/check-all.sh

check-capability-signed-producer-sync: check-exchange
	node tools/verify-signed-producer-sync.mjs

check-semantic-pack-continuity: check-exchange
	node tools/verify-semantic-pack.mjs ccf-continuity-pack-v1

check-semantic-pack-work: check-exchange
	node tools/verify-semantic-pack.mjs ccf-work-pack-v1

check-semantic-pack-agent: check-exchange
	node tools/verify-semantic-pack.mjs ccf-agent-pack-v1

rebuild:
	node tools/build-requirements.mjs
	node tools/build-capsule-metadata.mjs
	node tools/build-catalog.mjs
	node tools/build-bundles.mjs
