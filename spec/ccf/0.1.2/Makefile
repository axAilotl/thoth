UV ?= uv

.PHONY: check rebuild reproduce package

check:
	PYTHON=python $(UV) run --no-project --with-requirements requirements-checks.txt bash ./tools/check-all.sh

rebuild:
	node ./tools/build-catalog.mjs
	node ./tools/build-example.mjs
	node ./tools/build-vectors.mjs
	node ./tools/build-package-metadata.mjs

reproduce:
	bash ./tools/verify-reproducible.sh

package: rebuild check
	$(UV) run --no-project python ./tools/build-release-zip.py
