.PHONY: check rebuild reproduce package

check:
	bash ./tools/check-all.sh

rebuild:
	node ./tools/build-catalog.mjs
	node ./tools/build-example.mjs
	node ./tools/build-vectors.mjs
	node ./tools/build-package-metadata.mjs

reproduce:
	bash ./tools/verify-reproducible.sh

package: rebuild check
	python ./tools/build-release-zip.py
