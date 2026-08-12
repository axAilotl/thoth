.PHONY: check rebuild package

check:
	./tools/check-all.sh

rebuild:
	node ./tools/build-catalog.mjs
	node ./tools/build-example.mjs
	node ./tools/build-vectors.mjs

package: check
	cd .. && zip -qr ccf-0.1.1.zip ccf-0.1.1
