lint:
	hatch run lint:verify

fmt:
	hatch run lint:fmt

test:
	hatch run unit:test

test-cov:
	hatch run unit:test-cov-report