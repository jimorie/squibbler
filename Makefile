.PHONY: venv
venv:
	python3 -m venv venv

.PHONY: test
test:
	venv/bin/python -m doctest -o ELLIPSIS squibble/squibble.py squibble/dialect.py
