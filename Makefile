.PHONY: venv
venv:
	python3 -m venv venv

.PHONY: test
test:
	venv/bin/python -m doctest -o ELLIPSIS squibbler/squibbler.py squibbler/dialect.py README.md
