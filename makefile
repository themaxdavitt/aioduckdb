.venv:
	python -m venv .venv
	source .venv/bin/activate && make setup dev
	echo 'run `source .venv/bin/activate` to develop aioduckdb'

venv: .venv

setup:
	python -m pip install -U pip
	python -m pip install -Ur requirements-dev.txt

dev:
	flit install --symlink

release: lint test clean
	flit publish

format:
	python -m usort format aioduckdb
	python -m black aioduckdb

lint:
	python -m flake8 aioduckdb
	python -m usort check aioduckdb
	python -m black --check aioduckdb

test:
	python -m coverage run -m aioduckdb.tests
	python -m coverage report
	python -m mypy aioduckdb/*.py

perf:
	python -m unittest -v aioduckdb.tests.perf

.PHONY: html
html: .venv README.rst docs/*.rst docs/conf.py
	.venv/bin/sphinx-build -an -b html docs html

.PHONY: clean
clean:
	rm -rf build dist html README MANIFEST *.egg-info

.PHONY: distclean
distclean: clean
	rm -rf .venv
