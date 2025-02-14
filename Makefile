.PHONY: clean clean-test clean-pyc clean-build docs help
.DEFAULT_GOAL := help

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

docs: ## build documentation
	mkdocs build

serve-docs: ## serve and watch documentation
	mkdocs serve -a 0.0.0.0:8000

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache
	rm -fr .mypy_cache

init: clean ## initialize a development environment (to be run in virtualenv)
	git init
	git checkout -b develop || true
	pip install -U pip
	pip install --extra-index-url https://pypi.fury.io/dharpa/ -U -e '.[dev_utils]'
	pre-commit install
	pre-commit install --hook-type commit-msg
	setup-cfg-fmt setup.cfg || true
	git add "*" ".*"
	pre-commit run --all-files || true
	git add "*" ".*"

update-dependencies:  ## update all development dependencies
	pip install -U pip
	pip install --extra-index-url https://pypi.fury.io/dharpa/ -U -e '.[all_dev]'


setup-cfg-fmt: # format setup.cfg
	setup-cfg-fmt setup.cfg || true

black: ## run black
	black --config pyproject.toml setup.py src/kiara_plugin/topic_modelling tests

flake: ## check style with flake8
	flake8 src/kiara_plugin/topic_modelling tests

ruff: ## run ruff for linting
	ruff src/kiara_plugin/topic_modelling tests

mypy: ## run mypy
	mypy  --namespace-packages --explicit-package-base src/kiara_plugin/topic_modelling

test: ## run tests quickly with the default Python
	py.test

test-all: ## run tests on every Python version with tox
	tox

coverage: ## check code coverage quickly with the default Python
	coverage run -m pytest tests
	coverage report -m
	coverage html
	$(BROWSER) htmlcov/index.html

check: black flake mypy test ## run dev-related checks

pre-commit: ## run pre-commit on all files
	pre-commit run --all-files

dist: clean ## build source and wheel packages
	python setup.py sdist
	python setup.py bdist_wheel
	ls -l dist
