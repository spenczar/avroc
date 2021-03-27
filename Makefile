.PHONY: clean
clean:
	rm -rf dist

dist: clean
	python setup.py sdist bdist_wheel

.PHONY: publish
publish: dist
	twine

.PHONY: test-all
test-all:
	tox

.PHONY: test
test:
	pytest .

.PHONY: format
format:
	black ./tests ./avroc

.PHONY: lint
lint:
	flake8 ./avroc
