dist: pyproject.toml thingy/__init__.py thingy/__main__.py setup.py Makefile
	hatch build

dist/thingy-0.0.1.tar.gz: dist
	@:

dist/thingy-0.0.1-py3-none-any.whl: dist
	@:

dist/thingy-0.0.1-py3.10.egg: Makefile
	python setup.py bdist_egg
	git add dist/thingy-0.0.1-py3.10.egg -f
	rm -fr build thingy.egg-info

all: dist/thingy-0.0.1.tar.gz dist/thingy-0.0.1-py3-none-any.whl dist/thingy-0.0.1-py3.10.egg
	@:

clean:
	rm -fr dist build thingy.egg-info
