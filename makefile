all:
	rm -f dist.zip
	python setup.py py2exe
	winrar a -afzip dist.zip dist
	rm -rf build dist