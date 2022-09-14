package-project:
	# Packaging the project
	rm -rf ./dist
	python3 -m build

package-dependencies:
	# Packing the dependencies
	rm -rf ./dependencies dependencies.zip
	grep -v '^ *#\|^pyspark\|^py4j' requirements.txt > .requirements-filtered.txt
	pip install -t ./dependencies -r .requirements-filtered.txt
	rm .requirements-filtered.txt
	zip -r dependencies.zip ./dependencies

package: package-project package-dependencies

all: package
