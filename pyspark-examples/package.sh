#!/usr/bin/env bash

export PACKAGE_NAME=pyspark-examples
export MODULE=tools

echo "Packing the dependencies"
rm -rf ./$PACKAGE_NAME $PACKAGE_NAME.zip
grep -v '^ *#\|^pyspark\|^py4j' requirements.txt > .requirements-filtered.txt
pip install -t ./$PACKAGE_NAME -r .requirements-filtered.txt
rm .requirements-filtered.txt

# check to see if there are any external dependencies
# if not then create an empty file to seed zip with
if [ -z "$(ls -A $PACKAGE_NAME)" ]
then
    touch $PACKAGE_NAME/empty.txt
fi

cd $PACKAGE_NAME
zip -9mrv $PACKAGE_NAME.zip .
mv $PACKAGE_NAME.zip ..
cd ..

echo "Add all modules from local"
zip -ru9 $PACKAGE_NAME.zip $MODULE -x $MODULE/__pycache__/\*
