#!/bin/bash

for f in $(find src -name \*.java); do
    java -jar ./lib/google-java-format-1.3-all-deps.jar --replace $f
done

