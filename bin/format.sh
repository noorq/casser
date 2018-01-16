#!/usr/bin/env bash

if [ "X$1" == "Xall" ]; then
  for f in $(find ./src -name \*.java); do
    echo Formatting $f
    java -jar ./lib/google-java-format-1.3-all-deps.jar --replace $f
  done
else
  for file in $(git status --short | awk '{print $2}'); do
    echo $file
    java -jar ./lib/google-java-format-1.3-all-deps.jar --replace $file
  done
fi

