#!/bin/bash

mvn clean jar:jar javadoc:jar source:jar gpg:sign  install -Pcasser

