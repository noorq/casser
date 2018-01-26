#!/usr/bin/env bash

mvn clean jar:jar javadoc:jar source:jar install -Prelease
