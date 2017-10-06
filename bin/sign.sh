#!/bin/bash

mvn clean jar:jar javadoc:jar source:jar install -Prelease
