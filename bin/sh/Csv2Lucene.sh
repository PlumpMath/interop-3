#!/bin/bash

HOME=../..

java -cp $HOME/dist/interop.jar:$HOME/dist/lib/JSON-java.jar:$HOME/dist/lib/lucene-core-4.9.0.jar:$HOME/dist/lib/lucene-analyzers-common-4.9.0.jar:$HOME/dist/lib/lucene-queryparser-4.9.0.jar:$HOME/dist/lib/opencsv-2.3.jar  org.bireme.interop.Csv2Lucene "$@"
