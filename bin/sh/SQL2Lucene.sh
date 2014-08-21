#!/bin/bash

HOME=../..

java -cp $HOME/dist/interop.jar:$HOME/dist/lib/JSON-java.jar:$HOME/dist/lib/lucene-core-4.9.0.jar:$HOME/dist/lib/lucene-analyzers-common-4.9.0.jar:$HOME/dist/lib/lucene-queryparser-4.9.0.jar:$HOME/dist/lib/mariadb-java-client-1.1.7.jar:$HOME/dist/lib/postgresql-9.3-1102.jdbc41.jar org.bireme.interop.SQL2Lucene "$@"
