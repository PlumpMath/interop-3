#!/bin/bash

HOME=../..
java -cp $HOME/dist/Interop2.jar:$HOME/dist/lib/JSON-java.jar:$HOME/dist/lib/Bruma.jar:$HOME/dist/lib/mariadb-java-client-1.1.7.jar:$HOME/dist/lib/postgresql-9.3-1102.jdbc41.jar org.bireme.interop.SQL2Isis "$@"
