#!/bin/bash

HOME=../..

java -cp $HOME/dist/Interop2.jar:$HOME/dist/lib/JSON-java.jar org.bireme.interop.Couch2Couch "$@"
